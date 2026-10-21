"""Budget To Brilliance - Airflow orchestration (guide sections 16, 17, 18 and 19).

Airflow is the ORCHESTRATOR here, not a data transport: tasks exchange only
compact metadata (batch id, counts, paths, gate decisions). The data itself
lives in files (raw / prepared Parquet) and in the Data Warehouse.

    prepare_gx_project ----------+----------------+
                                 v                v
    extract_primary --> validate_primary_raw --+
                                               +--> transform_and_integrate --> validate_prepared
    extract_api     --> validate_api_raw     --+                                        |
                                                                          load_dimensional_dw --> reconcile_dw

Batch identity
    One DAG run = one batch. The batch id is the sanitized Airflow `run_id`
    (or the `batch_id` parameter when you want to re-process an existing batch).
    Every task derives it the same way, so extraction, validation,
    transformation and load always talk about the same data, and clearing a
    task in the UI reuses the same batch.

Failure semantics
    * Critical quality failure / reconciliation mismatch -> AirflowFailException:
      the task fails WITHOUT retries (the same input would fail again) and
      every dependent task stays `upstream_failed`, so nothing is loaded.
    * Warning / Informational -> the task succeeds; the outcome is in the log
      and in data/runs/<batch>/validation/<gate>.json.
    * Transient problems (network, API 5xx/429, database connection) -> a
      bounded retry, only on the tasks where repeating the same work is safe.
"""
from __future__ import annotations

import logging
import re
import shutil
from datetime import timedelta

import pendulum
import requests
from airflow.sdk import Param, dag, get_current_context, task

try:  # location of the exception depends on the Airflow 3.x minor version
    from airflow.sdk.exceptions import AirflowFailException
except ImportError:
    from airflow.exceptions import AirflowFailException

log = logging.getLogger(__name__)

# MySQL error codes that can disappear on a later attempt (server not ready, connection lost,
# lock wait timeout, deadlock). Anything else (access denied, unknown database, constraint
# violations) is deterministic and must not be retried.
TRANSIENT_MYSQL_CODES = {2003, 2006, 2013, 1205, 1213}


# ----------------------------------------------------------------------------- helpers
def _batch_id() -> str:
    """Batch identity: the `batch_id` parameter if given, otherwise the run id (file-system safe)."""
    context = get_current_context()
    override = (context["params"].get("batch_id") or "").strip()
    return re.sub(r"[^A-Za-z0-9_-]", "_", override or context["run_id"])


def _is_transient_http(exc: Exception) -> bool:
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return exc.response.status_code >= 500 or exc.response.status_code == 429
    return False


def _is_transient_db(exc: Exception) -> bool:
    return type(exc).__name__ == "OperationalError" and bool(exc.args) and exc.args[0] in TRANSIENT_MYSQL_CODES


def _guard(step, is_transient=lambda exc: False):
    """Runs one pipeline step and maps its outcome to Airflow's failure semantics."""
    from src.load.reconcile import ReconciliationError
    from src.policy import CriticalQualityFailure

    try:
        return step()
    except (CriticalQualityFailure, ReconciliationError) as exc:
        raise AirflowFailException(str(exc)) from exc          # deterministic: stop, do not retry
    except Exception as exc:
        if is_transient(exc):
            log.warning("Transient failure, Airflow may retry: %s: %s", type(exc).__name__, exc)
            raise                                               # let the task retry policy act
        raise AirflowFailException(f"{type(exc).__name__}: {exc}") from exc


def _gate_summary(summary: dict) -> dict:
    """Compact view of a validation gate (the full result stays on disk)."""
    return {
        "batch_id": summary["batch_id"],
        "gate": summary["gate"],
        "decision": summary["decision"],
        "checks_total": summary["checks_total"],
        "checks_failed": summary["checks_failed"],
        "failed_by_severity": summary["failed_by_severity"],
        "evidence": f"data/runs/{summary['batch_id']}/validation/{summary['gate']}.json",
    }


# ------------------------------------------------------------------------------- DAG
@dag(
    dag_id="budget_to_brilliance_etl",
    description="ICFES Saber 11 + MEN API -> validated star schema in MySQL (R1-R7)",
    schedule=None,   # manual: the sources change rarely (ICFES per semester, MEN yearly)
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,   # the load is a full refresh: two runs must never write the DW at once
    dagrun_timeout=timedelta(hours=3),
    default_args={"owner": "budget-to-brilliance", "retries": 0},
    params={"batch_id": Param("", type="string",
                              description="Optional. Leave empty to use the run id; set it to re-process an existing batch.")},
    tags=["etl", "icfes", "men", "delivery-2"],
    doc_md=__doc__,
)
def budget_to_brilliance_etl():

    # ----------------------------------------------- Great Expectations project (once)
    @task(
        retries=0,
        execution_timeout=timedelta(minutes=5),
        doc_md="""Builds/refreshes the Great Expectations project in `gx/` (suites, validation definitions, checkpoints).
It runs ONCE, before the two validation tasks: they run in parallel and must only READ `gx/`
(two processes rewriting the same files at the same time corrupt a read).
**Output:** number of checkpoints. **Rerun:** idempotent.""",
    )
    def prepare_gx_project() -> dict:
        from src.validation.gx_runner import build_gx_project

        names = _guard(build_gx_project)
        log.info("prepare_gx_project: %s checkpoints ready", len(names))
        return {"checkpoints": len(names)}

    # ---------------------------------------------------------------- primary branch
    @task(
        retries=0,   # missing/unreadable files are deterministic; repeating changes nothing
        execution_timeout=timedelta(hours=1),
        doc_md="""**Input:** `ICFES_SOURCE_DIR` with the 9 DataIcfes `.txt` files; batch id from the run.
**Output:** `data/raw/primary/<batch>/` (one Parquet per period + `manifest.json`). Returns counts only.
**Failure signal:** a missing file raises -> task failed (no retry).
**Rerun:** overwrites the same batch folder; same input gives the same raw copy.""",
    )
    def extract_primary() -> dict:
        from src import config
        from src.extract import primary_extract

        batch_id = _batch_id()
        log.info("extract_primary batch_id=%s source=%s", batch_id, config.ICFES_SOURCE_DIR)
        manifest = _guard(lambda: primary_extract.extract_all(batch_id))
        return {"batch_id": batch_id, "rows": manifest["total_rows"], "periods": len(manifest["files"]),
                "extracted_at_utc": manifest["extracted_at_utc"],
                "manifest": f"data/raw/primary/{batch_id}/manifest.json"}

    @task(
        retries=0,   # same data + same rules = same result
        execution_timeout=timedelta(minutes=30),
        doc_md="""**Gate 1a.** **Input:** the raw primary batch. **Output:** `data/runs/<batch>/validation/primary_raw.json`.
**Failure signal:** a Critical rule fails -> `AirflowFailException` -> downstream `upstream_failed`.
**Rerun:** re-evaluates the same raw batch and rewrites the evidence.""",
    )
    def validate_primary_raw(extracted: dict, gx_ready: dict) -> dict:
        from src.validation import validate_primary_raw as gate

        log.info("validate_primary_raw batch_id=%s rows=%s", extracted["batch_id"], extracted["rows"])
        return _gate_summary(_guard(lambda: gate.run(extracted["batch_id"], build_project=False)))

    # -------------------------------------------------------------------- API branch
    @task(
        retries=3,   # public API: short outages and timeouts are expected; extraction is idempotent
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,   # 30 s, 60 s, 120 s: waits out a brief outage without hammering the API
        max_retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=10),
        doc_md="""**Input:** MEN API (`ji8i-4anb`), whole dataset (the year filter is applied later, in the transformation).
**Output:** `data/raw/api/<batch>/` (Parquet + `manifest.json` with the service count). Returns counts only.
**Failure signal:** network error, timeout, HTTP 5xx/429 -> retried; HTTP 4xx or a bug -> failed without retry.
**Rerun:** overwrites the same batch folder.""",
    )
    def extract_api() -> dict:
        from src import config
        from src.extract import api_extract

        batch_id = _batch_id()
        log.info("extract_api batch_id=%s url=%s", batch_id, config.MEN_API_URL)
        manifest = _guard(lambda: api_extract.extract(batch_id), _is_transient_http)
        return {"batch_id": batch_id, "rows": manifest["rows_downloaded"],
                "service_count": manifest["service_count"], "pages": manifest["pages"],
                "extracted_at_utc": manifest["extracted_at_utc"],
                "manifest": f"data/raw/api/{batch_id}/manifest.json"}

    @task(
        retries=0,
        execution_timeout=timedelta(minutes=30),
        doc_md="""**Gate 1b.** **Input:** the raw API batch. **Output:** `data/runs/<batch>/validation/api_raw.json`.
**Failure signal:** a Critical rule fails -> `AirflowFailException` -> downstream `upstream_failed`.
**Rerun:** re-evaluates the same raw batch.""",
    )
    def validate_api_raw(extracted: dict, gx_ready: dict) -> dict:
        from src.validation import validate_api_raw as gate

        log.info("validate_api_raw batch_id=%s rows=%s service_count=%s",
                 extracted["batch_id"], extracted["rows"], extracted["service_count"])
        return _gate_summary(_guard(lambda: gate.run(extracted["batch_id"], build_project=False)))

    # ------------------------------------------------- convergence: both gates passed
    @task(
        retries=0,   # deterministic code on persisted input
        execution_timeout=timedelta(hours=1),
        doc_md="""**Input:** the two gate summaries (both must have passed) and the raw batches on disk.
**Output:** `data/prepared/<batch>/` and `data/runs/<batch>/transform_report.json` (counts, quarantine, thresholds).
**Failure signal:** any exception -> failed (no retry).
**Rerun:** the prepared folder of the batch is deleted first, so no file of a previous attempt survives.""",
    )
    def transform_and_integrate(primary_gate: dict, api_gate: dict) -> dict:
        from src import config
        from src.integrate.department_year import build_integration
        from src.transform.icfes_transform import transform_primary
        from src.transform.men_transform import transform_api

        batch_id = primary_gate["batch_id"]
        if api_gate["batch_id"] != batch_id:
            raise AirflowFailException(f"The branches belong to different batches: {batch_id} vs {api_gate['batch_id']}")
        log.info("transform_and_integrate batch_id=%s", batch_id)

        def step():
            shutil.rmtree(config.prepared_dir(batch_id), ignore_errors=True)   # safe rerun
            primary = transform_primary(batch_id)
            api = transform_api(batch_id)
            integration = build_integration(batch_id)
            return primary, api, integration

        primary, api, integration = _guard(step)
        return {"batch_id": batch_id,
                "icfes_rows_raw": primary["rows_raw"], "icfes_rows_prepared": primary["rows_prepared"],
                "schools": primary["schools"],
                "men_rows_raw": api["rows_raw"], "men_rows_window": api["rows_window"],
                "integration_rows": int(integration.height),
                "prepared": f"data/prepared/{batch_id}"}

    @task(
        retries=0,
        execution_timeout=timedelta(minutes=30),
        doc_md="""**Gate 2.** **Input:** the prepared batch. **Output:** `data/runs/<batch>/validation/prepared.json`.
**Failure signal:** a Critical rule fails -> `AirflowFailException` -> the load does not run.
**Rerun:** re-evaluates the same prepared batch.""",
    )
    def validate_prepared(transformed: dict) -> dict:
        from src.validation import validate_prepared as gate

        log.info("validate_prepared batch_id=%s prepared_rows=%s", transformed["batch_id"], transformed["icfes_rows_prepared"])
        return _gate_summary(_guard(lambda: gate.run(transformed["batch_id"], build_project=False)))

    # ----------------------------------------------------------------------- load
    @task(
        retries=2,   # connection problems can disappear; the load is a full refresh, so repeating it is safe
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,   # 1 min, 2 min
        max_retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=2),
        doc_md="""**Input:** the prepared batch accepted by Gate 2. **Output:** rows in the MySQL star schema. Returns counts only.
**Failure signal:** connection/lock errors -> retried; constraint or SQL errors -> failed without retry.
**Rerun:** full refresh (empty tables, reload) -> same result; an interrupted load is repaired by running again.""",
    )
    def load_dimensional_dw(prepared_gate: dict) -> dict:
        from src.load.load_all import load_all

        batch_id = prepared_gate["batch_id"]
        log.info("load_dimensional_dw batch_id=%s (gate decision: %s)", batch_id, prepared_gate["decision"])
        summary = _guard(lambda: load_all(batch_id), _is_transient_db)
        return {"batch_id": batch_id, "dimensions": summary["dimensions"],
                "fact_resultado_rows": sum(summary["facts"]["fact_resultado_saber11"].values()),
                "fact_indicador_rows": summary["facts"]["fact_indicador_departamento"]}

    @task(
        retries=2,   # only for connection problems; a mismatch is raised as a deterministic failure
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=30),
        doc_md="""**Input:** the loaded DW and the prepared batch. **Output:** `data/runs/<batch>/reconciliation.json`.
**Failure signal:** a count that does not match or an integrity query > 0 -> `ReconciliationError` -> failed (no retry).
**Rerun:** read-only; re-evaluates the DW.""",
    )
    def reconcile_dw(loaded: dict) -> dict:
        from src.load.reconcile import reconcile

        result = _guard(lambda: reconcile(loaded["batch_id"]), _is_transient_db)
        return {"batch_id": loaded["batch_id"], "checks": len(result["checks"]), "decision": result["decision"]}

    # ------------------------------------------------------------ dependencies
    gx_ready = prepare_gx_project()
    primary_gate = validate_primary_raw(extract_primary(), gx_ready)
    api_gate = validate_api_raw(extract_api(), gx_ready)
    transformed = transform_and_integrate(primary_gate, api_gate)
    prepared_gate = validate_prepared(transformed)
    loaded = load_dimensional_dw(prepared_gate)
    reconcile_dw(loaded)


budget_to_brilliance_etl()
