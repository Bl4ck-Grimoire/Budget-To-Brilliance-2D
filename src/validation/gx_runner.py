"""Great Expectations plumbing.

  build_gx_project()  creates/updates the file context in ./gx from the
                      declarative spec (data source, asset, batch definition,
                      expectation suite, validation definition, checkpoint).
                      It is idempotent: running it twice changes nothing.
  run_checkpoint()    validates one Polars DataFrame with one checkpoint and
                      returns plain dictionaries (one per expectation) that
                      policy.py turns into a decision.

GX works on pandas, so a Polars frame is converted right before validation.
Large tables are validated period by period to keep memory bounded.
"""
import logging

import great_expectations as gx
import polars as pl

from src import config
from src.validation.expectations import checkpoint_specs

log = logging.getLogger(__name__)

DATA_SOURCE = "pandas_runtime"


def get_context():
    return gx.get_context(mode="file", project_root_dir=str(config.GX_ROOT))


def _get_or_add_asset(data_source, name: str):
    try:
        return data_source.get_asset(name)
    except LookupError:
        return data_source.add_dataframe_asset(name)


def _get_or_add_batch_definition(asset, name: str = "whole_dataframe"):
    try:
        return asset.get_batch_definition(name)
    except KeyError:
        return asset.add_batch_definition_whole_dataframe(name)


def build_gx_project() -> list[str]:
    """Creates or refreshes suites, validation definitions and checkpoints. Returns the checkpoint names."""
    context = get_context()
    data_source = context.data_sources.add_or_update_pandas(DATA_SOURCE)
    names = []
    for name, spec in checkpoint_specs().items():
        asset = _get_or_add_asset(data_source, name)
        batch_definition = _get_or_add_batch_definition(asset)
        suite = context.suites.add_or_update(
            gx.ExpectationSuite(name=f"{name}_suite", expectations=spec["expectations"])
        )
        validation = context.validation_definitions.add_or_update(
            gx.ValidationDefinition(name=f"{name}_validation", data=batch_definition, suite=suite)
        )
        context.checkpoints.add_or_update(
            gx.Checkpoint(name=name, validation_definitions=[validation],
                          result_format={"result_format": "SUMMARY"})
        )
        names.append(name)
    log.info("[GX] project ready in %s/gx with %s checkpoints", config.GX_ROOT, len(names))
    return names


def run_checkpoint(name: str, frame: pl.DataFrame, gate: str, target: str,
                   parameters: dict | None = None) -> list[dict]:
    """Validates `frame` with checkpoint `name`. `target` labels the batch (e.g. a period)."""
    context = get_context()
    checkpoint = context.checkpoints.get(name)
    needed = _parameters_needed(name)
    params = {k: v for k, v in (parameters or {}).items() if k in needed}
    result = checkpoint.run(
        batch_parameters={"dataframe": frame.to_pandas()},
        expectation_parameters=params,
    )

    rows = []
    for validation_result in result.run_results.values():
        for r in validation_result.results:
            cfg = r.expectation_config
            res = r.result or {}
            error = _error_message(r.exception_info)
            rows.append({
                "gate": gate,
                "checkpoint": name,
                "target": target,
                "rule_id": cfg.meta["dq_rule_id"],
                "severity": cfg.meta["severity"],
                "expectation": cfg.type,
                "column": cfg.kwargs.get("column") or cfg.kwargs.get("column_list") or cfg.kwargs.get("column_A"),
                "success": bool(r.success) and error is None,
                "element_count": res.get("element_count"),
                "unexpected_count": res.get("unexpected_count"),
                "unexpected_percent": res.get("unexpected_percent"),
                "observed_value": res.get("observed_value"),
                "examples": (res.get("partial_unexpected_list") or [])[:5],
                "error": error,
            })
    return rows


def _parameters_needed(checkpoint_name: str) -> set[str]:
    """Names of the $PARAMETER placeholders used by a checkpoint's expectations."""
    needed = set()
    for exp in checkpoint_specs()[checkpoint_name]["expectations"]:
        for value in exp.configuration.kwargs.values():
            if isinstance(value, dict) and "$PARAMETER" in value:
                needed.add(value["$PARAMETER"])
    return needed


def _error_message(exception_info) -> str | None:
    """GX reports exceptions in a flat dict (when fine) or one dict per metric (when it fails).

    An expectation that raised an exception is NEVER treated as passed.
    """
    if not exception_info:
        return None
    if "raised_exception" in exception_info:  # flat shape
        return str(exception_info.get("exception_message")) if exception_info["raised_exception"] else None
    messages = {str(i.get("exception_message")) for i in exception_info.values()
                if isinstance(i, dict) and i.get("raised_exception")}
    return "; ".join(sorted(messages)) or None
