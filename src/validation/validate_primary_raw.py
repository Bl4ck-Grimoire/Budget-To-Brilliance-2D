"""Gate 1a - raw validation of the primary source (ICFES).

Question answered: is the extracted ICFES batch complete and structurally
usable, BEFORE any transformation touches it?

  * the manifest (one row per period file) is checked for period coverage and
    required columns (DQ-03, DQ-04);
  * each period's raw data is checked period by period (DQ-01 observation,
    DQ-06 numeric text, DQ-08, DQ-09, DQ-10).

A Critical failure raises CriticalQualityFailure and the primary branch stops.
"""
import argparse
import logging

import polars as pl

from src import config
from src.policy import apply_policy
from src.utils import read_json, setup_logging
from src.validation.expectations import GATE_PRIMARY_RAW
from src.validation.gx_runner import build_gx_project, run_checkpoint

log = logging.getLogger(__name__)

MANIFEST_COLUMNS = ["periodo", "source_file", "encoding", "separator", "n_columns",
                    "rows_read", "missing_required_columns"]


def run(batch_id: str, build_project: bool = True) -> dict:
    # Airflow builds the GX project once in its own task (prepare_gx_project): two gates running in
    # parallel must not rewrite the same gx/ files at the same time.
    if build_project:
        build_gx_project()
    raw_dir = config.raw_primary_dir(batch_id)
    manifest = read_json(raw_dir / "manifest.json")

    manifest_frame = pl.DataFrame([{c: f[c] for c in MANIFEST_COLUMNS} for f in manifest["files"]])
    results = run_checkpoint("icfes_raw_manifest", manifest_frame, GATE_PRIMARY_RAW, "manifest")

    for f in manifest["files"]:
        frame = pl.read_parquet(raw_dir / f["raw_file"])
        log.info("[%s] validating %s (%s rows)", GATE_PRIMARY_RAW, f["periodo"], frame.height)
        results += run_checkpoint("icfes_raw_data", frame, GATE_PRIMARY_RAW, f["periodo"])

    return apply_policy(GATE_PRIMARY_RAW, batch_id, results)


if __name__ == "__main__":
    setup_logging()
    parser = argparse.ArgumentParser(description="Validate the raw ICFES batch.")
    parser.add_argument("--batch-id", required=True)
    run(parser.parse_args().batch_id)
