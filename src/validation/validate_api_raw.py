"""Gate 1b - raw validation of the API source (MEN, ji8i-4anb).

Question answered: did the download bring everything the service reports, and
are the columns we rely on trustworthy, BEFORE any transformation?

Three data objects are validated:
  * the extraction manifest        (DQ-15 completeness, DQ-24 last year);
  * the raw data                   (DQ-16 uniqueness, DQ-19 numeric text, DQ-21 codes, DQ-22 canary);
  * a column profile               (DQ-22 the measures are not copies of other columns).

The raw data itself is not modified. The only derived column is
`suma_aprob_reprob_deser`, computed for validation only, and only for the rows
that will be loaded (ano > 2021): in 2011 Nariño the three rates add up to
101.54, a historical row that is outside the scope of R6 and R7.
"""
import argparse
import logging

import polars as pl

from src import config
from src.policy import apply_policy
from src.utils import read_json, setup_logging
from src.validation.expectations import GATE_API_RAW
from src.validation.gx_runner import build_gx_project, run_checkpoint

log = logging.getLogger(__name__)

KEY_COLUMNS = ("ano", "c_digo_departamento", "departamento")
CANARY_COLUMNS = ("aprobacion", "reprobacion", "desercion")
MANIFEST_COLUMNS = ["rows_downloaded", "service_count", "missing_required_fields", "max_ano", "pages"]


def add_canary_column(raw: pl.DataFrame) -> pl.DataFrame:
    """Adds the validation-only column: approval + failure + dropout (rows with ano > 2021)."""
    if not all(c in raw.columns for c in (*CANARY_COLUMNS, "ano")):
        return raw.with_columns(pl.lit(None, dtype=pl.Float64).alias("suma_aprob_reprob_deser"))
    parts = [pl.col(c).cast(pl.Float64, strict=False) for c in CANARY_COLUMNS]
    ano = pl.col("ano").cast(pl.Int32, strict=False)
    total = pl.when(ano > config.MEN_MIN_YEAR_EXCLUSIVE).then(parts[0] + parts[1] + parts[2]).otherwise(None)
    return raw.with_columns(total.alias("suma_aprob_reprob_deser"))


def column_profile(raw: pl.DataFrame) -> pl.DataFrame:
    """For each measure we load: the highest % of rows in which it equals ANOTHER column."""
    others = [c for c in raw.columns if c not in KEY_COLUMNS]
    rows = []
    for measure in config.MEN_MEASURES:
        if measure not in raw.columns:
            rows.append({"columna": measure, "max_pct_identica_otra_columna": 100.0})
            continue
        shares = [(raw[measure] == raw[c]).sum() / raw.height * 100 for c in others if c != measure]
        rows.append({"columna": measure, "max_pct_identica_otra_columna": float(max(shares, default=0.0))})
    return pl.DataFrame(rows)


def run(batch_id: str, build_project: bool = True) -> dict:
    # Airflow builds the GX project once in its own task (prepare_gx_project): two gates running in
    # parallel must not rewrite the same gx/ files at the same time.
    if build_project:
        build_gx_project()
    raw_dir = config.raw_api_dir(batch_id)
    manifest = read_json(raw_dir / "manifest.json")
    raw = pl.read_parquet(raw_dir / config.MEN_RAW_FILE)

    manifest_frame = pl.DataFrame([{c: manifest[c] for c in MANIFEST_COLUMNS}])
    results = run_checkpoint("men_raw_manifest", manifest_frame, GATE_API_RAW, "manifest")
    results += run_checkpoint("men_raw_data", add_canary_column(raw), GATE_API_RAW, "raw")
    results += run_checkpoint("men_raw_column_profile", column_profile(raw), GATE_API_RAW, "column_profile")
    return apply_policy(GATE_API_RAW, batch_id, results)


if __name__ == "__main__":
    setup_logging()
    parser = argparse.ArgumentParser(description="Validate the raw MEN API batch.")
    parser.add_argument("--batch-id", required=True)
    run(parser.parse_args().batch_id)
