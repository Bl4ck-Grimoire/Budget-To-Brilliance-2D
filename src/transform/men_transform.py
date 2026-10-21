"""Transformation of the API source (MEN) into FACT_INDICADOR_DEPARTAMENTO.

Order of the steps matters:
  T-04  text -> numbers (ano, measures)
  T-02  department code padded to 2 digits
  --    yearly change per department, computed on the WHOLE history, so the
        first year of the window (2022) can be compared with 2021
  --    historical thresholds for DQ-20 (P99 of the changes up to 2021)
  T-01  keep only ano > 2021               <- the year filter lives HERE
  T-03  keep only the approved columns
  --    link to the shared dimensions (id_departamento, id_anio)

Rows are never repaired: anything that does not fit ends as null/orphan and
the prepared gate (DQ-18, DQ-25) stops the branch.
"""
import logging

import polars as pl

from src import config
from src.utils import update_json_section

log = logging.getLogger(__name__)

NO_HISTORY_THRESHOLD = 1e9  # used only if there is no history before the window (the check then cannot fail)


def transform_api(batch_id: str) -> dict:
    raw = pl.read_parquet(config.raw_api_dir(batch_id) / config.MEN_RAW_FILE)
    prepared = config.prepared_dir(batch_id)
    dim_departamento = pl.read_parquet(prepared / "dim_departamento.parquet")
    dim_anio = pl.read_parquet(prepared / "dim_anio.parquet")

    typed = raw.select(                                                         # T-04, T-02
        pl.col("ano").cast(pl.Int32, strict=False),
        pl.col("c_digo_departamento").str.zfill(2).alias("cod_dane_depto"),
        *[pl.col(m).cast(pl.Float64, strict=False) for m in config.MEN_MEASURES],
    ).sort("cod_dane_depto", "ano")

    typed = typed.with_columns(
        *[(pl.col(m) - pl.col(m).shift(1).over("cod_dane_depto")).abs().alias(f"audit_cambio_abs_{m}")
          for m in config.MEN_MEASURES]
    )

    history = typed.filter(pl.col("ano") <= config.MEN_MIN_YEAR_EXCLUSIVE)
    thresholds = {}
    for m in config.MEN_MEASURES:
        changes = history[f"audit_cambio_abs_{m}"].drop_nulls()
        thresholds[m] = float(changes.quantile(config.YOY_PERCENTILE)) if len(changes) else NO_HISTORY_THRESHOLD

    window = typed.filter(pl.col("ano") > config.MEN_MIN_YEAR_EXCLUSIVE)        # T-01

    fact = (
        window
        .join(dim_departamento.select("cod_dane_depto", "id_departamento"), on="cod_dane_depto", how="left")
        .join(dim_anio.rename({"anio": "ano"}), on="ano", how="left")
        .select("id_departamento", "id_anio", *config.MEN_MEASURES,              # T-03
                *[f"audit_cambio_abs_{m}" for m in config.MEN_MEASURES])
    )
    fact.write_parquet(prepared / "fact_indicador_departamento.parquet")

    report = {
        "rows_raw": raw.height,
        "rows_window": fact.height,
        "min_year_exclusive": config.MEN_MIN_YEAR_EXCLUSIVE,
        "years_in_window": sorted(window["ano"].unique().to_list()),
        "yoy_thresholds_p99_history": thresholds,
        "history_changes_used": int(sum(len(history[f"audit_cambio_abs_{m}"].drop_nulls()) for m in config.MEN_MEASURES)),
    }
    update_json_section(config.run_dir(batch_id) / "transform_report.json", "api", report)
    log.info("[TRANSFORM] MEN: %s raw rows -> %s in the window %s; DQ-20 thresholds %s",
             raw.height, fact.height, report["years_in_window"], {k: round(v, 2) for k, v in thresholds.items()})
    return report
