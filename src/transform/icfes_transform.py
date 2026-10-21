"""Transformation of the primary source (ICFES) into the prepared fact table.

Works period by period (about 360k rows each) so memory stays bounded even
with 3.29M rows in total. Implements T-05, T-07, T-08, T-09, T-10, T-11, T-14
(see src/transform/rules.py for the rationale of each one).

Nothing is hidden: rows that cannot be loaded are quarantined and counted, and
every period produces a reconciliation row (raw rows = prepared + removed +
quarantined) that the prepared gate validates (DQ-02).
"""
import logging
from pathlib import Path

import polars as pl

from src import config
from src.transform.dimensions import build_and_write_dimensions
from src.utils import update_json_section

log = logging.getLogger(__name__)

KEY = "estu_consecutivo"
FACT_COLUMNS = [
    "estu_consecutivo", "id_colegio", "id_genero", "id_periodo", *config.PUNT_COLS,
    "puntaje_sospechoso", "audit_tiene_colegio_origen", "audit_conversion_ok",
]


def split_duplicates(raw: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, int]:
    """T-09. Returns (clean rows, quarantined rows, number of exact copies removed).

    * exact copies of a row (same key and same values in every required column): keep the first;
    * a key with DIFFERENT values, or a null key: all its rows go to quarantine.
    """
    value_columns = [c for c in raw.columns if c != "periodo_archivo"]
    repeated = raw.filter(pl.col(KEY).is_not_null() & pl.col(KEY).is_duplicated())
    conflicting_keys = []
    if repeated.height:
        conflicting_keys = (
            repeated.group_by(KEY)
            .agg(pl.struct(value_columns).n_unique().alias("n_distinct"))
            .filter(pl.col("n_distinct") > 1)[KEY]
            .to_list()
        )
    quarantine_mask = pl.col(KEY).is_null() | pl.col(KEY).is_in(conflicting_keys)
    quarantine = raw.filter(quarantine_mask)
    kept = raw.filter(~quarantine_mask)
    clean = kept.unique(subset=[KEY], keep="first", maintain_order=True)
    return clean, quarantine, kept.height - clean.height


def transform_period(periodo: str, raw_dir: Path, out_dir: Path,
                     colegio_ids: pl.DataFrame, id_periodo: int) -> dict:
    raw = pl.read_parquet(raw_dir / f"icfes_{periodo}.parquet")
    clean, quarantine, n_exact = split_duplicates(raw)                           # T-09

    if quarantine.height:
        (out_dir / "quarantine").mkdir(parents=True, exist_ok=True)
        quarantine.write_parquet(out_dir / "quarantine" / f"icfes_{periodo}_conflictos.parquet")
        log.warning("[TRANSFORM] %s: %s rows sent to quarantine", periodo, quarantine.height)

    as_number = [pl.col(c).cast(pl.Float64, strict=False) for c in config.PUNT_COLS]   # T-14
    conversion_ok = pl.all_horizontal(
        [pl.col(c).is_null() | pl.col(c).cast(pl.Float64, strict=False).is_not_null() for c in config.PUNT_COLS]
    )

    fact = (
        clean
        .with_columns(
            conversion_ok.alias("audit_conversion_ok"),
            *[expr.alias(c) for expr, c in zip(as_number, config.PUNT_COLS)],
            pl.col("estu_genero").replace_strict({"F": 1, "M": 2}, default=-1).cast(pl.Int32).alias("id_genero"),  # T-10
            pl.col("cole_codigo_icfes").is_not_null().alias("audit_tiene_colegio_origen"),
            pl.lit(id_periodo, dtype=pl.Int32).alias("id_periodo"),                                              # T-05
        )
        .with_columns((pl.col("punt_global") == 0).fill_null(False).alias("puntaje_sospechoso"))                 # T-08
        .join(colegio_ids, on="cole_codigo_icfes", how="left")                                                   # T-11
        .with_columns(
            pl.when(pl.col("audit_tiene_colegio_origen")).then(pl.col("id_colegio"))
            .otherwise(pl.lit(-1, dtype=pl.Int32)).alias("id_colegio")
        )
        .select(FACT_COLUMNS)
    )
    fact.write_parquet(out_dir / f"fact_resultado_{periodo}.parquet")

    result = {
        "periodo": periodo,
        "filas_raw": raw.height,
        "duplicados_exactos_descartados": n_exact,
        "filas_en_cuarentena": quarantine.height,
        "filas_preparadas": fact.height,
        "diferencia_conciliacion": raw.height - n_exact - quarantine.height - fact.height,
        "filas_sin_colegio": int((~fact["audit_tiene_colegio_origen"]).sum()),
        "filas_genero_no_informado": int((fact["id_genero"] == -1).sum()),
        "filas_puntaje_cero": int(fact["puntaje_sospechoso"].sum()),
    }
    log.info("[TRANSFORM] %s: %s raw -> %s prepared (%s exact copies removed, %s quarantined)",
             periodo, raw.height, fact.height, n_exact, quarantine.height)
    return result


def transform_primary(batch_id: str) -> dict:
    raw_dir = config.raw_primary_dir(batch_id)
    out_dir = config.prepared_dir(batch_id)
    dims = build_and_write_dimensions(raw_dir, out_dir)

    dim_colegio = dims["dim_colegio"]
    colegio_ids = dim_colegio.filter(pl.col("id_colegio") != -1).select("cole_codigo_icfes", "id_colegio")
    period_ids = dict(zip(dims["dim_periodo"]["periodo"].to_list(), dims["dim_periodo"]["id_periodo"].to_list()))

    reconciliation = [transform_period(p, raw_dir, out_dir, colegio_ids, period_ids[p]) for p in config.PERIODOS]

    real = dim_colegio.filter(pl.col("id_colegio") != -1)
    report = {
        "reconciliation": reconciliation,
        "rows_raw": sum(r["filas_raw"] for r in reconciliation),
        "rows_prepared": sum(r["filas_preparadas"] for r in reconciliation),
        "schools": real.height,
        "schools_with_attribute_conflict": int(real["audit_conflicto_atributos"].sum()),
        "schools_with_attribute_conflict_pct": round(100 * real["audit_conflicto_atributos"].mean(), 4) if real.height else 0,
        "colegio_attr_policy": config.COLEGIO_ATTR_POLICY,
    }
    update_json_section(config.run_dir(batch_id) / "transform_report.json", "primary", report)
    return report
