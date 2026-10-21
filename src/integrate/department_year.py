"""Integration of both sources at the common grain: department x year (T-12).

Rule of the model: the two fact tables are never joined row by row. The ICFES
fact (student grain, ~3.29M rows) is first AGGREGATED to department x year and
only then joined with the MEN indicator fact (department x year grain).
Joining before aggregating would repeat every MEN value once per student and
corrupt every average.

Students counted in the average are the valid ones: they belong to a real
school (id_colegio <> -1, so they have a department), their score is not
flagged as suspicious and the score is present.

The join is FULL so that a department-year present in only one source stays
visible (audit_en_icfes / audit_en_men) and the prepared gate can stop it (DQ-28).
"""
import logging

import polars as pl

from src import config
from src.utils import update_json_section

log = logging.getLogger(__name__)


def build_integration(batch_id: str) -> pl.DataFrame:
    prepared = config.prepared_dir(batch_id)
    dim_colegio = pl.read_parquet(prepared / "dim_colegio.parquet").select("id_colegio", "id_departamento")
    dim_periodo = pl.read_parquet(prepared / "dim_periodo.parquet").select("id_periodo", "id_anio")
    indicador = pl.read_parquet(prepared / "fact_indicador_departamento.parquet")
    window_years = indicador["id_anio"].unique().to_list()

    icfes = (
        pl.scan_parquet(prepared / "fact_resultado_*.parquet")
        .filter((pl.col("id_colegio") != -1) & (~pl.col("puntaje_sospechoso")) & pl.col("punt_global").is_not_null())
        .select("id_colegio", "id_periodo", "punt_global")
        .join(dim_colegio.lazy(), on="id_colegio")
        .join(dim_periodo.lazy(), on="id_periodo")
        .filter(pl.col("id_anio").is_in(window_years))
        .group_by("id_departamento", "id_anio")
        .agg(pl.col("punt_global").mean().alias("punt_global_prom"),
             pl.len().alias("n_estudiantes"))
        .with_columns(pl.lit(True).alias("audit_en_icfes"))
        .collect(engine="streaming")
    )

    men = (indicador.select("id_departamento", "id_anio", *config.MEN_MEASURES)
           .with_columns(pl.lit(True).alias("audit_en_men")))

    integration = (
        icfes.join(men, on=["id_departamento", "id_anio"], how="full", coalesce=True)
        .with_columns(pl.col("audit_en_icfes").fill_null(False), pl.col("audit_en_men").fill_null(False))
        .sort("id_departamento", "id_anio")
    )
    integration.write_parquet(prepared / "integration_depto_anio.parquet")

    covered_periods = dim_periodo.filter(pl.col("id_anio").is_in(window_years)).height
    report = {
        "rows": integration.height,
        "department_years_with_icfes": int(integration["audit_en_icfes"].sum()),
        "department_years_with_men": int(integration["audit_en_men"].sum()),
        "periods_with_men_indicators": covered_periods,
        "periods_total": dim_periodo.height,
    }
    update_json_section(config.run_dir(batch_id) / "transform_report.json", "integration", report)
    log.info("[INTEGRATE] %s department-years; %s of %s ICFES periods have MEN indicators",
             integration.height, covered_periods, dim_periodo.height)
    return integration
