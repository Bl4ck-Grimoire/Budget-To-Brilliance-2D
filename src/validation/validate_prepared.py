"""Gate 2 - validation of the prepared layer (section 14 of the guide).

Question answered: is the prepared data fit to be loaded into the Data
Warehouse? A Critical failure here is what prevents the load.

Seven data objects are validated, always against the schema and keys the DW
will receive:
  reconciliation, dim_colegio, dim_departamento, dim_time (periodo + anio),
  fact_resultado (period by period), fact_indicador, integration.
Runtime parameters (valid ids, expected row counts, historical thresholds)
are computed here from the prepared dimensions, never from the facts under test.
"""
import argparse
import logging

import polars as pl

from src import config
from src.policy import apply_policy
from src.utils import read_json, setup_logging
from src.validation.expectations import GATE_PREPARED
from src.validation.gx_runner import build_gx_project, run_checkpoint

log = logging.getLogger(__name__)


def run(batch_id: str, build_project: bool = True) -> dict:
    # Airflow builds the GX project once in its own task (prepare_gx_project): two gates running in
    # parallel must not rewrite the same gx/ files at the same time.
    if build_project:
        build_gx_project()
    prepared = config.prepared_dir(batch_id)
    report = read_json(config.run_dir(batch_id) / "transform_report.json")

    dim_colegio = pl.read_parquet(prepared / "dim_colegio.parquet")
    dim_departamento = pl.read_parquet(prepared / "dim_departamento.parquet")
    dim_anio = pl.read_parquet(prepared / "dim_anio.parquet")
    dim_periodo = pl.read_parquet(prepared / "dim_periodo.parquet")
    dim_genero = pl.read_parquet(prepared / "dim_genero.parquet")
    indicador = pl.read_parquet(prepared / "fact_indicador_departamento.parquet")
    integration = pl.read_parquet(prepared / "integration_depto_anio.parquet")

    real_departamento_ids = dim_departamento.filter(pl.col("id_departamento") != -1)["id_departamento"].to_list()
    n_years = indicador["id_anio"].n_unique()
    params = {
        "valid_departamento_ids": dim_departamento["id_departamento"].to_list(),
        "real_departamento_ids": real_departamento_ids,
        "valid_colegio_ids": dim_colegio["id_colegio"].to_list(),
        "valid_genero_ids": dim_genero["id_genero"].to_list(),
        "valid_periodo_ids": dim_periodo["id_periodo"].to_list(),
        "valid_anio_ids": dim_anio["id_anio"].to_list(),
        "n_expected_rows": len(real_departamento_ids) * n_years,   # complete panel: 33 x years
        **{f"thr_{m}": t for m, t in report["api"]["yoy_thresholds_p99_history"].items()},
    }

    g = GATE_PREPARED
    results = []
    results += run_checkpoint("prepared_reconciliation", pl.DataFrame(report["primary"]["reconciliation"]), g, "reconciliation")
    results += run_checkpoint("prepared_dim_colegio", dim_colegio, g, "dim_colegio", params)
    results += run_checkpoint("prepared_dim_departamento", dim_departamento, g, "dim_departamento", params)
    time_frame = dim_periodo.join(dim_anio.rename({"anio": "anio_dim_anio"}), on="id_anio", how="left")
    results += run_checkpoint("prepared_dim_time", time_frame, g, "dim_periodo+dim_anio", params)
    for periodo in config.PERIODOS:
        frame = pl.read_parquet(prepared / f"fact_resultado_{periodo}.parquet")
        results += run_checkpoint("prepared_fact_resultado", frame, g, periodo, params)
    results += run_checkpoint("prepared_fact_indicador", indicador, g, "fact_indicador_departamento", params)
    results += run_checkpoint("prepared_integration", integration, g, "integration_depto_anio", params)

    return apply_policy(GATE_PREPARED, batch_id, results)


if __name__ == "__main__":
    setup_logging()
    parser = argparse.ArgumentParser(description="Validate the prepared layer before loading.")
    parser.add_argument("--batch-id", required=True)
    run(parser.parse_args().batch_id)
