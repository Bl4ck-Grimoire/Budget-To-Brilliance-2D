"""Post-load reconciliation (section 15): what entered the DW equals what was accepted.

Compares, table by table and period by period, the rows in MySQL with the rows of the
prepared (accepted) layer, and runs integrity queries on the loaded data. The evidence is
written to data/runs/<batch>/reconciliation.json BEFORE raising, so a failure leaves proof.
"""
import logging

import polars as pl

from src import config
from src.load.db import get_connection
from src.utils import setup_logging, utc_now_iso, write_json

log = logging.getLogger(__name__)

INTEGRITY_QUERIES = {
    "fact_resultado_orphan_colegio": "SELECT COUNT(*) FROM fact_resultado_saber11 f LEFT JOIN dim_colegio c ON c.id_colegio = f.id_colegio WHERE c.id_colegio IS NULL",
    "fact_resultado_orphan_genero": "SELECT COUNT(*) FROM fact_resultado_saber11 f LEFT JOIN dim_genero g ON g.id_genero = f.id_genero WHERE g.id_genero IS NULL",
    "fact_resultado_orphan_periodo": "SELECT COUNT(*) FROM fact_resultado_saber11 f LEFT JOIN dim_periodo p ON p.id_periodo = f.id_periodo WHERE p.id_periodo IS NULL",
    "dim_colegio_without_departamento": "SELECT COUNT(*) FROM dim_colegio c LEFT JOIN dim_departamento d ON d.id_departamento = c.id_departamento WHERE d.id_departamento IS NULL",
    "real_schools_in_no_aplica_departamento": "SELECT COUNT(*) FROM dim_colegio WHERE id_colegio <> -1 AND id_departamento = -1",
    "fact_indicador_orphan_departamento": "SELECT COUNT(*) FROM fact_indicador_departamento i LEFT JOIN dim_departamento d ON d.id_departamento = i.id_departamento WHERE d.id_departamento IS NULL",
    "fact_indicador_orphan_anio": "SELECT COUNT(*) FROM fact_indicador_departamento i LEFT JOIN dim_anio a ON a.id_anio = i.id_anio WHERE a.id_anio IS NULL",
}


class ReconciliationError(Exception):
    """Raised when the DW does not match the accepted prepared data."""


def _count(cur, sql: str) -> int:
    cur.execute(sql)
    return int(cur.fetchone()[0])


def reconcile(batch_id: str) -> dict:
    prepared = config.prepared_dir(batch_id)
    checks = []

    def compare(name, expected, actual):
        checks.append({"check": name, "expected": expected, "loaded": actual, "ok": expected == actual})

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for table in ["dim_departamento", "dim_anio", "dim_genero", "dim_periodo", "dim_colegio"]:
                compare(f"rows {table}", pl.read_parquet(prepared / f"{table}.parquet").height,
                        _count(cur, f"SELECT COUNT(*) FROM {table}"))

            total_expected = 0
            for periodo in config.PERIODOS:
                expected = pl.scan_parquet(prepared / f"fact_resultado_{periodo}.parquet").select(pl.len()).collect().item()
                total_expected += expected
                cur.execute("SELECT COUNT(*) FROM fact_resultado_saber11 f JOIN dim_periodo p ON p.id_periodo = f.id_periodo WHERE p.periodo = %s", (periodo,))
                compare(f"rows fact_resultado_saber11 {periodo}", expected, int(cur.fetchone()[0]))
            compare("rows fact_resultado_saber11 TOTAL", total_expected, _count(cur, "SELECT COUNT(*) FROM fact_resultado_saber11"))

            compare("rows fact_indicador_departamento",
                    pl.read_parquet(prepared / "fact_indicador_departamento.parquet").height,
                    _count(cur, "SELECT COUNT(*) FROM fact_indicador_departamento"))
            for name, sql in INTEGRITY_QUERIES.items():
                compare(name, 0, _count(cur, sql))
    finally:
        conn.close()

    failed = [c for c in checks if not c["ok"]]
    result = {"batch_id": batch_id, "evaluated_at_utc": utc_now_iso(), "checks": checks,
              "failed": len(failed), "decision": "OK" if not failed else "MISMATCH"}
    write_json(config.run_dir(batch_id) / "reconciliation.json", result)
    log.info("[RECONCILE] %s checks, %s failed", len(checks), len(failed))
    if failed:
        raise ReconciliationError("DW does not match the prepared data: " + ", ".join(c["check"] for c in failed))
    return result


if __name__ == "__main__":
    import argparse
    setup_logging()
    parser = argparse.ArgumentParser(description="Reconcile the DW against the prepared layer.")
    parser.add_argument("--batch-id", required=True)
    reconcile(parser.parse_args().batch_id)
