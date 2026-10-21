"""Loads the two fact tables. Only accepted data reaches this step (the prepared gate already passed).

FACT_RESULTADO_SABER11 is loaded period by period with one commit per period, so memory stays
bounded and a failure never leaves a half-loaded period.
"""
import logging

import polars as pl

from src import config
from src.load.db import insert_frame

log = logging.getLogger(__name__)

FACT_RESULTADO_COLUMNS = [
    "estu_consecutivo", "id_colegio", "id_genero", "id_periodo", *config.PUNT_COLS, "puntaje_sospechoso",
]
FACT_INDICADOR_COLUMNS = ["id_departamento", "id_anio", *config.MEN_MEASURES]


def load_facts(conn, batch_id: str) -> dict:
    prepared = config.prepared_dir(batch_id)
    loaded = {"fact_resultado_saber11": {}, "fact_indicador_departamento": 0}

    for periodo in config.PERIODOS:
        frame = pl.read_parquet(prepared / f"fact_resultado_{periodo}.parquet")
        loaded["fact_resultado_saber11"][periodo] = insert_frame(conn, "fact_resultado_saber11", frame,
                                                                 FACT_RESULTADO_COLUMNS)
        conn.commit()
        log.info("[LOAD] fact_resultado_saber11 %s: %s rows", periodo, loaded["fact_resultado_saber11"][periodo])

    indicador = pl.read_parquet(prepared / "fact_indicador_departamento.parquet")
    loaded["fact_indicador_departamento"] = insert_frame(conn, "fact_indicador_departamento", indicador,
                                                         FACT_INDICADOR_COLUMNS)
    conn.commit()
    log.info("[LOAD] fact_indicador_departamento: %s rows", loaded["fact_indicador_departamento"])
    return loaded
