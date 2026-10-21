"""Loads the five dimensions (they must exist before any fact that references them)."""
import logging

import polars as pl

from src import config
from src.load.db import insert_frame

log = logging.getLogger(__name__)

# Explicit whitelist per table: only these columns reach the DW (audit_* columns never do)
DIMENSIONS = [
    ("dim_departamento", ["id_departamento", "cod_dane_depto", "nombre_departamento"]),
    ("dim_anio", ["id_anio", "anio"]),
    ("dim_genero", ["id_genero", "estu_genero", "genero_descripcion"]),
    ("dim_periodo", ["id_periodo", "periodo", "anio", "semestre", "id_anio"]),
    ("dim_colegio", ["id_colegio", "cole_codigo_icfes", "cole_nombre_establecimiento", "cole_naturaleza",
                     "cole_jornada", "cole_area_ubicacion", "cole_depto_ubicacion", "cole_mcpio_ubicacion",
                     "id_departamento"]),
]


def load_dimensions(conn, batch_id: str) -> dict:
    prepared = config.prepared_dir(batch_id)
    loaded = {}
    for table, columns in DIMENSIONS:
        frame = pl.read_parquet(prepared / f"{table}.parquet")
        loaded[table] = insert_frame(conn, table, frame, columns)
        log.info("[LOAD] %s: %s rows", table, loaded[table])
    conn.commit()
    return loaded
