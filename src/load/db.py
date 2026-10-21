"""Database helpers for the Data Warehouse (MySQL)."""
import logging
from pathlib import Path

import pymysql

from src import config

log = logging.getLogger(__name__)

# Children first: used to empty the DW without breaking foreign keys
TABLES_CHILDREN_FIRST = [
    "fact_resultado_saber11", "fact_indicador_departamento",
    "dim_colegio", "dim_periodo", "dim_genero", "dim_anio", "dim_departamento",
]


def _connect(database: str | None, autocommit: bool):
    cfg = config.db_config()
    return pymysql.connect(
        host=cfg["host"], port=int(cfg["port"]), user=cfg["user"], password=cfg["password"],
        database=database, charset="utf8mb4", autocommit=autocommit, local_infile=False,
    )


def ensure_database_exists() -> None:
    """Connecting straight to a database that does not exist yet is rejected by MySQL."""
    dbname = config.db_config()["dbname"]
    conn = _connect(None, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{dbname}` CHARACTER SET utf8mb4")
    finally:
        conn.close()
    log.info("[LOAD] database '%s' verified/created", dbname)


def get_connection():
    return _connect(config.db_config()["dbname"], autocommit=False)


def create_schema(conn, sql_path: Path | None = None) -> None:
    """Runs the DDL. Comments are stripped line by line first, because a ';' inside a comment
    would otherwise break the split into statements."""
    sql_path = sql_path or config.SQL_DIR / "schema" / "create_dw.sql"
    lines = [line.split("--", 1)[0] for line in sql_path.read_text(encoding="utf-8").splitlines()]
    with conn.cursor() as cur:
        for statement in "\n".join(lines).split(";"):
            if statement.strip():
                cur.execute(statement)
    conn.commit()
    log.info("[LOAD] schema verified/created from %s (nothing was dropped)", sql_path.name)


def empty_warehouse(conn) -> None:
    """Full refresh strategy: the load always starts from empty tables, so a re-run gives the same
    result and never duplicates rows. TRUNCATE is used (a DELETE of 3.29M rows is far slower)."""
    with conn.cursor() as cur:
        cur.execute("SET FOREIGN_KEY_CHECKS = 0")
        for table in TABLES_CHILDREN_FIRST:
            cur.execute(f"TRUNCATE TABLE {table}")
        cur.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()
    log.info("[LOAD] warehouse emptied for a full refresh")


def insert_frame(conn, table: str, frame, columns: list[str], batch_size: int = 20_000) -> int:
    """Inserts a Polars frame with an EXPLICIT column list (so audit_* columns can never be loaded).

    pymysql rewrites executemany of an INSERT into multi-row statements, which is fast enough
    for ~360k rows per period without extra machinery.
    """
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    total = 0
    with conn.cursor() as cur:
        for chunk in frame.select(columns).iter_slices(batch_size):
            cur.executemany(sql, list(chunk.iter_rows()))
            total += chunk.height
    return total
