"""Runs the whole load step: schema -> empty -> dimensions -> facts (each fact period commits on its own)."""
import argparse
import logging

from src import config
from src.load.db import create_schema, empty_warehouse, ensure_database_exists, get_connection
from src.load.load_dimensions import load_dimensions
from src.load.load_facts import load_facts
from src.utils import setup_logging, update_json_section

log = logging.getLogger(__name__)


def load_all(batch_id: str) -> dict:
    ensure_database_exists()
    conn = get_connection()
    try:
        create_schema(conn)
        empty_warehouse(conn)          # full refresh: safe to re-run
        dims = load_dimensions(conn, batch_id)
        facts = load_facts(conn, batch_id)
    finally:
        conn.close()
    summary = {"dimensions": dims, "facts": facts}
    update_json_section(config.run_dir(batch_id) / "transform_report.json", "load", summary)
    return summary


if __name__ == "__main__":
    setup_logging()
    parser = argparse.ArgumentParser(description="Load the prepared layer into the Data Warehouse.")
    parser.add_argument("--batch-id", required=True)
    load_all(parser.parse_args().batch_id)
