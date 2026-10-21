"""Runs the whole pipeline locally, in the order the future Airflow DAG will follow.

    primary:  extract ICFES -> validate raw
    api:      extract MEN   -> validate raw
    then:     transform -> integrate -> validate prepared -> load -> reconcile

A Critical failure raises CriticalQualityFailure and the run stops where it happened.
Usage:  python -m src.run_pipeline [--batch-id ID]
"""
import argparse
import logging

from src import config
from src.extract import api_extract, primary_extract
from src.integrate.department_year import build_integration
from src.load.load_all import load_all
from src.load.reconcile import reconcile
from src.transform.icfes_transform import transform_primary
from src.transform.men_transform import transform_api
from src.utils import setup_logging
from src.validation import validate_api_raw, validate_prepared, validate_primary_raw

log = logging.getLogger(__name__)


def run(batch_id: str) -> None:
    log.info("=== batch %s ===", batch_id)
    primary_extract.extract_all(batch_id)
    validate_primary_raw.run(batch_id)
    api_extract.extract(batch_id)
    validate_api_raw.run(batch_id)

    transform_primary(batch_id)
    transform_api(batch_id)
    build_integration(batch_id)
    validate_prepared.run(batch_id)

    load_all(batch_id)
    reconcile(batch_id)
    log.info("=== batch %s finished OK ===", batch_id)


if __name__ == "__main__":
    setup_logging()
    parser = argparse.ArgumentParser(description="Run the full pipeline locally.")
    parser.add_argument("--batch-id", default=config.new_batch_id())
    run(parser.parse_args().batch_id)
