"""API extraction: MEN education statistics by department (datos.gov.co, ji8i-4anb).

The WHOLE dataset is downloaded (all years). The year filter (ano > 2021) is
applied later, in the transformation step (T-01). Filtering here would hide
from the raw gate exactly what DQ-15 has to measure: that the download is
complete compared with the count reported by the service.

HTTP is done with `requests` (Polars has no HTTP client with query
parameters); everything else (frames, types, Parquet) is Polars.
"""
import argparse
import logging

import polars as pl
import requests

from src import config
from src.utils import setup_logging, sha256_file, utc_now_iso, write_json

log = logging.getLogger(__name__)


def _get(params: dict) -> list[dict]:
    headers = {"Accept": "application/json"}
    if config.SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = config.SOCRATA_APP_TOKEN
    response = requests.get(config.MEN_API_URL, params=params, headers=headers,
                            timeout=config.MEN_API_TIMEOUT)
    response.raise_for_status()
    return response.json()


def fetch_service_count() -> int:
    """Row count reported by the service itself (reference for DQ-15)."""
    rows = _get({"$select": "count(*)"})
    return int(next(iter(rows[0].values())))


def fetch_all_rows(page_size: int) -> tuple[list[dict], int]:
    """Pages through the dataset until it is exhausted. Ordering by :id keeps pages stable."""
    records, offset, pages = [], 0, 0
    while True:
        page = _get({"$limit": page_size, "$offset": offset, "$order": ":id"})
        if not page:
            break
        records.extend(page)
        pages += 1
        offset += page_size
        if len(page) < page_size:
            break
    return records, pages


def extract(batch_id: str) -> dict:
    out_dir = config.raw_api_dir(batch_id)
    out_dir.mkdir(parents=True, exist_ok=True)

    service_count = fetch_service_count()
    records, pages = fetch_all_rows(config.MEN_API_PAGE_SIZE)

    # Union of keys over all records: Socrata omits fields that are null in a row.
    df = pl.from_dicts(records, infer_schema_length=None).cast(pl.String)
    out_file = out_dir / config.MEN_RAW_FILE
    df.write_parquet(out_file)

    missing = [c for c in config.MEN_REQUIRED_FIELDS if c not in df.columns]
    years = df["ano"].cast(pl.Int32, strict=False).drop_nulls() if "ano" in df.columns else pl.Series([], dtype=pl.Int32)

    manifest = {
        "source": "MEN Estadísticas en educación PBM por departamento (ji8i-4anb)",
        "batch_id": batch_id,
        "url": config.MEN_API_URL,
        "extracted_at_utc": utc_now_iso(),
        "service_count": service_count,
        "rows_downloaded": df.height,
        "pages": pages,
        "page_size": config.MEN_API_PAGE_SIZE,
        "n_columns": df.width,
        "missing_required_fields": len(missing),
        "missing_fields_list": missing,
        "min_ano": int(years.min()) if len(years) else None,
        "max_ano": int(years.max()) if len(years) else None,
        "raw_file": out_file.name,
        "sha256": sha256_file(out_file),
    }
    write_json(out_dir / "manifest.json", manifest)
    log.info("[EXTRACT] MEN API: %s rows downloaded (service reports %s), %s pages, years %s-%s",
             df.height, service_count, pages, manifest["min_ano"], manifest["max_ano"])
    return manifest


if __name__ == "__main__":
    setup_logging()
    parser = argparse.ArgumentParser(description="Download the MEN dataset to raw Parquet.")
    parser.add_argument("--batch-id", default=config.new_batch_id())
    extract(parser.parse_args().batch_id)
