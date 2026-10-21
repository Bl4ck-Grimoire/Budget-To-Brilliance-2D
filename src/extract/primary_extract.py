"""Primary-source extraction: ICFES Saber 11 .txt files -> raw Parquet.

What this step does (and does not do):
  * reads each period file with Polars, every column as text (no type guessing);
  * keeps only the columns the project needs (projection) and adds
    `periodo_archivo`, the period taken from the file name;
  * writes one Parquet per period and a manifest (file, encoding, separator,
    row count, missing columns, hash) that the raw gate validates.
  * It never cleans, converts or filters values: that belongs to transformation.

The original .txt files are left untouched.
"""
import argparse
import csv
import logging
import tempfile
from pathlib import Path

import polars as pl

from src import config
from src.utils import setup_logging, sha256_file, utc_now_iso, write_json

log = logging.getLogger(__name__)

_INTERNAL_SEP = "\x1f"  # single-byte separator used when the original one is multi-byte in UTF-8


def detect_encoding(path: Path, sample_bytes: int = 200_000) -> str:
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            with open(path, "r", encoding=enc) as f:
                f.read(sample_bytes)
            return enc
        except UnicodeDecodeError:
            continue
    return "latin-1"


def detect_separator(path: Path, encoding: str) -> str:
    with open(path, "r", encoding=encoding, errors="replace") as f:
        sample = f.read(65_536)
    try:
        return csv.Sniffer().sniff(sample, delimiters="|;,\t\xac").delimiter
    except csv.Error:
        first_line = sample.splitlines()[0] if sample else ""
        return max(["|", ";", ",", "\t", "\xac"], key=first_line.count)


def _prepare_for_polars(path: Path, encoding: str, separator: str, tmp_dir: Path):
    """Polars needs UTF-8 and a one-byte separator.

    If the file already satisfies both, it is read as is. Otherwise it is
    re-encoded in streaming blocks into a temporary UTF-8 copy (the separator
    '¬' used by DataIcfes is two bytes in UTF-8, so it is swapped for a
    one-byte control character).
    """
    if encoding == "utf-8" and len(separator.encode("utf-8")) == 1:
        return path, separator

    new_sep = separator if len(separator.encode("utf-8")) == 1 else _INTERNAL_SEP
    tmp_file = tmp_dir / f"{path.stem}_utf8.txt"
    with open(path, "r", encoding=encoding, newline="") as src, \
            open(tmp_file, "w", encoding="utf-8", newline="") as dst:
        while block := src.read(8 * 1024 * 1024):
            dst.write(block.replace(separator, new_sep) if new_sep != separator else block)
    return tmp_file, new_sep


def extract_period(periodo: str, out_dir: Path) -> dict:
    source = config.ICFES_SOURCE_DIR / config.periodo_to_filename(periodo)
    if not source.exists():
        raise FileNotFoundError(
            f"Could not find '{source}'. Place the DataIcfes .txt files in "
            f"{config.ICFES_SOURCE_DIR} or set ICFES_SOURCE_DIR in .env."
        )

    encoding = detect_encoding(source)
    separator = detect_separator(source, encoding)
    out_file = out_dir / f"icfes_{periodo}.parquet"

    with tempfile.TemporaryDirectory() as tmp:
        read_path, read_sep = _prepare_for_polars(source, encoding, separator, Path(tmp))
        lazy = pl.scan_csv(read_path, separator=read_sep, infer_schema=False)
        header = lazy.collect_schema().names()
        present = [c for c in config.ICFES_REQUIRED_COLUMNS if c in header]
        missing = [c for c in config.ICFES_REQUIRED_COLUMNS if c not in header]
        (
            lazy.select(present)
            .with_columns(pl.lit(periodo).alias("periodo_archivo"))
            .sink_parquet(out_file)
        )

    rows = pl.scan_parquet(out_file).select(pl.len()).collect().item()
    log.info("[EXTRACT] %s <- %s (enc=%s, sep=%r): %s rows, %s columns in file, %s required missing",
             periodo, source.name, encoding, separator, rows, len(header), len(missing))
    return {
        "periodo": periodo,
        "source_file": source.name,
        "encoding": encoding,
        "separator": separator,
        "n_columns": len(header),
        "rows_read": rows,
        "missing_required_columns": len(missing),
        "missing_columns_list": missing,
        "raw_file": out_file.name,
        "sha256_source": sha256_file(source),
    }


def extract_all(batch_id: str) -> dict:
    out_dir = config.raw_primary_dir(batch_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    files = [extract_period(p, out_dir) for p in config.PERIODOS]
    manifest = {
        "source": "ICFES Saber 11 (DataIcfes .txt)",
        "batch_id": batch_id,
        "extracted_at_utc": utc_now_iso(),
        "total_rows": sum(f["rows_read"] for f in files),
        "files": files,
    }
    write_json(out_dir / "manifest.json", manifest)
    log.info("[EXTRACT] primary source done: %s rows in %s periods", manifest["total_rows"], len(files))
    return manifest


if __name__ == "__main__":
    setup_logging()
    parser = argparse.ArgumentParser(description="Extract the ICFES files to raw Parquet.")
    parser.add_argument("--batch-id", default=config.new_batch_id())
    extract_all(parser.parse_args().batch_id)
