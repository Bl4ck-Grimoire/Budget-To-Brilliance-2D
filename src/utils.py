"""Small helpers shared by the pipeline."""
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s | %(message)s")
    logging.getLogger("great_expectations").setLevel(logging.WARNING)  # GX is very chatty at INFO


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def update_json_section(path: Path, section: str, content: dict) -> None:
    """Adds or replaces one section of a JSON report (used by the transformation report)."""
    report = read_json(path) if path.exists() else {}
    report[section] = content
    write_json(path, report)


def sha256_file(path: Path, block: int = 1 << 20) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(block):
            digest.update(chunk)
    return digest.hexdigest()
