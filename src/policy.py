"""Severity policy: turns validation results into a pipeline decision.

Great Expectations only *detects* violations. This module decides what the
pipeline does with them (course guide, section 8.1):

    Critical      -> the affected branch stops; downstream work does not run.
    Warning       -> recorded and visible; the run continues.
    Informational -> recorded for monitoring; never blocks.

The evidence file is written BEFORE any exception is raised, so a blocked
run still leaves its validation result on disk.
"""
import logging

from src import config
from src.utils import utc_now_iso, write_json

log = logging.getLogger(__name__)

POLICY = {
    "Critical": "STOP",
    "Warning": "CONTINUE_AND_RECORD",
    "Informational": "RECORD",
}


class CriticalQualityFailure(Exception):
    """Raised when a Critical rule fails: the branch must not continue."""


def apply_policy(gate: str, batch_id: str, results: list[dict]) -> dict:
    failed = [r for r in results if not r["success"]]
    by_severity = {s: [r for r in failed if r["severity"] == s] for s in POLICY}

    decision = "CONTINUE"
    if by_severity["Critical"]:
        decision = "STOP"
    elif by_severity["Warning"]:
        decision = "CONTINUE_WITH_WARNINGS"

    summary = {
        "gate": gate,
        "batch_id": batch_id,
        "evaluated_at_utc": utc_now_iso(),
        "checks_total": len(results),
        "checks_failed": len(failed),
        "failed_by_severity": {s: len(v) for s, v in by_severity.items()},
        "decision": decision,
        "results": results,
    }
    write_json(config.run_dir(batch_id) / "validation" / f"{gate}.json", summary)

    log_by_severity = {"Critical": log.error, "Warning": log.warning, "Informational": log.info}
    for severity, items in by_severity.items():
        for r in items:
            log_by_severity[severity](
                "[%s] %s %s on %s failed: unexpected=%s observed=%s",
                gate, r["rule_id"], r["expectation"], r["target"],
                r["unexpected_count"], r["observed_value"],
            )

    log.info("[%s] %s checks, %s failed -> %s", gate, len(results), len(failed), decision)
    if decision == "STOP":
        rules = sorted({r["rule_id"] for r in by_severity["Critical"]})
        raise CriticalQualityFailure(f"Gate '{gate}' blocked by Critical rules: {', '.join(rules)}")
    return summary
