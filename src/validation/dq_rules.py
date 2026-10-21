"""Catalog of the 28 data quality rules (single source of truth).

The Great Expectations suites read the severity from here, and
docs/traceability/dq_matrix.md is generated from here
(`python -m src.validation.dq_rules`), so code and documentation cannot drift.

Severity scale (course guide, section 8.1):
    Critical      -> stops the affected branch
    Warning       -> recorded, the run continues
    Informational -> recorded for monitoring

Removed during design review: DQ-14 (national average per period) and DQ-29
(minimum students per department-year), because those quantities are too
volatile to have a defensible theoretical range. IDs were not renumbered.
"""
from pathlib import Path

from src import config

C, W, I = "Critical", "Warning", "Informational"


def _rule(layer, attributes, dimension, rule, threshold, basis, severity, requirements):
    return {
        "layer": layer, "attributes": attributes, "dimension": dimension, "rule": rule,
        "threshold": threshold, "basis": basis, "severity": severity, "requirements": requirements,
    }


RULES = {
    # ------------------------------------------------------------------ ICFES
    "DQ-01": _rule(
        "ICFES raw + prepared (fact)", "estu_consecutivo, id_periodo", "Uniqueness",
        "An exam attempt appears once per period and its key is never null. Exact copies are removed (T-09); "
        "copies with different values are quarantined and block the load.",
        "Prepared: duplicates = 0 and quarantined rows = 0. Raw (observation, Warning): >= 99.99% unique.",
        "Historical: the First Delivery profile found 2 exact duplicates in 3.29M rows (period 2025-2). Great Expectations flags every copy of a duplicated key, so 2 duplicates count as 4 rows; the raw tolerance (0.01%) covers that.",
        C, "R1-R7"),
    "DQ-02": _rule(
        "ICFES raw -> prepared", "row counts per period", "Completeness",
        "Rows prepared = rows read - exact duplicates removed - rows quarantined.",
        "Difference = 0 for every period.",
        "Contractual: reconciliation required by the guide (sections 14-15).",
        C, "R1-R7"),
    "DQ-03": _rule(
        "ICFES raw (manifest)", "periodo", "Completeness",
        "The 9 periods 2021-2 ... 2025-2 are present and each has rows.",
        "9 of 9 periods; rows per period > 0.",
        "Project scope declared in the README (periods 2021-2 to 2025-2).",
        C, "R1-R7"),
    "DQ-04": _rule(
        "ICFES raw (manifest)", "required columns of the ETL", "Consistency",
        "Every period file contains the columns the ETL uses, even if the rest of the schema changes.",
        "Missing required columns = 0 per file (changes in other columns are recorded in the manifest).",
        "Contractual: the pipeline cannot run without them. The ICFES schema changed in 5 of 9 periods (README section 6).",
        C, "R1-R7"),
    "DQ-05": _rule(
        "ICFES prepared (fact)", "punt_global, punt_* by area", "Validity",
        "Scores are inside the official ICFES range.",
        "punt_global in [0, 500] (Critical). Area scores in [0, 100] (Warning, range to confirm in the first real run).",
        "Official scale of the exam; the First Delivery already validated punt_global in [0, 500].",
        C, "R1-R7"),
    "DQ-06": _rule(
        "ICFES raw + prepared", "punt_*", "Validity",
        "Every non-null raw score is numeric and the numeric conversion creates no new nulls.",
        "Non-numeric raw values = 0; rows with a lost score after conversion = 0.",
        "The First Delivery converted with errors='coerce', which would hide this silently.",
        C, "R1-R7"),
    "DQ-07": _rule(
        "ICFES prepared (fact)", "punt_global, puntaje_sospechoso", "Validity",
        "Every global score equal to 0 is flagged as suspicious and excluded from performance KPIs; the flagged share is monitored.",
        "Unflagged zeros = 0; flagged share <= 0.01%.",
        "Historical: 28 rows (0.00085%) in the README profile; 0.01% leaves roughly a 10x margin.",
        W, "R1-R7"),
    "DQ-08": _rule(
        "ICFES raw + prepared", "estu_genero, id_genero", "Completeness",
        "Null or unknown gender is rare; it maps to id_genero = -1 and only leaves R5.",
        "Nulls and values outside the 2 valid ones <= 0.01%.",
        "Historical: 141 nulls (0.004%) in the README profile.",
        I, "R5"),
    "DQ-09": _rule(
        "ICFES raw + prepared", "cole_area_ubicacion", "Validity",
        "Raw: URBANO, URBANA or RURAL. Prepared: only URBANO, RURAL or 'No aplica'; mandatory for real schools.",
        "Out-of-domain values = 0; nulls for real schools = 0.",
        "README section 6 (URBANA/URBANO harmonized); NOT NULL in the DW schema.",
        C, "R2, R3"),
    "DQ-10": _rule(
        "ICFES raw + prepared", "cole_naturaleza, cole_jornada", "Validity",
        "Values belong to the closed domains and have no encoding damage (for example the N-tilde in MAÑANA).",
        "naturaleza: 2 values; jornada: 6 values; nulls for real schools = 0.",
        "README section 6 and the domains declared in the DW schema.",
        C, "R1, R2"),
    "DQ-11": _rule(
        "ICFES prepared (fact)", "id_colegio, cole_codigo_icfes", "Consistency",
        "id_colegio = -1 only when the record has no school at the origin; a real school can never fall into -1.",
        "Exceptions = 0 (historical reference: 451,150 rows, 13.73%, in the First Delivery).",
        "Structural. The First Delivery assigned -1 to any key not found (fillna(-1)), which masks mapping failures.",
        C, "R1-R4, R6, R7"),
    "DQ-12": _rule(
        "ICFES prepared (dim_colegio)", "school attributes (nature, area, department)", "Consistency",
        "A school keeps the same attributes in every period (the dimension stores one value per school, T-13).",
        ">= 99% of schools without conflicts. Shift (jornada) is excluded because it can change legitimately.",
        "Operational: if <= 1% of schools change, group averages move by less than 1% since each school weighs proportionally.",
        W, "R1-R4, R6, R7"),
    "DQ-13": _rule(
        "ICFES prepared (fact)", "id_colegio, id_genero, id_periodo", "Integrity",
        "No foreign key is null or orphan.",
        "Nulls = 0; orphans = 0 on the 3 keys.",
        "Structural; equivalent to First Delivery checks V3 and V5.",
        C, "R1-R7"),
    # -------------------------------------------------------------------- MEN API
    "DQ-15": _rule(
        "API raw (manifest)", "all required fields", "Completeness",
        "Paging brings every row the service reports, with all required fields.",
        "Rows downloaded = service count (difference 0); missing required fields = 0.",
        "Contractual. A previous export of another MEN dataset stopped at exactly 1,000 rows, the usual Socrata limit.",
        C, "R6, R7"),
    "DQ-16": _rule(
        "API raw", "c_digo_departamento, ano", "Uniqueness",
        "One record per department and year.",
        "Duplicates = 0.",
        "Structural: it is the grain of FACT_INDICADOR_DEPARTAMENTO.",
        C, "R6, R7"),
    "DQ-17": _rule(
        "API prepared (window)", "id_departamento, id_anio", "Completeness",
        "The window is a complete panel: all 33 departments for every year that appears.",
        "Rows = 33 x number of years in the window; 33 distinct departments.",
        "Structural: declared grain department x year.",
        C, "R6, R7"),
    "DQ-18": _rule(
        "API prepared (window)", "desercion_media, repitencia_secundaria", "Completeness",
        "Both measures are informed in every row.",
        "Nulls = 0.",
        "The two requirements cannot be computed without them (profile: 0 nulls).",
        C, "R6, R7"),
    "DQ-19": _rule(
        "API raw + prepared", "desercion_media, repitencia_secundaria, ano", "Validity",
        "Raw: numeric text. Prepared: valid percentages.",
        "Non-numeric = 0; values outside [0, 100] = 0.",
        "Analytical: they are rates by definition.",
        C, "R6, R7"),
    "DQ-20": _rule(
        "API prepared (window)", "yearly change of both measures", "Accuracy",
        "The year-over-year change of each department is plausible compared with its history.",
        "|change| <= P99 of |change| in the years before the window (computed every run).",
        "Historical: derived from 2012-2021, never tuned on the window. Current values: 6.72 pp (dropout) and 8.78 pp (repetition).",
        W, "R6, R7"),
    "DQ-21": _rule(
        "API raw + prepared (dim_departamento)", "c_digo_departamento, cod_dane_depto", "Validity",
        "The code, padded to 2 digits, belongs to the 33 DANE departments; the name is never used as key.",
        "Codes outside the list = 0; unpadded codes after T-02 = 0.",
        "DANE codes. The name varies between years in the API (for example 'Bogotá, D,C,' in 2021).",
        C, "R6, R7"),
    "DQ-22": _rule(
        "API raw", "desercion_media, repitencia_secundaria; aprobacion, reprobacion, desercion", "Consistency",
        "The measures we load are not copies of other columns, and approval + failure + dropout = 100 for the rows that will be loaded.",
        "Share identical to another column <= 50%; |sum - 100| <= 0.5.",
        "Analytical: the three rates split enrolled students. Observed copies were >= 79% identical and coincidences <= 0.4%. "
        "Nariño 2011 sums 101.54, so the check only covers the loaded window.",
        C, "R6, R7"),
    "DQ-23": _rule(
        "API prepared -> DW", "columns loaded", "Validity",
        "Only the approved columns are loaded; the 7 defective ones never enter the DW.",
        "Exact column set of the indicator fact.",
        "Profiling excluded copied or empty columns (see docs/integration_contract.md).",
        C, "R6, R7"),
    "DQ-24": _rule(
        "API raw (manifest)", "ano", "Timeliness",
        "The last published year is monitored.",
        f"Last year = {config.MEN_LAST_YEAR_EXPECTED}; a later year raises a notice to review the window.",
        "Contractual: the profiled dataset ends in 2024.",
        I, "R6, R7"),
    # -------------------------------------------------------------- integration in the DW
    "DQ-25": _rule(
        "DW prepared (fact_indicador_departamento)", "id_departamento, id_anio", "Integrity",
        "Keys are unique, not null, not -1 and exist in their dimensions.",
        "Duplicates = 0; nulls = 0; orphans = 0.",
        "Structural.",
        C, "R6, R7"),
    "DQ-26": _rule(
        "DW prepared (dim_colegio -> dim_departamento)", "cole_depto_ubicacion, id_departamento", "Integrity",
        "Every real school is linked to a DANE department; -1 is only for the 'No aplica' member.",
        "Real schools without department = 0; 33 distinct departments.",
        "Structural: the ICFES stores the department only as free text, so a normalization table is needed (T-06).",
        C, "R6, R7"),
    "DQ-27": _rule(
        "DW prepared (dim_periodo -> dim_anio)", "id_anio, anio", "Consistency",
        "Each period points to its year and dim_anio holds exactly the years of the project periods.",
        "Mismatches = 0; years = 2021-2025.",
        "Structural: dim_anio is the conformed time key shared by both fact tables.",
        C, "R6, R7"),
    "DQ-28": _rule(
        "DW prepared (integration)", "departamento x año", "Completeness",
        "Every department-year with ICFES examinees in the window has its MEN row, and every MEN row has examinees.",
        "Unmatched department-years = 0.",
        "Structural.",
        C, "R6, R7"),
    "DQ-30": _rule(
        "DW prepared (integration)", "departamento x año", "Consistency",
        "ICFES is aggregated to department x year BEFORE joining; the result has one row per department-year.",
        "Duplicates = 0; rows = 33 x number of years in the window.",
        "Structural: prevents row explosion (fan-out) when joining the two fact tables.",
        C, "R6, R7"),
}


def export_matrix(path: Path) -> None:
    """Writes docs/traceability/dq_matrix.md in the format of the course template."""
    from src.validation.expectations import checkpoint_specs

    where: dict[str, set[str]] = {}
    for checkpoint, spec in checkpoint_specs().items():
        for exp in spec["expectations"]:
            where.setdefault(exp.meta["dq_rule_id"], set()).add(checkpoint)

    lines = [
        "# Data Quality Matrix",
        "",
        "Generated from `src/validation/dq_rules.py` (do not edit by hand). "
        "Severity scale: Critical stops the branch, Warning is recorded and the run continues, "
        "Informational is recorded only.",
        "",
        "| Rule ID | Source or Layer | Attribute(s) | Quality Dimension | Quality Rule | Metric or Threshold | Severity | Related Requirement | Implemented in (GX checkpoint) |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for rule_id, r in RULES.items():
        checkpoints = ", ".join(f"`{c}`" for c in sorted(where.get(rule_id, [])))
        lines.append(
            f"| {rule_id} | {r['layer']} | {r['attributes']} | {r['dimension']} | {r['rule']} | "
            f"{r['threshold']} | {r['severity']} | {r['requirements']} | {checkpoints} |"
        )
    lines += ["", "## Basis of each threshold", ""]
    for rule_id, r in RULES.items():
        lines.append(f"- **{rule_id}**: {r['basis']}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    export_matrix(config.PROJECT_ROOT / "docs" / "traceability" / "dq_matrix.md")
    print("dq_matrix.md written")
