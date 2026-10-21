"""Transformation catalog (T-01 ... T-14) and small pure helpers.

The catalog is the documentation of every business transformation: why it
exists, which fields it touches, what the data looked like before and after,
what happens to values that do not fit, and what it changes analytically.
`docs/transformations.md` is generated from it
(`python -m src.transform.rules`), so the table in the report always matches
the code that implements it.
"""
import re
import unicodedata
from pathlib import Path

from src import config


def _t(tid, rationale, fields, before_after, exception, impact):
    return {"id": tid, "rationale": rationale, "fields": fields, "before_after": before_after,
            "exception": exception, "impact": impact}


TRANSFORMATIONS = [
    _t("T-01",
       "Scope of R6 and R7: only years after 2021. Applied in transformation, after the raw gate, so the "
       "raw validation still sees the whole dataset (DQ-15 needs the full download to compare with the service count).",
       "ano",
       "Before: 462 rows (2011-2024). After: 99 rows (33 departments x 2022-2024). The yearly change of each "
       "department is computed BEFORE the filter, so 2022 can be compared with 2021 (DQ-20).",
       "A non-numeric year cannot reach this step: DQ-19 (Critical) stops the branch in the raw gate.",
       "Defines the loadable set of FACT_INDICADOR_DEPARTAMENTO."),
    _t("T-02",
       "The API returns the department code without the leading zero and DANE uses 2 digits. The name is never a key.",
       "c_digo_departamento -> cod_dane_depto",
       "'5' -> '05'; '11' stays '11'.",
       "A code outside the 33 DANE codes does not find its dimension row: id_departamento stays null and DQ-25 "
       "(Critical) stops the branch.",
       "Without this step the departments with a one-digit code could not be joined with the ICFES."),
    _t("T-03",
       "Only the columns approved in profiling enter the prepared layer; the defective ones (copied or empty) "
       "are not repaired because their true value is unknown. Raw keeps all 37 columns.",
       "7 excluded: aprobacion_secundaria, reprobacion_primaria, reprobacion_transicion, repitencia_transicion, "
       "aprobacion_transicion, tamano_promedio_grupo, sedes_conectadas_a_internet",
       "Before: 37 columns. After: id_departamento, id_anio, desercion_media, repitencia_secundaria (+ audit columns).",
       "DQ-23 (Critical) fails if any column outside the whitelist reaches the indicator fact.",
       "The model does not change; wrong measures are never loaded."),
    _t("T-04",
       "The API delivers text. The measures must be numeric to be aggregated and compared.",
       "ano, desercion_media, repitencia_secundaria",
       "Before: text ('3.71'). After: integer year and decimal rates.",
       "A value that cannot be converted becomes null and DQ-18 (Critical) stops the branch; nothing is imputed.",
       "Enables the comparisons of R6 and R7."),
    _t("T-05",
       "ICFES is semestral and MEN annual. The period (taken from the file name) is linked to its year so both "
       "facts share the same time key.",
       "periodo -> id_periodo, id_anio",
       "'2022-1' and '2022-2' -> id_anio of 2022 (dim_periodo.id_anio).",
       "A period outside the project list cannot be built: DQ-03 and DQ-27 (Critical) decide.",
       "It is the conformed time key that joins FACT_RESULTADO_SABER11 with FACT_INDICADOR_DEPARTAMENTO."),
    _t("T-06",
       "The ICFES stores the department only as free text. A common key (DANE code) is needed to integrate "
       "with the MEN.",
       "cole_depto_ubicacion -> id_departamento (in DIM_COLEGIO)",
       "Before: text with variants (accents, 'D.C.', 'San Andres'). After: id_departamento of the DANE department, "
       "after normalizing case, accents and punctuation and applying the alias table in config.py.",
       "A school whose text has no equivalent keeps id_departamento null and DQ-26 (Critical) stops the load. "
       "It is NOT sent to -1.",
       "Makes R6 and R7 possible by department. It is the most sensitive integration of the project."),
    _t("T-07",
       "R2 and R3 need closed domains. The area has two spellings of the same value.",
       "cole_area_ubicacion (cole_naturaleza and cole_jornada are validated, not changed)",
       "'URBANA' -> 'URBANO'; RURAL unchanged.",
       "An unforeseen label is stopped earlier by DQ-09 (Critical, raw gate); it is never converted silently to null.",
       "Affects DIM_COLEGIO and R1-R3."),
    _t("T-08",
       "Scores equal to 0 may be real. The guide forbids altering data only to pass a validation, so they are "
       "flagged, not removed.",
       "punt_global, puntaje_sospechoso",
       "Before: zeros without mark. After: puntaje_sospechoso = true; the row stays in the fact.",
       "A zero without mark would trigger DQ-07 (Warning).",
       "The grain does not change. Averages of R6 and R7 exclude flagged rows."),
    _t("T-09",
       "The key of an exam attempt is (estu_consecutivo, periodo) and must be unique.",
       "estu_consecutivo, id_periodo",
       "Before: exact copies of the same row (2 in 2025-2 in the First Delivery). After: one row. "
       "Verified as exact copy across all required columns, not only by key.",
       "Copies with different values, or a null key, go to quarantine (data/prepared/<batch>/quarantine) and "
       "DQ-01 (Critical) stops the load until someone decides.",
       "Guarantees the grain of FACT_RESULTADO_SABER11."),
    _t("T-10",
       "Missing gender is not imputed. It only affects R5.",
       "estu_genero -> id_genero",
       "F -> 1, M -> 2, null or unknown -> -1 ('No informado').",
       "More than 0.01% of -1 raises DQ-08 (Informational).",
       "R5 loses those records; R1-R4, R6 and R7 are unchanged."),
    _t("T-11",
       "The First Delivery sent ANY key not found to -1 (fillna(-1)), mixing real validantes with real schools "
       "that were lost. Now -1 means only 'no school at the origin'.",
       "cole_codigo_icfes -> id_colegio",
       "Before: any missing key -> -1. After: -1 only if cole_codigo_icfes is null; a real school always finds its row.",
       "A real school without id_colegio stays null and DQ-13 / DQ-11 (Critical) stop the load.",
       "Prevents real schools from disappearing from R1-R4, R6 and R7 without notice."),
    _t("T-12",
       "MEN is at department x year grain and ICFES at student grain. ICFES must be brought to the MEN grain "
       "before joining, otherwise the join multiplies rows.",
       "punt_global, puntaje_sospechoso, id_colegio -> id_departamento, id_anio",
       "Before: about 3.29M rows. After: at most 33 x years rows with the mean of punt_global of the valid "
       "students (real school, not suspicious, score present) and the student count.",
       "A department-year without valid students has a null average, never 0; DQ-28 and DQ-30 (Critical) control it.",
       "Bridge between both sources (data/prepared/<batch>/integration_depto_anio.parquet)."),
    _t("T-13",
       "A school can change attributes between periods and the dimension keeps one value per school. "
       "Default: first non-null value seen (First Delivery behaviour); COLEGIO_ATTR_POLICY=latest switches to the latest.",
       "cole_naturaleza, cole_jornada, cole_area_ubicacion, cole_depto_ubicacion, cole_mcpio_ubicacion, cole_nombre_establecimiento",
       "Before: possibly several values per school. After: one value; audit_conflicto_atributos marks schools whose "
       "nature, area or department changed.",
       "More than 1% of schools in conflict raises DQ-12 (Warning) and the report lists the percentage.",
       "Decides whether R1-R4, R6 and R7 use the first or the latest school profile. Re-evaluate with the first real run."),
    _t("T-14",
       "Scores arrive as text. The First Delivery converted with errors='coerce', which hides invalid values.",
       "punt_lectura_critica, punt_matematicas, punt_sociales_ciudadanas, punt_c_naturales, punt_ingles, punt_global",
       "Before: text. After: decimal. audit_conversion_ok records that no score was lost in the conversion.",
       "A score that was present but could not be converted: DQ-06 (Critical) stops the load.",
       "Enables averages and the range check of DQ-05."),
]


def normalize_text(text) -> str | None:
    """Upper case, no accents, punctuation replaced by single spaces."""
    if text is None:
        return None
    decomposed = unicodedata.normalize("NFKD", str(text).upper())
    no_accents = "".join(ch for ch in decomposed if not unicodedata.combining(ch))
    cleaned = re.sub(r"[^A-Z0-9]+", " ", no_accents).strip()
    return cleaned or None


def _department_lookup() -> dict:
    lookup = {normalize_text(name): code for code, name in config.DANE_DEPARTAMENTOS.items()}
    lookup.update(config.DEPARTAMENTO_ALIASES)
    return lookup


_LOOKUP = _department_lookup()


def department_code_from_text(text) -> str | None:
    """'Bogotá, D.C.' / 'BOGOTA' / 'San Andrés' -> DANE code, or None when there is no equivalence."""
    return _LOOKUP.get(normalize_text(text))


def export_transformations_table(path: Path) -> None:
    lines = [
        "# Transformation catalog",
        "",
        "Generated from `src/transform/rules.py` (do not edit by hand).",
        "",
        "| Transformation ID | Rule or Rationale | Affected Fields | Before and After Behavior | Exception Handling | Analytical or Dimensional Impact |",
        "|---|---|---|---|---|---|",
    ]
    for t in TRANSFORMATIONS:
        lines.append(f"| {t['id']} | {t['rationale']} | {t['fields']} | {t['before_after']} | "
                     f"{t['exception']} | {t['impact']} |")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    export_transformations_table(config.PROJECT_ROOT / "docs" / "transformations.md")
    print("transformations.md written")
