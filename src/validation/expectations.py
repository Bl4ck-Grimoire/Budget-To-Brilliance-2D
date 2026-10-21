"""Declarative definition of every Great Expectations checkpoint.

Each entry of `checkpoint_specs()` describes one data object (a DataFrame
that will be validated) and the expectations that apply to it. Every
expectation carries `dq_rule_id` and `severity` in its metadata, so a result
can always be traced back to the rule catalog (dq_rules.py).

Naming used in the data objects:
  * columns starting with `audit_` exist only to be validated; they are
    dropped before loading (they are not part of the DW).
  * `$PARAMETER` values are supplied at run time (ids of the dimensions, row
    counts, historical thresholds) by validate_prepared.py.
"""
import great_expectations.expectations as gxe

from src import config
from src.validation.dq_rules import RULES

NUMERIC_TEXT = r"^\d+(?:\.\d+)?$"
GATE_PRIMARY_RAW, GATE_API_RAW, GATE_PREPARED = "primary_raw", "api_raw", "prepared"

# Columns of the indicator fact that may be loaded (DQ-23) + audit columns used for validation
FACT_INDICADOR_COLUMNS = ["id_departamento", "id_anio", *config.MEN_MEASURES]
FACT_INDICADOR_AUDIT = [f"audit_cambio_abs_{m}" for m in config.MEN_MEASURES]


def param(name: str) -> dict:
    return {"$PARAMETER": name}


def _e(expectation_class, rule: str, severity: str | None = None, **kwargs):
    meta = {"dq_rule_id": rule, "severity": severity or RULES[rule]["severity"]}
    return expectation_class(meta=meta, **kwargs)


def _where(condition: str) -> dict:
    return {"row_condition": condition, "condition_parser": "pandas"}


def checkpoint_specs() -> dict:
    """Returns {checkpoint_name: {"gate": ..., "expectations": [...]}} (fresh objects on each call)."""
    areas_lo, areas_hi = config.RANGE_PUNT_AREA
    rate_lo, rate_hi = config.RANGE_RATE
    g_lo, g_hi = config.RANGE_PUNT_GLOBAL
    dane_codes = sorted(config.DANE_DEPARTAMENTOS)
    # codes as they may arrive from the API: with and without the leading zero
    raw_codes = sorted({*dane_codes, *(str(int(c)) for c in dane_codes)})
    area_scores = [c for c in config.PUNT_COLS if c != "punt_global"]

    specs = {}

    # ================================================================= PRIMARY RAW
    specs["icfes_raw_manifest"] = {"gate": GATE_PRIMARY_RAW, "expectations": [
        _e(gxe.ExpectColumnDistinctValuesToEqualSet, "DQ-03", column="periodo", value_set=config.PERIODOS),
        _e(gxe.ExpectColumnValuesToBeBetween, "DQ-03", column="rows_read", min_value=1),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-04", column="missing_required_columns", value_set=[0]),
    ]}

    specs["icfes_raw_data"] = {"gate": GATE_PRIMARY_RAW, "expectations": [
        _e(gxe.ExpectColumnValuesToNotBeNull, "DQ-01", column="estu_consecutivo"),
        _e(gxe.ExpectColumnValuesToBeUnique, "DQ-01", severity="Warning",
           column="estu_consecutivo", mostly=config.MOSTLY_UNIQUE_RAW),
        *[_e(gxe.ExpectColumnValuesToMatchRegex, "DQ-06", column=c, regex=NUMERIC_TEXT) for c in config.PUNT_COLS],
        _e(gxe.ExpectColumnValuesToNotBeNull, "DQ-08", column="estu_genero", mostly=config.MOSTLY_FLAG_RARE),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-08", column="estu_genero", value_set=["F", "M"],
           mostly=config.MOSTLY_FLAG_RARE),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-09", column="cole_area_ubicacion", value_set=config.DOMAIN_AREA_RAW),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-10", column="cole_naturaleza", value_set=config.DOMAIN_NATURALEZA),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-10", column="cole_jornada", value_set=config.DOMAIN_JORNADA),
    ]}

    # ================================================================= API RAW
    specs["men_raw_manifest"] = {"gate": GATE_API_RAW, "expectations": [
        _e(gxe.ExpectColumnPairValuesToBeEqual, "DQ-15", column_A="rows_downloaded", column_B="service_count"),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-15", column="missing_required_fields", value_set=[0]),
        _e(gxe.ExpectColumnValuesToBeBetween, "DQ-24", column="max_ano",
           min_value=config.MEN_LAST_YEAR_EXPECTED, max_value=config.MEN_LAST_YEAR_EXPECTED),
    ]}

    specs["men_raw_data"] = {"gate": GATE_API_RAW, "expectations": [
        _e(gxe.ExpectCompoundColumnsToBeUnique, "DQ-16", column_list=["c_digo_departamento", "ano"]),
        *[_e(gxe.ExpectColumnValuesToMatchRegex, "DQ-19", column=c, regex=NUMERIC_TEXT) for c in config.MEN_MEASURES],
        _e(gxe.ExpectColumnValuesToMatchRegex, "DQ-19", column="ano", regex=r"^\d{4}$"),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-21", column="c_digo_departamento", value_set=raw_codes),
        _e(gxe.ExpectColumnValuesToBeBetween, "DQ-22", column="suma_aprob_reprob_deser",
           min_value=100 - config.CANARY_TOLERANCE, max_value=100 + config.CANARY_TOLERANCE),
    ]}

    specs["men_raw_column_profile"] = {"gate": GATE_API_RAW, "expectations": [
        _e(gxe.ExpectColumnValuesToBeBetween, "DQ-22", column="max_pct_identica_otra_columna",
           max_value=config.MAX_PCT_IDENTICAL_COLUMN),
    ]}

    # ================================================================= PREPARED
    specs["prepared_reconciliation"] = {"gate": GATE_PREPARED, "expectations": [
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-02", column="diferencia_conciliacion", value_set=[0]),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-01", severity="Warning", column="filas_en_cuarentena", value_set=[0]),
    ]}

    specs["prepared_dim_colegio"] = {"gate": GATE_PREPARED, "expectations": [
        _e(gxe.ExpectColumnValuesToNotBeNull, "DQ-09", column="cole_area_ubicacion"),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-09", column="cole_area_ubicacion",
           value_set=[*config.DOMAIN_AREA, config.NO_APLICA]),
        _e(gxe.ExpectColumnValuesToNotBeNull, "DQ-10", column="cole_naturaleza"),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-10", column="cole_naturaleza",
           value_set=[*config.DOMAIN_NATURALEZA, config.NO_APLICA]),
        _e(gxe.ExpectColumnValuesToNotBeNull, "DQ-10", column="cole_jornada"),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-10", column="cole_jornada",
           value_set=[*config.DOMAIN_JORNADA, config.NO_APLICA]),
        _e(gxe.ExpectColumnValuesToNotBeNull, "DQ-26", column="id_departamento"),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-26", column="id_departamento", value_set=param("valid_departamento_ids")),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-26", column="id_departamento", value_set=[-1],
           **_where("id_colegio == -1")),
        _e(gxe.ExpectColumnValuesToNotBeInSet, "DQ-26", column="id_departamento", value_set=[-1],
           **_where("id_colegio != -1")),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-12", column="audit_conflicto_atributos", value_set=[False],
           mostly=config.MOSTLY_ATTR_STABLE),
    ]}

    specs["prepared_dim_departamento"] = {"gate": GATE_PREPARED, "expectations": [
        _e(gxe.ExpectColumnValuesToMatchRegex, "DQ-21", column="cod_dane_depto", regex=r"^\d{2}$"),
        _e(gxe.ExpectColumnDistinctValuesToEqualSet, "DQ-21", column="cod_dane_depto", value_set=[*dane_codes, "00"]),
        _e(gxe.ExpectColumnValuesToBeUnique, "DQ-26", column="cod_dane_depto"),
    ]}

    specs["prepared_dim_time"] = {"gate": GATE_PREPARED, "expectations": [
        _e(gxe.ExpectColumnValuesToNotBeNull, "DQ-27", column="id_anio"),
        _e(gxe.ExpectColumnPairValuesToBeEqual, "DQ-27", column_A="anio", column_B="anio_dim_anio"),
        _e(gxe.ExpectColumnDistinctValuesToEqualSet, "DQ-27", column="anio_dim_anio", value_set=config.PERIODO_YEARS),
    ]}

    specs["prepared_fact_resultado"] = {"gate": GATE_PREPARED, "expectations": [
        _e(gxe.ExpectCompoundColumnsToBeUnique, "DQ-01", column_list=["estu_consecutivo", "id_periodo"]),
        *[_e(gxe.ExpectColumnValuesToNotBeNull, "DQ-13", column=c) for c in ("id_colegio", "id_genero", "id_periodo")],
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-13", column="id_colegio", value_set=param("valid_colegio_ids")),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-13", column="id_genero", value_set=param("valid_genero_ids")),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-13", column="id_periodo", value_set=param("valid_periodo_ids")),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-11", column="audit_tiene_colegio_origen", value_set=[False],
           **_where("id_colegio == -1")),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-11", column="audit_tiene_colegio_origen", value_set=[True],
           **_where("id_colegio != -1")),
        _e(gxe.ExpectColumnValuesToBeBetween, "DQ-05", column="punt_global", min_value=g_lo, max_value=g_hi),
        *[_e(gxe.ExpectColumnValuesToBeBetween, "DQ-05", severity="Warning", column=c,
             min_value=areas_lo, max_value=areas_hi) for c in area_scores],
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-06", column="audit_conversion_ok", value_set=[True]),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-07", column="puntaje_sospechoso", value_set=[True],
           **_where("punt_global == 0")),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-07", column="puntaje_sospechoso", value_set=[False],
           mostly=config.MOSTLY_FLAG_RARE),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-08", column="id_genero", value_set=[1, 2],
           mostly=config.MOSTLY_FLAG_RARE),
    ]}

    specs["prepared_fact_indicador"] = {"gate": GATE_PREPARED, "expectations": [
        _e(gxe.ExpectCompoundColumnsToBeUnique, "DQ-25", column_list=["id_departamento", "id_anio"]),
        _e(gxe.ExpectColumnValuesToNotBeNull, "DQ-25", column="id_departamento"),
        _e(gxe.ExpectColumnValuesToNotBeNull, "DQ-25", column="id_anio"),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-25", column="id_departamento", value_set=param("real_departamento_ids")),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-25", column="id_anio", value_set=param("valid_anio_ids")),
        _e(gxe.ExpectTableRowCountToEqual, "DQ-17", value=param("n_expected_rows")),
        _e(gxe.ExpectColumnDistinctValuesToEqualSet, "DQ-17", column="id_departamento",
           value_set=param("real_departamento_ids")),
        *[_e(gxe.ExpectColumnValuesToNotBeNull, "DQ-18", column=m) for m in config.MEN_MEASURES],
        *[_e(gxe.ExpectColumnValuesToBeBetween, "DQ-19", column=m, min_value=rate_lo, max_value=rate_hi)
          for m in config.MEN_MEASURES],
        *[_e(gxe.ExpectColumnValuesToBeBetween, "DQ-20", column=f"audit_cambio_abs_{m}",
             min_value=0, max_value=param(f"thr_{m}")) for m in config.MEN_MEASURES],
        _e(gxe.ExpectTableColumnsToMatchSet, "DQ-23",
           column_set=[*FACT_INDICADOR_COLUMNS, *FACT_INDICADOR_AUDIT], exact_match=True),
    ]}

    specs["prepared_integration"] = {"gate": GATE_PREPARED, "expectations": [
        _e(gxe.ExpectCompoundColumnsToBeUnique, "DQ-30", column_list=["id_departamento", "id_anio"]),
        _e(gxe.ExpectTableRowCountToEqual, "DQ-30", value=param("n_expected_rows")),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-28", column="audit_en_men", value_set=[True],
           **_where("audit_en_icfes == True")),
        _e(gxe.ExpectColumnValuesToBeInSet, "DQ-28", column="audit_en_icfes", value_set=[True]),
    ]}

    return specs
