"""Dimensions of the Second Delivery star schema (see docs/model/star_schema.mmd).

  dim_departamento  NEW  33 DANE departments + member -1 'No aplica' (shared by both facts)
  dim_anio          NEW  calendar years of the project periods (shared by both facts)
  dim_periodo       +id_anio (links each semester to its year)
  dim_colegio       +id_departamento (links each school to its DANE department)
  dim_genero        unchanged

Surrogate keys are assigned from sorted natural keys, so the same input
always produces the same ids inside one batch.
"""
import logging
from pathlib import Path

import polars as pl

from src import config
from src.transform.rules import department_code_from_text

log = logging.getLogger(__name__)

SCHOOL_ATTRS = [c for c in config.COLS_COLEGIO if c != "cole_codigo_icfes"]


def build_dim_departamento() -> pl.DataFrame:
    rows = [{"id_departamento": i, "cod_dane_depto": code, "nombre_departamento": name}
            for i, (code, name) in enumerate(sorted(config.DANE_DEPARTAMENTOS.items()), start=1)]
    rows.insert(0, {"id_departamento": -1, "cod_dane_depto": "00", "nombre_departamento": config.NO_APLICA})
    return pl.DataFrame(rows, schema={"id_departamento": pl.Int32, "cod_dane_depto": pl.String,
                                      "nombre_departamento": pl.String})


def build_dim_anio() -> pl.DataFrame:
    return pl.DataFrame({"id_anio": list(range(1, len(config.PERIODO_YEARS) + 1)),
                         "anio": config.PERIODO_YEARS},
                        schema={"id_anio": pl.Int32, "anio": pl.Int32})


def build_dim_genero() -> pl.DataFrame:
    return pl.DataFrame({
        "id_genero": [-1, 1, 2],
        "estu_genero": ["No informado", "F", "M"],
        "genero_descripcion": ["No informado", "Femenino", "Masculino"],
    }, schema={"id_genero": pl.Int32, "estu_genero": pl.String, "genero_descripcion": pl.String})


def build_dim_periodo(dim_anio: pl.DataFrame) -> pl.DataFrame:
    year_to_id = dict(zip(dim_anio["anio"].to_list(), dim_anio["id_anio"].to_list()))
    rows = []
    for i, periodo in enumerate(config.PERIODOS, start=1):
        anio, semestre = periodo.split("-")
        rows.append({"id_periodo": i, "periodo": periodo, "anio": int(anio),
                     "semestre": int(semestre), "id_anio": year_to_id[int(anio)]})
    return pl.DataFrame(rows, schema={"id_periodo": pl.Int32, "periodo": pl.String, "anio": pl.Int32,
                                      "semestre": pl.Int32, "id_anio": pl.Int32})


def build_dim_colegio(raw_dir: Path, dim_departamento: pl.DataFrame) -> pl.DataFrame:
    """One row per school code (T-06, T-07, T-13), plus the 'No aplica' member (-1)."""
    lazy = pl.scan_parquet(raw_dir / "icfes_*.parquet")

    # T-06: the text -> DANE code table is built from the (few) distinct texts, not row by row
    texts = lazy.select("cole_depto_ubicacion").unique().collect()["cole_depto_ubicacion"].drop_nulls().to_list()
    text_to_code = {t: department_code_from_text(t) for t in texts}
    text_to_code = {t: c for t, c in text_to_code.items() if c}
    code_to_id = dict(zip(dim_departamento["cod_dane_depto"].to_list(), dim_departamento["id_departamento"].to_list()))
    text_to_id = {t: code_to_id[c] for t, c in text_to_code.items()}
    unmapped = sorted(set(texts) - set(text_to_id))
    if unmapped:
        log.warning("[DIM_COLEGIO] department texts without DANE equivalence: %s", unmapped)

    pick = (lambda c: pl.col(c).drop_nulls().last()) if config.COLEGIO_ATTR_POLICY == "latest" \
        else (lambda c: pl.col(c).drop_nulls().first())  # T-13

    dept_id_row = (pl.col("cole_depto_ubicacion").replace_strict(text_to_id, default=None)
                   if text_to_id else pl.lit(None, dtype=pl.Int32))
    schools = (
        lazy.filter(pl.col("cole_codigo_icfes").is_not_null())
        .select([*config.COLS_COLEGIO, "periodo_archivo"])
        .with_columns(
            pl.col("cole_area_ubicacion").replace_strict(config.AREA_HARMONIZATION, default=None),  # T-07
            dept_id_row.alias("_dept_id_row"),
        )
        .sort("periodo_archivo")
        .group_by("cole_codigo_icfes", maintain_order=True)
        .agg(
            *[pick(c).alias(c) for c in SCHOOL_ATTRS],
            (pl.col("cole_naturaleza").drop_nulls().n_unique() > 1).alias("_c_nat"),
            (pl.col("cole_area_ubicacion").drop_nulls().n_unique() > 1).alias("_c_area"),
            (pl.col("_dept_id_row").drop_nulls().n_unique() > 1).alias("_c_dept"),
        )
        .collect(engine="streaming")
    )
    # department of the chosen text (policy), consistent with the other chosen attributes
    schools = schools.with_columns(
        pl.col("cole_depto_ubicacion").replace_strict(text_to_id, default=None).alias("id_departamento")
        if text_to_id else pl.lit(None, dtype=pl.Int32).alias("id_departamento"),
        (pl.col("_c_nat") | pl.col("_c_area") | pl.col("_c_dept")).alias("audit_conflicto_atributos"),
    ).drop("_c_nat", "_c_area", "_c_dept")

    schools = (schools.sort("cole_codigo_icfes")
               .with_row_index("id_colegio", offset=1)
               .with_columns(pl.col("id_colegio").cast(pl.Int32), pl.col("id_departamento").cast(pl.Int32)))

    member = pl.DataFrame([{
        "id_colegio": -1, "cole_codigo_icfes": "NO_APLICA",
        "cole_nombre_establecimiento": "No aplica (sin colegio -- validante)",
        "cole_naturaleza": config.NO_APLICA, "cole_jornada": config.NO_APLICA,
        "cole_area_ubicacion": config.NO_APLICA, "cole_depto_ubicacion": config.NO_APLICA,
        "cole_mcpio_ubicacion": config.NO_APLICA, "id_departamento": -1, "audit_conflicto_atributos": False,
    }], schema={**{c: pl.String for c in config.COLS_COLEGIO}, "id_colegio": pl.Int32,
                "id_departamento": pl.Int32, "audit_conflicto_atributos": pl.Boolean})

    columns = ["id_colegio", *config.COLS_COLEGIO, "id_departamento", "audit_conflicto_atributos"]
    dim = pl.concat([member.select(columns), schools.select(columns)])
    log.info("[DIM_COLEGIO] %s schools + 1 'No aplica' member; %s with attribute conflicts",
             schools.height, int(schools["audit_conflicto_atributos"].sum()))
    return dim


def build_and_write_dimensions(raw_dir: Path, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    dim_departamento = build_dim_departamento()
    dim_anio = build_dim_anio()
    dims = {
        "dim_departamento": dim_departamento,
        "dim_anio": dim_anio,
        "dim_genero": build_dim_genero(),
        "dim_periodo": build_dim_periodo(dim_anio),
        "dim_colegio": build_dim_colegio(raw_dir, dim_departamento),
    }
    for name, frame in dims.items():
        frame.write_parquet(out_dir / f"{name}.parquet")
    return dims
