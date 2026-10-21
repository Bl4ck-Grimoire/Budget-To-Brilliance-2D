"""Central configuration for the Second Delivery pipeline.

Everything that is a decision (periods, thresholds, domains, API location,
DANE codes) lives here, so the rest of the code has no magic values.
"""
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------- paths
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.getenv("DATA_DIR", PROJECT_ROOT / "data"))
SQL_DIR = PROJECT_ROOT / "sql"
GX_ROOT = PROJECT_ROOT  # Great Expectations file context -> ./gx

# Folder with the original ICFES .txt files (never modified by the pipeline)
ICFES_SOURCE_DIR = Path(os.getenv("ICFES_SOURCE_DIR", DATA_DIR / "source" / "icfes"))


def new_batch_id() -> str:
    """A batch identifies one full run (raw copies, evidence, prepared data)."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def raw_primary_dir(batch_id: str) -> Path:
    return DATA_DIR / "raw" / "primary" / batch_id


def raw_api_dir(batch_id: str) -> Path:
    return DATA_DIR / "raw" / "api" / batch_id


def prepared_dir(batch_id: str) -> Path:
    return DATA_DIR / "prepared" / batch_id


def run_dir(batch_id: str) -> Path:
    """Evidence of a run: validation results, transformation and reconciliation reports."""
    return DATA_DIR / "runs" / batch_id


# ------------------------------------------------------- primary source
PERIODOS = [
    "2021-2", "2022-1", "2022-2", "2023-1", "2023-2",
    "2024-1", "2024-2", "2025-1", "2025-2",
]
PERIODO_YEARS = sorted({int(p.split("-")[0]) for p in PERIODOS})  # 2021..2025


def periodo_to_filename(periodo: str) -> str:
    """'2021-2' -> 'Examen_Saber_11_20212.txt' (real DataIcfes file pattern)."""
    return f"Examen_Saber_11_{periodo.replace('-', '')}.txt"


COLS_COLEGIO = [
    "cole_codigo_icfes", "cole_nombre_establecimiento", "cole_naturaleza",
    "cole_jornada", "cole_area_ubicacion", "cole_depto_ubicacion", "cole_mcpio_ubicacion",
]
PUNT_COLS = [
    "punt_lectura_critica", "punt_matematicas", "punt_sociales_ciudadanas",
    "punt_c_naturales", "punt_ingles", "punt_global",
]
# Columns the pipeline needs from every ICFES file (raw projection, DQ-04)
ICFES_REQUIRED_COLUMNS = COLS_COLEGIO + ["estu_consecutivo", "estu_genero"] + PUNT_COLS

# Closed domains (DQ-09, DQ-10). The raw area domain still contains URBANA (T-07).
DOMAIN_NATURALEZA = ["OFICIAL", "NO OFICIAL"]
DOMAIN_JORNADA = ["COMPLETA", "UNICA", "MAÑANA", "TARDE", "NOCHE", "SABATINA"]
DOMAIN_AREA_RAW = ["URBANO", "URBANA", "RURAL"]
DOMAIN_AREA = ["URBANO", "RURAL"]
AREA_HARMONIZATION = {"URBANA": "URBANO", "URBANO": "URBANO", "RURAL": "RURAL"}
NO_APLICA = "No aplica"

# T-13: which value of a school attribute is kept when it changes between periods
#   "first"  -> value of the first period seen (First Delivery behaviour)
#   "latest" -> value of the most recent period
COLEGIO_ATTR_POLICY = os.getenv("COLEGIO_ATTR_POLICY", "first")

# ------------------------------------------------------------ API source
MEN_API_URL = os.getenv("MEN_API_URL", "https://www.datos.gov.co/resource/ji8i-4anb.json")
MEN_API_PAGE_SIZE = int(os.getenv("MEN_API_PAGE_SIZE", "1000"))
MEN_API_TIMEOUT = int(os.getenv("MEN_API_TIMEOUT", "60"))
MEN_RAW_FILE = "men_estadisticas_departamento.parquet"
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", "")  # optional, raises rate limits

# Fields the API branch cannot work without (DQ-15)
MEN_REQUIRED_FIELDS = [
    "ano", "c_digo_departamento", "departamento",
    "desercion_media", "repitencia_secundaria",
    "aprobacion", "reprobacion", "desercion",  # used by the DQ-22 consistency check
]
# Only these measures are loaded into the DW (DQ-23, whitelist)
MEN_MEASURES = ["desercion_media", "repitencia_secundaria"]
# T-01: only rows with ano > 2021 are prepared (R6 and R7 scope)
MEN_MIN_YEAR_EXCLUSIVE = int(os.getenv("MEN_MIN_YEAR_EXCLUSIVE", "2021"))
MEN_LAST_YEAR_EXPECTED = 2024  # DQ-24 raises an Informational notice when the API goes beyond

# ----------------------------------------------------- thresholds (see dq_rules.py for the basis)
MAX_PCT_IDENTICAL_COLUMN = 50      # DQ-22: a column equal to another in >50% of rows is not independent
CANARY_TOLERANCE = 0.5             # DQ-22: |aprobacion + reprobacion + desercion - 100|
RANGE_PUNT_GLOBAL = (0, 500)       # DQ-05
RANGE_PUNT_AREA = (0, 100)         # DQ-05
RANGE_RATE = (0, 100)              # DQ-19 (percentages)
MOSTLY_UNIQUE_RAW = 0.9999         # DQ-01 raw observation (GX flags every copy of a duplicated key)
MOSTLY_ATTR_STABLE = 0.99          # DQ-12
MOSTLY_FLAG_RARE = 0.9999          # DQ-07 / DQ-08
YOY_PERCENTILE = 0.99              # DQ-20: threshold = P99 of |yearly change| before the window

# ------------------------------------------------ DANE departments (DQ-21)
DANE_DEPARTAMENTOS = {
    "05": "Antioquia", "08": "Atlántico", "11": "Bogotá, D.C.", "13": "Bolívar",
    "15": "Boyacá", "17": "Caldas", "18": "Caquetá", "19": "Cauca", "20": "Cesar",
    "23": "Córdoba", "25": "Cundinamarca", "27": "Chocó", "41": "Huila",
    "44": "La Guajira", "47": "Magdalena", "50": "Meta", "52": "Nariño",
    "54": "Norte de Santander", "63": "Quindío", "66": "Risaralda", "68": "Santander",
    "70": "Sucre", "73": "Tolima", "76": "Valle del Cauca", "81": "Arauca",
    "85": "Casanare", "86": "Putumayo",
    "88": "Archipiélago de San Andrés, Providencia y Santa Catalina",
    "91": "Amazonas", "94": "Guainía", "95": "Guaviare", "97": "Vaupés", "99": "Vichada",
}
# Extra spellings seen in public sources (already normalized: upper case, no accents/punctuation)
DEPARTAMENTO_ALIASES = {
    "BOGOTA": "11", "BOGOTA D C": "11", "BOGOTA DC": "11", "BOGOTA DISTRITO CAPITAL": "11",
    "SAN ANDRES": "88", "SAN ANDRES Y PROVIDENCIA": "88",
    "SAN ANDRES PROVIDENCIA Y SANTA CATALINA": "88",
    "VALLE": "76", "NORTE SANTANDER": "54",
}

# --------------------------------------------------- Data Warehouse
DATABASE_URL = os.getenv("DATABASE_URL", "mysql+pymysql://root:@localhost:3306/icfes_dw")


def db_config() -> dict:
    url = urlparse(DATABASE_URL)
    dbname = os.getenv("MYSQL_DATABASE") or url.path.lstrip("/")
    return {
        "host": url.hostname or "localhost",
        "port": url.port or 3306,
        "user": unquote(url.username or "root"),
        "password": unquote(url.password or ""),
        "dbname": dbname,
    }
