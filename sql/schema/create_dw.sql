-- ============================================================================
-- Budget To Brilliance - Data Warehouse schema, Second Delivery [MySQL 8]
-- ICFES Saber 11 (ODS 4) + MEN education statistics by department
--
-- Two fact tables with two different grains, joined only through shared
-- (conformed) dimensions:
--
--   FACT_RESULTADO_SABER11        one exam attempt of one student in one period
--                                 PK (estu_consecutivo, id_periodo)
--   FACT_INDICADOR_DEPARTAMENTO   the annual MEN statistics of one department in one year
--                                 PK (id_departamento, id_anio)
--
-- Shared dimensions: DIM_DEPARTAMENTO (through DIM_COLEGIO) and DIM_ANIO (through DIM_PERIODO).
--
-- The script is idempotent: CREATE TABLE IF NOT EXISTS, never DROP.
-- Convention "-1 = not applicable": dim_colegio, dim_genero and dim_departamento
-- include a row with surrogate key -1. The ICFES fact never loses a row because of it.
--
-- Rates in FACT_INDICADOR_DEPARTAMENTO are percentages (0-100): NON-ADDITIVE.
-- Never SUM them across departments; use them per department-year.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- DIM_DEPARTAMENTO (NEW) - the 33 DANE departments + member -1
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_departamento (
    id_departamento      INT PRIMARY KEY,
    cod_dane_depto       CHAR(2)      NOT NULL,      -- '05'; '00' for the -1 member
    nombre_departamento  VARCHAR(80)  NOT NULL,
    UNIQUE KEY uq_cod_dane_depto (cod_dane_depto)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- DIM_ANIO (NEW) - calendar years of the project periods
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_anio (
    id_anio  INT PRIMARY KEY,
    anio     INT NOT NULL,
    UNIQUE KEY uq_anio (anio)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- DIM_GENERO (supports R5)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_genero (
    id_genero           INT PRIMARY KEY,
    estu_genero         VARCHAR(20) NOT NULL,   -- F / M / No informado
    genero_descripcion  VARCHAR(30) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- DIM_PERIODO (semester). id_anio links each semester to its calendar year.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_periodo (
    id_periodo  INT PRIMARY KEY,
    periodo     VARCHAR(10) NOT NULL,   -- '2021-2'
    anio        INT NOT NULL,
    semestre    INT NOT NULL,           -- 1 or 2
    id_anio     INT NOT NULL,
    UNIQUE KEY uq_periodo (periodo),
    CONSTRAINT fk_periodo_anio FOREIGN KEY (id_anio) REFERENCES dim_anio(id_anio)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- DIM_COLEGIO (supports R1-R4, R6, R7). id_colegio = -1: student with no school (validante).
-- id_departamento links each school to its DANE department; cole_depto_ubicacion keeps
-- the original text so the First Delivery queries (R1-R5) keep working unchanged.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_colegio (
    id_colegio                  INT PRIMARY KEY,
    cole_codigo_icfes           VARCHAR(20)  NOT NULL,
    cole_nombre_establecimiento VARCHAR(255),
    cole_naturaleza             VARCHAR(20)  NOT NULL,   -- OFICIAL / NO OFICIAL / No aplica
    cole_jornada                VARCHAR(20)  NOT NULL,   -- COMPLETA/UNICA/MAÑANA/TARDE/NOCHE/SABATINA/No aplica
    cole_area_ubicacion         VARCHAR(20)  NOT NULL,   -- URBANO / RURAL / No aplica
    cole_depto_ubicacion        VARCHAR(80)  NOT NULL,
    cole_mcpio_ubicacion        VARCHAR(80),
    id_departamento             INT          NOT NULL,
    UNIQUE KEY uq_cole_codigo (cole_codigo_icfes),
    INDEX idx_colegio_depto (id_departamento),
    CONSTRAINT fk_colegio_departamento FOREIGN KEY (id_departamento) REFERENCES dim_departamento(id_departamento)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- FACT_RESULTADO_SABER11 - grain: one exam attempt of one student in one period
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_resultado_saber11 (
    estu_consecutivo         VARCHAR(30) NOT NULL,
    id_colegio               INT NOT NULL,
    id_genero                INT NOT NULL,
    id_periodo               INT NOT NULL,
    punt_lectura_critica     DECIMAL(5,1),
    punt_matematicas         DECIMAL(5,1),
    punt_sociales_ciudadanas DECIMAL(5,1),
    punt_c_naturales         DECIMAL(5,1),
    punt_ingles              DECIMAL(5,1),
    punt_global              DECIMAL(5,1),
    puntaje_sospechoso       BOOLEAN NOT NULL DEFAULT FALSE,   -- TRUE if punt_global = 0
    PRIMARY KEY (estu_consecutivo, id_periodo),
    CONSTRAINT fk_fact_colegio FOREIGN KEY (id_colegio) REFERENCES dim_colegio(id_colegio),
    CONSTRAINT fk_fact_genero  FOREIGN KEY (id_genero)  REFERENCES dim_genero(id_genero),
    CONSTRAINT fk_fact_periodo FOREIGN KEY (id_periodo) REFERENCES dim_periodo(id_periodo),
    INDEX idx_fact_colegio (id_colegio),
    INDEX idx_fact_genero  (id_genero),
    INDEX idx_fact_periodo (id_periodo)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------------------------
-- FACT_INDICADOR_DEPARTAMENTO (NEW) - grain: the annual MEN statistics of one
-- department in one calendar year (R6: dropout in media; R7: repetition in secundaria)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_indicador_departamento (
    id_departamento        INT NOT NULL,
    id_anio                INT NOT NULL,
    desercion_media        DECIMAL(5,2) NOT NULL,   -- percentage, non-additive
    repitencia_secundaria  DECIMAL(5,2) NOT NULL,   -- percentage, non-additive
    PRIMARY KEY (id_departamento, id_anio),
    CONSTRAINT fk_indicador_departamento FOREIGN KEY (id_departamento) REFERENCES dim_departamento(id_departamento),
    CONSTRAINT fk_indicador_anio         FOREIGN KEY (id_anio)         REFERENCES dim_anio(id_anio),
    INDEX idx_indicador_anio (id_anio)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
