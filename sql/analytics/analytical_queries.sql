-- ============================================================================
-- Analytical queries R1-R7 -- all of them run against the Data Warehouse
-- (never against the source file nor an intermediate DataFrame, as
-- required by section 15).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- R1: Official vs. non-official gap, by period
-- (excludes the "No aplica" member -- students with no school -- because
--  R1 specifically compares types of school)
-- ----------------------------------------------------------------------------
SELECT
    p.periodo,
    c.cole_naturaleza,
    ROUND(AVG(f.punt_global), 1) AS puntaje_global_promedio,
    COUNT(*) AS n_estudiantes
FROM fact_resultado_saber11 f
JOIN dim_colegio c ON f.id_colegio = c.id_colegio
JOIN dim_periodo p ON f.id_periodo = p.id_periodo
WHERE c.cole_naturaleza <> 'No aplica'
GROUP BY p.periodo, c.cole_naturaleza
ORDER BY p.periodo, c.cole_naturaleza;


-- ----------------------------------------------------------------------------
-- R2: Average score by school shift (jornada), controlling for naturaleza
-- ----------------------------------------------------------------------------
SELECT
    c.cole_naturaleza,
    c.cole_jornada,
    ROUND(AVG(f.punt_global), 1) AS puntaje_global_promedio,
    COUNT(*) AS n_estudiantes
FROM fact_resultado_saber11 f
JOIN dim_colegio c ON f.id_colegio = c.id_colegio
WHERE c.cole_jornada <> 'No aplica'
GROUP BY c.cole_naturaleza, c.cole_jornada
ORDER BY c.cole_naturaleza, puntaje_global_promedio DESC;


-- ----------------------------------------------------------------------------
-- R3: Urban-rural gap by department (top 10 largest gap)
-- ----------------------------------------------------------------------------
SELECT
    depto,
    urbano_prom,
    rural_prom,
    ROUND(urbano_prom - rural_prom, 1) AS brecha
FROM (
    SELECT
        c.cole_depto_ubicacion AS depto,
        AVG(CASE WHEN c.cole_area_ubicacion = 'URBANO' THEN f.punt_global END) AS urbano_prom,
        AVG(CASE WHEN c.cole_area_ubicacion = 'RURAL'  THEN f.punt_global END) AS rural_prom,
        COUNT(*) AS n
    FROM fact_resultado_saber11 f
    JOIN dim_colegio c ON f.id_colegio = c.id_colegio
    WHERE c.cole_area_ubicacion <> 'No aplica'
    GROUP BY c.cole_depto_ubicacion
    HAVING n >= 200
) t
WHERE urbano_prom IS NOT NULL AND rural_prom IS NOT NULL
ORDER BY brecha DESC
LIMIT 10;


-- ----------------------------------------------------------------------------
-- R4: Departmental ranking of the average global score, by period
-- (top/bottom 5 of the most recent period, as an example of the general
--  pattern)
-- ----------------------------------------------------------------------------
SELECT
    p.periodo,
    c.cole_depto_ubicacion AS depto,
    ROUND(AVG(f.punt_global), 1) AS puntaje_global_promedio,
    COUNT(*) AS n_estudiantes
FROM fact_resultado_saber11 f
JOIN dim_colegio c ON f.id_colegio = c.id_colegio
JOIN dim_periodo p ON f.id_periodo = p.id_periodo
WHERE c.cole_depto_ubicacion <> 'No aplica' AND p.periodo = '2025-2'
GROUP BY p.periodo, c.cole_depto_ubicacion
HAVING n_estudiantes >= 200
ORDER BY puntaje_global_promedio DESC;
-- (for the full 2021-2 to 2025-2 evolution, drop the period filter and add
--  ORDER BY p.periodo, puntaje_global_promedio DESC)


-- ----------------------------------------------------------------------------
-- R5: Gender gap in the global score and by subject area, by period
-- ----------------------------------------------------------------------------
SELECT
    p.periodo,
    g.genero_descripcion,
    ROUND(AVG(f.punt_global), 1)              AS punt_global_prom,
    ROUND(AVG(f.punt_matematicas), 1)         AS punt_matematicas_prom,
    ROUND(AVG(f.punt_lectura_critica), 1)     AS punt_lectura_critica_prom,
    COUNT(*) AS n_estudiantes
FROM fact_resultado_saber11 f
JOIN dim_genero g ON f.id_genero = g.id_genero
JOIN dim_periodo p ON f.id_periodo = p.id_periodo
WHERE g.estu_genero IN ('F', 'M')
GROUP BY p.periodo, g.genero_descripcion
ORDER BY p.periodo, g.genero_descripcion;


-- ----------------------------------------------------------------------------
-- R6: Dropout in media (desercion_media) vs mean Saber 11 global score, by department and year
-- Grain of the result: department x year (2022-2024). The ICFES fact is AGGREGATED to
-- department x year first and only then joined with FACT_INDICADOR_DEPARTAMENTO, so no MEN
-- value is repeated per student. Averages use valid students only: real school (id_colegio <> -1),
-- score present and not flagged as suspicious. Association only, not causality.
-- ----------------------------------------------------------------------------
WITH puntaje AS (
    SELECT c.id_departamento, p.id_anio,
           AVG(f.punt_global) AS punt_global_prom,
           COUNT(*)           AS n_estudiantes
    FROM fact_resultado_saber11 f
    JOIN dim_colegio c ON c.id_colegio = f.id_colegio
    JOIN dim_periodo p ON p.id_periodo = f.id_periodo
    WHERE f.id_colegio <> -1
      AND f.puntaje_sospechoso = FALSE
      AND f.punt_global IS NOT NULL
    GROUP BY c.id_departamento, p.id_anio
)
SELECT a.anio,
       d.cod_dane_depto,
       d.nombre_departamento,
       ROUND(pu.punt_global_prom, 1) AS puntaje_global_promedio,
       i.desercion_media                   AS desercion_media,
       pu.n_estudiantes,
       RANK() OVER (PARTITION BY a.anio ORDER BY i.desercion_media DESC)        AS ranking_desercion_mayor_a_menor,
       RANK() OVER (PARTITION BY a.anio ORDER BY pu.punt_global_prom ASC) AS ranking_puntaje_menor_a_mayor
FROM puntaje pu
JOIN fact_indicador_departamento i ON i.id_departamento = pu.id_departamento AND i.id_anio = pu.id_anio
JOIN dim_departamento d ON d.id_departamento = pu.id_departamento
JOIN dim_anio a         ON a.id_anio = pu.id_anio
ORDER BY a.anio, i.desercion_media DESC;


-- ----------------------------------------------------------------------------
-- R6 (summary): Spearman rank correlation per year between desercion_media and the mean score
-- (approximate: RANK() without tie correction; the rates have 2 decimals, so ties are rare)
-- ----------------------------------------------------------------------------
WITH puntaje AS (
    SELECT c.id_departamento, p.id_anio, AVG(f.punt_global) AS punt_global_prom
    FROM fact_resultado_saber11 f
    JOIN dim_colegio c ON c.id_colegio = f.id_colegio
    JOIN dim_periodo p ON p.id_periodo = f.id_periodo
    WHERE f.id_colegio <> -1 AND f.puntaje_sospechoso = FALSE AND f.punt_global IS NOT NULL
    GROUP BY c.id_departamento, p.id_anio
),
ranked AS (
    SELECT pu.id_anio,
           RANK() OVER (PARTITION BY pu.id_anio ORDER BY pu.punt_global_prom) AS r_puntaje,
           RANK() OVER (PARTITION BY pu.id_anio ORDER BY i.desercion_media)         AS r_medida,
           COUNT(*)  OVER (PARTITION BY pu.id_anio)                           AS n
    FROM puntaje pu
    JOIN fact_indicador_departamento i ON i.id_departamento = pu.id_departamento AND i.id_anio = pu.id_anio
)
SELECT a.anio,
       MAX(r.n) AS n_departamentos,
       ROUND(1 - 6 * SUM(POW(CAST(r.r_puntaje AS SIGNED) - CAST(r.r_medida AS SIGNED), 2)) / (MAX(r.n) * (POW(MAX(r.n), 2) - 1)), 3) AS spearman_desercion_vs_puntaje
FROM ranked r
JOIN dim_anio a ON a.id_anio = r.id_anio
GROUP BY a.anio
ORDER BY a.anio;


-- ----------------------------------------------------------------------------
-- R7: Repetition in secundaria (repitencia_secundaria) vs mean Saber 11 global score, by department and year
-- Grain of the result: department x year (2022-2024). The ICFES fact is AGGREGATED to
-- department x year first and only then joined with FACT_INDICADOR_DEPARTAMENTO, so no MEN
-- value is repeated per student. Averages use valid students only: real school (id_colegio <> -1),
-- score present and not flagged as suspicious. Association only, not causality.
-- ----------------------------------------------------------------------------
WITH puntaje AS (
    SELECT c.id_departamento, p.id_anio,
           AVG(f.punt_global) AS punt_global_prom,
           COUNT(*)           AS n_estudiantes
    FROM fact_resultado_saber11 f
    JOIN dim_colegio c ON c.id_colegio = f.id_colegio
    JOIN dim_periodo p ON p.id_periodo = f.id_periodo
    WHERE f.id_colegio <> -1
      AND f.puntaje_sospechoso = FALSE
      AND f.punt_global IS NOT NULL
    GROUP BY c.id_departamento, p.id_anio
)
SELECT a.anio,
       d.cod_dane_depto,
       d.nombre_departamento,
       ROUND(pu.punt_global_prom, 1) AS puntaje_global_promedio,
       i.repitencia_secundaria                   AS repitencia_secundaria,
       pu.n_estudiantes,
       RANK() OVER (PARTITION BY a.anio ORDER BY i.repitencia_secundaria DESC)        AS ranking_repitencia_mayor_a_menor,
       RANK() OVER (PARTITION BY a.anio ORDER BY pu.punt_global_prom ASC) AS ranking_puntaje_menor_a_mayor
FROM puntaje pu
JOIN fact_indicador_departamento i ON i.id_departamento = pu.id_departamento AND i.id_anio = pu.id_anio
JOIN dim_departamento d ON d.id_departamento = pu.id_departamento
JOIN dim_anio a         ON a.id_anio = pu.id_anio
ORDER BY a.anio, i.repitencia_secundaria DESC;


-- ----------------------------------------------------------------------------
-- R7 (summary): Spearman rank correlation per year between repitencia_secundaria and the mean score
-- (approximate: RANK() without tie correction; the rates have 2 decimals, so ties are rare)
-- ----------------------------------------------------------------------------
WITH puntaje AS (
    SELECT c.id_departamento, p.id_anio, AVG(f.punt_global) AS punt_global_prom
    FROM fact_resultado_saber11 f
    JOIN dim_colegio c ON c.id_colegio = f.id_colegio
    JOIN dim_periodo p ON p.id_periodo = f.id_periodo
    WHERE f.id_colegio <> -1 AND f.puntaje_sospechoso = FALSE AND f.punt_global IS NOT NULL
    GROUP BY c.id_departamento, p.id_anio
),
ranked AS (
    SELECT pu.id_anio,
           RANK() OVER (PARTITION BY pu.id_anio ORDER BY pu.punt_global_prom) AS r_puntaje,
           RANK() OVER (PARTITION BY pu.id_anio ORDER BY i.repitencia_secundaria)         AS r_medida,
           COUNT(*)  OVER (PARTITION BY pu.id_anio)                           AS n
    FROM puntaje pu
    JOIN fact_indicador_departamento i ON i.id_departamento = pu.id_departamento AND i.id_anio = pu.id_anio
)
SELECT a.anio,
       MAX(r.n) AS n_departamentos,
       ROUND(1 - 6 * SUM(POW(CAST(r.r_puntaje AS SIGNED) - CAST(r.r_medida AS SIGNED), 2)) / (MAX(r.n) * (POW(MAX(r.n), 2) - 1)), 3) AS spearman_repitencia_vs_puntaje
FROM ranked r
JOIN dim_anio a ON a.id_anio = r.id_anio
GROUP BY a.anio
ORDER BY a.anio;
