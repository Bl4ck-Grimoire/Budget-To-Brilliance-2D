# ETL Project. Data Engineering for Sustainable Development in Colombia, second delivery (2D)

## Table of Contents

1. [Problem and Analytical Objective](#1-problem-and-analytical-objective)
2. [First Delivery Baseline and Repository Link](#2-first-delivery-baseline-and-repository-link)
3. [Status and Changes for R1–R5](#3-status-and-changes-for-r1r5)
4. [New Integrated Analytical Requirements R6 and R7](#4-new-integrated-analytical-requirements-r6-and-r7)
5. [Primary Source and Programmatic API Source](#5-primary-source-and-programmatic-api-source)
6. [Analytical Value Contributed by the API](#6-analytical-value-contributed-by-the-api)
7. [System Architecture](#7-system-architecture)
8. [Data Profiling Findings](#8-data-profiling-findings)
9. [Quality Risks and Quality Rules](#9-quality-risks-and-quality-rules)
10. [Great Expectations Validation Design and Rule Traceability](#10-great-expectations-validation-design-and-rule-traceability)
11. [Raw Validation Design for Both Sources (Gate 1a & Gate 1b)](#11-raw-validation-design-for-both-sources-gate-1a--gate-1b)
12. [Transformation Strategy and Integration Contract](#12-transformation-strategy-and-integration-contract)
13. [Refined Dimensional Model and Change Justification](#13-refined-dimensional-model-and-change-justification)
14. [Prepared-Data Validation (Gate 2)](#14-prepared-data-validation-gate-2)
15. [Data Warehouse Implementation and Load](#15-data-warehouse-implementation-and-load)
16. [Airflow DAG Design and Task Interfaces](#16-airflow-dag-design-and-task-interfaces)
17. [Failure and Retry Policies](#17-failure-and-retry-policies)
18. [Mandatory Reliability Test A: Successful Execution Evidence](#18-mandatory-reliability-test-a-successful-execution-evidence)
19. [Mandatory Reliability Test B: Controlled Critical Failure Evidence](#19-mandatory-reliability-test-b-controlled-critical-failure-evidence)
20. [Mandatory Reliability Test C: Repeatability and Safe-Rerun Strategy](#20-mandatory-reliability-test-c-repeatability-and-safe-rerun-strategy)
21. [Dashboard and Analytical Outputs](#21-dashboard-and-analytical-outputs)
22. [Requirement-to-KPI and End-to-End Traceability Matrix](#22-requirement-to-kpi-and-end-to-end-traceability-matrix)
23. [Setup and Execution Instructions](#23-setup-and-execution-instructions)
24. [Assumptions and Limitations](#24-assumptions-and-limitations)
25. [Team Members and Responsibilities](#25-team-members-and-responsibilities)

---

## 1. Problem and Analytical Objective

### 1.1 Context and Problem Statement
Quality education (**SDG 4**) is a cornerstone of socioeconomic mobility and territorial equity in Colombia. However, national public policy evaluations often analyze student learning outcomes in isolation from structural indicators of school trajectory failure. 

In Colombia, standardized academic achievement is captured at the exit of secondary education through the **ICFES Saber 11** state examination. While this dataset provides granular records regarding student performance, school characteristics (sector, shift, location area), and demographics across 9 semesters (2021-2 through 2025-2), it suffers from an inherent **survivorship bias**: it only observes students who persisted in the formal educational system until Grade 11. It provides no visibility into grade retention, dropout dynamics, or educational vulnerability across regional territories.

Conversely, the **Ministerio de Educación Nacional (MEN)** publishes annual departmental statistics on educational access and trajectory efficiency through the Colombian Open Data portal (*Datos Abiertos Colombia*, Socrata API). This dataset documents macro-indicators such as the upper-secondary dropout rate (*deserción media*) and secondary grade repetition rate (*repitencia secundaria*). However, the MEN dataset contains no individual school data, no student evaluations, and no socioeconomic test metrics.

### 1.2 Analytical Objective
The objective of this Second Delivery is to evolve the initial analytical prototype into an **automated, reliable, and production-ready Data Engineering pipeline** that:
1. Programmatically ingests the MEN departmental education statistics via its Socrata REST API.
2. Automates data quality validation across three explicit gates using **Great Expectations 1.24.0** with strict severity policies.
3. Formulates and answers two new integrated analytical requirements (**R6** and **R7**) alongside maintained/refined baseline requirements (**R1–R5**).
4. Re-engineers the dimensional model into a conformed **Fact Constellation** schema loaded into a **MySQL 8** Data Warehouse (`icfes_dw`).
5. Orchestrates the end-to-end workflow using **Apache Airflow 3.1.8** via the TaskFlow API (`@dag`, `@task`), demonstrating robust observability, selective retries, failure quarantine, and idempotent rerun safety.

---

## 2. First Delivery Baseline and Repository Link

**First Delivery Repositories** 
- [https://github.com/Johann0811/Budget-To-Brilliance](https://github.com/Johann0811/Budget-To-Brilliance)  
- [https://github.com/Bl4ck-Grimoire/Budget-To-Brilliance](https://github.com/Bl4ck-Grimoire/Budget-To-Brilliance)

### Baseline Scope and Transition Summary
The First Delivery established the baseline problem, SDG 4 context, and an initial dimensional model centered exclusively on ICFES Saber 11 test attempts. While functionally capable of answering basic descriptive queries (R1–R5), the baseline system exhibited several engineering limitations that have been corrected in this Second Delivery:

| Baseline Limitation (First Delivery) | Engineering Correction (Second Delivery) | Rationale |
|---|---|---|
| **Masking missing keys with `fillna(-1)`** | Explicit distinction between legitimate non-affiliated examinees (*validantes*) and unmapped schools. | Using `fillna(-1)` silently masked mapping failures. In 2D, real schools that fail mapping trigger `Critical` quality failures (`DQ-11`, `DQ-26`). |
| **Silent numeric conversion via `errors='coerce'`** | Strict regex validation (`DQ-06`) and conversion audits (`T-14`, `audit_conversion_ok`). | Converting invalid score strings into `NaN` silently hid source corruption. |
| **No automated quality gates** | Implementation of 3 explicit Great Expectations gates (`Gate 1a`, `Gate 1b`, `Gate 2`) enforcing 28 traceable rules. | Data quality is now programmatic and halts downstream processing on critical contract or domain violations. |
| **Free-text departmental geography** | Formalized conformed dimension `dim_departamento` based on official 2-digit DANE codes (`T-06`). | Free text prevented integration with external national administrative registries. |
| **Unorchestrated execution** | End-to-end workflow orchestrated in Apache Airflow 3.1.8 using the TaskFlow API with metadata-only XCom contracts. | Replaced ad-hoc script runs with automated dependency management, retries, and SLA observability. |
| **Isolated data source** | Programmatic ingestion of the MEN Open Data API (`ji8i-4anb`). | Enriched exit-level exam scores with structural trajectory indicators (dropout and repetition). |

---

## 3. Status and Changes for R1–R5

In accordance with course continuity requirements, the five baseline requirements were preserved exactly as they were before:

| ID | Requirement Name | Status | Output in Second Delivery | Analytical / Design Justification |
|---|---|---|---|---|
| **R1** | **Official vs. Non-Official School Gap** | **Maintained** | Average `punt_global` and student volume grouped by `periodo` and `cole_naturaleza`. | Preserved as the core baseline indicator of socioeconomic stratification in Colombian education. Excludes non-school examinees (`cole_naturaleza <> 'No aplica'`). |
| **R2** | **Score Gap by School Shift (*Jornada*)** | **Maintained** | Average `punt_global` by `cole_naturaleza` × `cole_jornada`. | Perserved as a close domain standardized (`COMPLETA`, `UNICA`, `MAÑANA`, `TARDE`, `NOCHE`, `SABATINA`). |
| **R3** | **Urban-Rural Gap by Department** | **Refined** | Top 10 departments with highest absolute gap (`urbano_prom - rural_prom`) and student count. | Standardized categorical values in `cole_area_ubicacion` (merging legacy `'URBANA'` into `'URBANO'` via `T-07`). |
| **R4** | **Departmental Performance Ranking** | **Maintained** | Ranked average global score by department across all 9 periods (2021-2 to 2025-2). | Preserved to analyze regional learning convergence/divergence over time. Now joined through the conformed `dim_departamento` via DANE codes. |
| **R5** | **Gender Gap in Global and Core Subjects** | **Maintained** | Evolution of average scores (`punt_global`, `punt_matematicas`, `punt_lectura_critica`) across male (`M`) and female (`F`) examinees. | Preserves the longitudinal monitoring of gender parity in STEM and reading literacy. Unrecorded gender values map to surrogate key `-1` ("No informado") and are safely excluded from the binary gap calculation. |

---

## 4. New Integrated Analytical Requirements R6 and R7

Requirements **R6** and **R7** answer novel analytical questions that cannot be addressed by either source in isolation. They integrate student performance with institutional trajectory vulnerability:

| ID | Analytical Requirement | Primary Source Data (ICFES) | API Data (MEN Socrata) | Integration Key & Strategy | Expected KPI / Measure | Required Level of Detail |
|---|---|---|---|---|---|---|
| **R6** | **Upper Secondary Dropout vs. Saber 11 Performance**<br>How does the upper-secondary dropout rate (*deserción media*) correlate with standardized academic achievement across Colombian departments? | `fact_resultado_saber11` (`punt_global`), `dim_colegio` (`id_departamento`), `dim_periodo` (`id_anio`). Filtered to valid students (`id_colegio <> -1`, `puntaje_sospechoso = FALSE`). | `fact_indicador_departamento` (`desercion_media`, `id_departamento`, `id_anio`). | Conformed dimensions `dim_departamento` (`id_departamento`) and `dim_anio` (`id_anio`). **Pre-aggregation strategy:** Student-level ICFES scores are aggregated to department × year *prior* to joining with MEN indicator facts (`T-12`, `DQ-30`). | **KPI-06a:** Mean Saber 11 score vs. Deserción Media rate (%) per department-year.<br>**KPI-06b:** Annual Spearman rank correlation coefficient ($\rho$).<br>**KPI-06c:** Bivariate departmental risk quadrants (High Dropout / Low Performance). | Department × Calendar Year (33 DANE departments × 3 years: 2022–2024 = 99 panel observations). |
| **R7** | **Secondary Grade Repetition vs. Saber 11 Performance**<br>What is the relationship between secondary grade retention (*repitencia secundaria*) and final standardized exit exam outcomes at the departmental level? | `fact_resultado_saber11` (`punt_global`), `dim_colegio` (`id_departamento`), `dim_periodo` (`id_anio`). Filtered to valid students. | `fact_indicador_departamento` (`repitencia_secundaria`, `id_departamento`, `id_anio`). | Conformed dimensions `dim_departamento` (`id_departamento`) and `dim_anio` (`id_anio`). Pre-aggregated at department × year. | **KPI-07a:** Mean Saber 11 score vs. Repitencia Secundaria rate (%) per department-year.<br>**KPI-07b:** Annual Spearman rank correlation coefficient ($\rho$).<br>**KPI-07c:** Departmental ranking of academic vulnerability combining highest repetition and lowest scores. | Department × Calendar Year (33 DANE departments × 3 years: 2022–2024 = 99 panel observations). |

---

## 5. Primary Source and Programmatic API Source

### 5.1 Primary Source: ICFES Saber 11 Exam Registries
- **Provider:** Instituto Colombiano para la Evaluación de la Educación (ICFES), DataIcfes open repository.
- **Data Scope:** 9 academic exam semesters: `2021-2`, `2022-1`, `2022-2`, `2023-1`, `2023-2`, `2024-1`, `2024-2`, `2025-1`, and `2025-2`.
- **Physical Volume:** 3,293,842 total examinee records; ~2.2 GB of raw delimited text files.
- **Format & Encodings:** Flat text files (`.txt`), delimited with diverse separators (`|`, `;`, `,`, `\t`, `¬`), encoded across `utf-8`, `latin-1`, and `utf-8-sig`. DataIcfes uses the two-byte UTF-8 character `¬` as separator, which the extraction engine streams into a temporary single-byte control character (`0x1F`) to enable high-throughput zero-copy ingestion via Polars.
- **Storage:** Persisted uncleaned in `data/raw/primary/<batch_id>/` as snappy-compressed Parquet per period alongside `manifest.json`.

### 5.2 API Source: MEN Estadísticas en Educación PBM por Departamento
- **Provider & Service:** Ministerio de Educación Nacional (MEN) via Socrata Open Data API (*Datos Abiertos Colombia*).
- **Dataset Identifier:** `ji8i-4anb`
- **Endpoint URL:** `https://www.datos.gov.co/resource/ji8i-4anb.json`
- **Programmatic Extraction Details (`src/extract/api_extract.py`):**
  - **HTTP Client:** Python `requests` with custom headers (`Accept: application/json`, optional `X-App-Token`).
  - **Service Verification:** Queries `$select=count(*)` prior to pagination to record official server row counts.
  - **Pagination Strategy:** Loops `$limit=1000`, `$offset`, and `$order=:id` until the remote dataset is exhausted (avoids standard 1,000-row Socrata truncations).
  - **Fault Tolerance:** Bounded retries (3 attempts) with exponential backoff (`30s`, `60s`, `120s`) for transient HTTP 429/5xx status codes.
  - **Extraction Scope:** Complete dataset downloaded (462 raw records spanning 2011–2024). Filtering to the project window (`ano > 2021`) is deliberately deferred to the transformation layer (`T-01`) so that Gate 1b can validate complete extraction against the service counter (`DQ-15`).
  - **Storage:** Persisted in `data/raw/api/<batch_id>/men_estadisticas_departamento.parquet` with cryptographic SHA-256 hash and metadata in `manifest.json`.

---

## 6. Analytical Value Contributed by the API

The integration of the MEN API transforms the analytical scope of the Data Warehouse:

```mermaid
flowchart TD
    subgraph S1 ["ICFES Primary Source"]
        direction TB
        I1["• Exit-level exam scores"]
        I2["• School characteristics & shift"]
        I3["• Individual examinee demographics"]
    end

    subgraph S2 ["MEN Socrata API"]
        direction TB
        M1["• Grade repetition rates (Grades 6-9)"]
        M2["• Dropout rates in media (Grades 10-11)"]
        M3["• Macro institutional efficiency"]
    end

    subgraph DW ["Integrated Dimensional Data Warehouse"]
        direction TB
        D1["• Uncovers hidden educational traps"]
        D2["• Eliminates survivorship bias"]
        D3["• Bivariate vulnerability quadrant analysis"]
        D4["• Departmental correlation metrics"]
    end

    S1 --> DW
    S2 --> DW
```

1. **Overcoming Survivorship Bias:** A department might register deceptively high Saber 11 average scores simply because high dropout rates in Grades 10 and 11 purged academically vulnerable students before exam registration. By fusing `desercion_media` with `punt_global`, educational planners can distinguish genuine academic excellence from selective attrition.
2. **Identifying Structural Bottlenecks:** Comparing `repitencia_secundaria` against final exit exam scores establishes whether early retention interventions successfully remediate learning deficits or merely delay student departure.
3. **Evidence-Based Resource Allocation:** Rather than rewarding departments based solely on exit scores, the national government can direct funding toward territories suffering the compound burden of low performance and high educational desertion.

---

## 7. System Architecture

The pipeline follows a decoupled, observable batch architecture orchestrated by Apache Airflow 3.1.8:

```mermaid
flowchart TD
    subgraph Sources ["1. External Data Sources"]
        S1["ICFES Flat Files (9 Periods)\n3.29M Rows (.txt)"]
        S2["MEN API (Socrata REST)\nji8i-4anb.json"]
    end

    subgraph Airflow ["2. Apache Airflow 3.1.8 Orchestration (TaskFlow API)"]
        T0["prepare_gx_project\n(Init GX Context)"]
        
        subgraph BranchPrimary ["Primary Source Branch"]
            T1["extract_primary\n(Stream to Raw Parquet)"]
            T2["validate_primary_raw\n(Gate 1a: DQ-01,03,04,06,08,09,10)"]
        end
        
        subgraph BranchAPI ["API Source Branch"]
            T3["extract_api\n(HTTP Paging + Parquet)"]
            T4["validate_api_raw\n(Gate 1b: DQ-15,16,19,21,22,24)"]
        end
        
        subgraph Convergence ["Convergence & Integration"]
            T5["transform_and_integrate\n(T-01..T-14: Cleaning, Keys & Pre-agg)"]
            T6["validate_prepared\n(Gate 2: DQ-02,05,07,11,12,13,17,18,20,23,25..30)"]
        end
        
        subgraph TargetDW ["Warehouse Load & Audit"]
            T7["load_dimensional_dw\n(Full Refresh: Dims -> Facts)"]
            T8["reconcile_dw\n(Count Reconciliation & 7 Integrity Checks)"]
        end
    end

    subgraph Storage ["3. Storage & Analytics Layers"]
        P_RAW["data/raw/primary & api\n(Immutable Raw Parquet)"]
        P_PREP["data/prepared/\n(Star Schema Parquet)"]
        MYSQL[("MySQL 8.0 DW (icfes_dw)\nInnoDB / utf8mb4")]
        BI["Power BI Dashboard & SQL Analytics\n(R1 - R7 KPIs)"]
    end

    S1 --> T1
    S2 --> T3
    T0 --> T2
    T0 --> T4
    T1 --> P_RAW --> T2
    T3 --> P_RAW --> T4
    T2 & T4 -->|Both Gates PASS| T5
    T5 --> P_PREP --> T6
    T6 -->|Gate 2 PASS| T7
    T7 --> MYSQL --> T8
    MYSQL --> BI
```

---

## 8. Data Profiling Findings

Rigorous profiling was conducted across both sources without modifying the underlying data:

### 8.1 Primary Source Profiling (ICFES Saber 11)
- **Scale:** 3,293,842 examinee attempts across 9 periods (`2021-2` to `2025-2`).
- **Delimiter Inconsistencies:** The file format varied between semicolons, pipes, and double-byte `¬` delimiters across periods.
- **Key Uniqueness & Duplicates:** Exact duplicate examinee rows were discovered in historical data (e.g. 2 exact duplicates in period `2025-2`). Conflicting keys (`estu_consecutivo` associated with differing score values) were isolated.
- **Missing School Key:** 451,150 examinees (13.73%) lacked school codes (`cole_codigo_icfes = NULL`). These correspond to *validantes* (independent examinees).
- **Gender Reporting:** 141 records (0.004%) contained null or unrecorded gender values.
- **Suspicious Scores:** 28 records across the dataset contained global scores of exactly 0 (`punt_global = 0`). These represent test invalidations or administrative absences.
- **Geographic Free-Text Inconsistency:** Department names in `cole_depto_ubicacion` contained 81 distinct spelling variants, accents, and casing differences (e.g., `'BOGOTA D.C.'`, `'BOGOTA'`, `'SAN ANDRES'`, `'VALLE'`).
- **Domain Variants:** `cole_area_ubicacion` contained both `'URBANA'` and `'URBANO'` representing the same category.

### 8.2 API Source Profiling (MEN Socrata API)
- **Dataset Span:** 462 total records representing 33 DANE departments across 14 years (2011 to 2024).
- **Zero-Padding on Keys:** Department codes (`c_digo_departamento`) were stripped of leading zeros (e.g. `'5'` for Antioquia instead of `'05'`), preventing direct joins with national DANE registries.
- **Defective / Copied Attributes:** Profiling detected severe data quality defects in 7 raw API columns:
  - `tamano_promedio_grupo` and `sedes_conectadas_a_internet` were 100% null.
  - Columns such as `aprobacion_secundaria` and `reprobacion_primaria` were identical copies of other columns across $\ge 79\%$ of rows. A strict whitelist (`T-03`, `DQ-23`) was established to bar these defective fields from entering the Data Warehouse.
- **Panel Invariants:** For valid rows, the conservation identity $|aprobacion + reprobacion + desercion - 100| \le 0.5$ held across the project window.
- **Historical Rate Volatility:** Evaluation of year-over-year rate changes between 2012 and 2021 established empirical P99 anomaly thresholds: **6.72 pp** for dropout and **8.78 pp** for repetition.

---

## 9. Quality Risks and Quality Rules

The pipeline enforces **28 explicit data quality rules** grouped across layers. Each rule has an assigned severity and engineering justification:

### 9.1 Severity Policy Interpretation
| Severity | Policy Action | Pipeline Response |
|---|---|---|
| **Critical** | `STOP` | The affected branch halts immediately; an `AirflowFailException` is raised; downstream tasks transition to `upstream_failed`; Data Warehouse transactions are rolled back. |
| **Warning** | `CONTINUE_WITH_WARNINGS` | The anomaly is logged and preserved in the run's validation evidence JSON; pipeline execution continues normally. |
| **Informational** | `RECORD` | Recorded exclusively for longitudinal monitoring, drift tracking, and operational awareness; never halts processing. |

### 9.2 Complete Data Quality Rules Catalog
*(Single source of truth defined in `src/validation/dq_rules.py`)*

| Rule ID | Layer / Source | Attribute(s) | Quality Dimension | Quality Rule Description | Metric or Threshold | Severity | Related Requirements | Engineering Justification & Basis |
|---|---|---|---|---|---|---|---|---|
| **DQ-01** | ICFES Raw / Prep | `estu_consecutivo`, `id_periodo` | Uniqueness | Exam attempt appears once per period; key is non-null. Exact copies dropped (`T-09`); conflicting keys quarantined. | Prep: duplicates = 0, quarantined = 0.<br>Raw: $\ge 99.99\%$ unique. | **Critical** | R1–R7 | Historical profiling identified 2 duplicate rows in 3.29M records. Quarantining protects Fact table primary key integrity. |
| **DQ-02** | ICFES Raw $\to$ Prep | Row counts per period | Completeness | Prepared rows must exactly equal raw rows minus exact duplicates minus quarantined records. | Reconciliation difference = 0 for all 9 periods. | **Critical** | R1–R7 | Contractual pipeline requirement; prevents silent data loss during ingestion. |
| **DQ-03** | ICFES Raw | `periodo` | Completeness | All 9 expected periods (`2021-2` through `2025-2`) must be present with row count > 0. | 9 of 9 periods present; rows per period > 0. | **Critical** | R1–R7 | Defines the contractual analytical scope of the project. |
| **DQ-04** | ICFES Raw | Required ETL columns | Consistency | Manifest must verify that all 14 columns required by the ETL exist in each period file. | Missing required columns = 0. | **Critical** | R1–R7 | Contractual integrity; ICFES schema evolved across 5 periods. |
| **DQ-05** | ICFES Prepared | `punt_global`, `punt_*` | Validity | Test scores must reside within official ICFES scales. | `punt_global` in $[0, 500]$ (**Critical**); subject scores in $[0, 100]$ (**Warning**). | **Critical** | R1–R7 | Official examination scoring specifications. |
| **DQ-06** | ICFES Raw / Prep | `punt_*` | Validity | All raw scores must be numeric text; conversion must yield zero new nulls. | Non-numeric strings = 0; conversion loss = 0. | **Critical** | R1–R7 | Prevents silent coercion errors (`errors='coerce'`) from dropping test results. |
| **DQ-07** | ICFES Prepared | `punt_global`, `puntaje_sospechoso` | Validity | Scores equal to 0 must be flagged as suspicious and excluded from performance KPIs. | Unflagged zeros = 0; flagged share $\le 0.01\%$. | **Warning** | R1–R7 | Historical baseline found 28 zero-score rows (0.00085%). 0.01% provides a 10x safety margin. |
| **DQ-08** | ICFES Raw / Prep | `estu_genero`, `id_genero` | Completeness | Unreported gender must be minimal; maps cleanly to `-1`. | Unreported share $\le 0.01\%$. | **Informational** | R5 | Historical baseline revealed 141 null records (0.004%). |
| **DQ-09** | ICFES Raw / Prep | `cole_area_ubicacion` | Validity | Raw: `URBANO`, `URBANA`, `RURAL`. Prepared: only `URBANO`, `RURAL`, or `'No aplica'`. | Out-of-domain values = 0; nulls for real schools = 0. | **Critical** | R2, R3 | Eliminates schema corruption; satisfies `NOT NULL` constraints in DW. |
| **DQ-10** | ICFES Raw / Prep | `cole_naturaleza`, `cole_jornada` | Validity | Values must belong to closed domains with zero character encoding corruption (e.g. N-tilde). | Naturaleza: 2 values; Jornada: 6 values; nulls for real schools = 0. | **Critical** | R1, R2 | Enforces dimensional integrity for R1 and R2 queries. |
| **DQ-11** | ICFES Prepared | `id_colegio`, `cole_codigo_icfes` | Consistency | `id_colegio = -1` allowed only when school code was null at source (*validante*). | Real school falling to `-1` = 0. | **Critical** | R1–R4, R6, R7 | Structural protection against legacy `fillna(-1)` bugs. |
| **DQ-12** | ICFES Prepared | School attributes in `dim_colegio` | Consistency | Schools must preserve attribute stability across periods. | $\ge 99\%$ of schools without attribute conflict across periods. | **Warning** | R1–R4, R6, R7 | Operational rule; bounds territorial misclassification bias to $< 1\%$. |
| **DQ-13** | ICFES Prepared | Foreign keys in `fact_resultado` | Integrity | Foreign keys (`id_colegio`, `id_genero`, `id_periodo`) must be non-null and non-orphan. | Foreign key nulls = 0; orphan keys = 0. | **Critical** | R1–R7 | Enforces relational integrity across the Fact Constellation. |
| **DQ-15** | MEN Raw Manifest | Required API fields | Completeness | Download must match official service record count without missing required fields. | Downloaded rows = Service count ($Diff = 0$); missing fields = 0. | **Critical** | R6, R7 | Contractual API verification; guarantees pagination successfully ingested all data. |
| **DQ-16** | MEN Raw | `c_digo_departamento`, `ano` | Uniqueness | One record per department and calendar year in the raw extract. | Duplicate keys = 0. | **Critical** | R6, R7 | Protects the natural grain of `FACT_INDICADOR_DEPARTAMENTO`. |
| **DQ-17** | MEN Prepared | `id_departamento`, `id_anio` | Completeness | The window must form a complete panel: all 33 departments across each active year. | Rows = $33 \times \text{years}$; distinct departments = 33. | **Critical** | R6, R7 | Guarantees balanced panel integrity for bivariate correlation. |
| **DQ-18** | MEN Prepared | `desercion_media`, `repitencia_secundaria` | Completeness | Core indicators must be populated for every panel observation. | Null values = 0. | **Critical** | R6, R7 | R6 and R7 cannot be computed with missing indicator values. |
| **DQ-19** | MEN Raw / Prep | Indicator rates | Validity | Rates must be numeric percentages within valid bounds. | Non-numeric = 0; values outside $[0, 100]$ = 0. | **Critical** | R6, R7 | Mathematical bounds of educational rates. |
| **DQ-20** | MEN Prepared | YoY rate change | Accuracy | Annual rate variation must remain within plausible historical bounds. | $\|\Delta \text{rate}\| \le \text{P99}$ of historical changes (2012–2021). | **Warning** | R6, R7 | Empirical baseline: 6.72 pp for dropout; 8.78 pp for repetition. |
| **DQ-21** | MEN Raw / Prep | `cod_dane_depto` | Validity | 2-digit department code must belong to official 33 DANE jurisdictions. | Codes outside DANE list = 0; unpadded codes = 0. | **Critical** | R6, R7 | DANE administrative code standard. |
| **DQ-22** | MEN Raw | Rate conservation identity | Consistency | Sum of rates must approximate 100%: $|Aprobación + Reprobación + Deserción - 100| \le 0.5$. Defective duplicate columns excluded. | $|Sum - 100| \le 0.5$; identical columns $\le 50\%$. | **Critical** | R6, R7 | Mathematical conservation of student cohorts; catches corrupted Socrata columns. |
| **DQ-23** | MEN Prep $\to$ DW | Fact table schema | Validity | Only whitelisted indicator columns enter the Data Warehouse. | Exact schema match with `fact_indicador_departamento`. | **Critical** | R6, R7 | Profiling safeguard barring defective API fields from DW. |
| **DQ-24** | MEN Raw Manifest | `ano` | Timeliness | Monitors whether MEN has published data beyond 2024. | Last year = 2024. | **Informational** | R6, R7 | Signals when analytical window should be expanded. |
| **DQ-25** | DW Prepared | Keys in `fact_indicador` | Integrity | Department and year keys must be unique, non-null, and non-orphan. | Duplicates = 0; nulls = 0; orphans = 0. | **Critical** | R6, R7 | Star schema structural integrity. |
| **DQ-26** | DW Prepared | `dim_colegio` $\to$ `dim_departamento` | Integrity | Every real school must resolve to a valid DANE department. | Real schools without department = 0; distinct depts = 33. | **Critical** | R6, R7 | Enables departmental geographic aggregation for R6 and R7. |
| **DQ-27** | DW Prepared | `dim_periodo` $\to$ `dim_anio` | Consistency | Each semester period points to its calendar year; `dim_anio` covers 2021–2025. | Mismatches = 0; calendar years = 2021–2025. | **Critical** | R6, R7 | Establishes the conformed time dimension shared by both facts. |
| **DQ-28** | DW Prepared | Department $\times$ Year coverage | Completeness | Every department-year in the window with ICFES examinees has a MEN row, and vice versa. | Unmatched department-years = 0. | **Critical** | R6, R7 | Prevents asymmetrical missing data in R6/R7 joins. |
| **DQ-30** | DW Prepared | Pre-aggregated grain | Consistency | ICFES must be aggregated to department $\times$ year before joining with MEN indicators. | Duplicates = 0; panel rows = 99 ($33 \times 3$). | **Critical** | R6, R7 | Eliminates row multiplication (fan-out) and double-counting. |

*(Note: DQ-14 and DQ-29 were formally retired during initial architecture design review because national quarterly score averages and minimum departmental student headcounts exhibit structural seasonal volatility that lacks defensible deterministic thresholds).*

---

## 10. Great Expectations Validation Design and Rule Traceability

Validation is automated using **Great Expectations 1.24.0** with fluent pandas runtime datasources:
- **GX Context:** Configured in `gx/great_expectations.yml`.
- **Runtime Binding:** High-speed Polars DataFrames are converted to PyArrow-backed pandas DataFrames strictly at the validation boundary.
- **Suite & Checkpoint Design:** Declaratively defined in `src/validation/expectations.py`. Each expectation carries `dq_rule_id` and `severity` inside its `meta` dictionary.
- **Traceability Chain:** Every check traces directly from `dq_rules.py` through its checkpoint to the serialized execution report in `data/runs/<batch_id>/validation/<gate>.json`.

```mermaid
flowchart TD
    REQ["Analytical Requirement"]
    RISK["Quality Risk Catalog"]
    RULE["DQ Rule ID (DQ.01-30)"]
    SEV["Severity: Critical / Warning / Info"]
    SUITE["GX Expectation in Suite"]
    RUNNER["Checkpoint Execution (gx_runner.py)"]
    EVID["Validation Evidence JSON"]
    GATE["Pipeline Policy Gate: CONTINUE / STOP"]

    REQ --> RISK
    RISK --> RULE
    RULE --> SEV
    RULE --> SUITE
    SUITE --> RUNNER
    SUITE --> EVID
    EVID --> GATE
```

### Checkpoints and Associated Data Objects
| Checkpoint Name | Validation Gate | Data Object Evaluated | Rules Implemented |
|---|---|---|---|
| `icfes_raw_manifest` | Gate 1a (Primary Raw) | `data/raw/primary/<batch>/manifest.json` | DQ-03, DQ-04 |
| `icfes_raw_data` | Gate 1a (Primary Raw) | Period Parquet files in `data/raw/primary/<batch>/` | DQ-01, DQ-06, DQ-08, DQ-09, DQ-10 |
| `men_raw_manifest` | Gate 1b (API Raw) | `data/raw/api/<batch>/manifest.json` | DQ-15, DQ-24 |
| `men_raw_data` | Gate 1b (API Raw) | `men_estadisticas_departamento.parquet` | DQ-16, DQ-19, DQ-21, DQ-22 |
| `men_raw_column_profile` | Gate 1b (API Raw) | Column correlation and uniqueness matrix | DQ-22 |
| `prepared_reconciliation` | Gate 2 (Prepared) | Row reconciliation counters per period | DQ-01, DQ-02 |
| `prepared_dim_colegio` | Gate 2 (Prepared) | `dim_colegio.parquet` | DQ-09, DQ-10, DQ-12, DQ-26 |
| `prepared_dim_departamento`| Gate 2 (Prepared) | `dim_departamento.parquet` | DQ-21, DQ-26 |
| `prepared_dim_time` | Gate 2 (Prepared) | `dim_periodo.parquet` and `dim_anio.parquet` | DQ-27 |
| `prepared_fact_resultado` | Gate 2 (Prepared) | `fact_resultado_*.parquet` | DQ-01, DQ-05, DQ-06, DQ-07, DQ-08, DQ-11, DQ-13 |
| `prepared_fact_indicador` | Gate 2 (Prepared) | `fact_indicador_departamento.parquet` | DQ-17, DQ-18, DQ-19, DQ-20, DQ-23, DQ-25 |
| `prepared_integration` | Gate 2 (Prepared) | `integration_depto_anio.parquet` | DQ-28, DQ-30 |

---

## 11. Raw Validation Design for Both Sources (Gate 1a & Gate 1b)

The ingestion layer implements two independent, parallel raw validation gates:

```mermaid
flowchart TD
    subgraph PrimaryBranch ["Primary Branch"]
        EP["Extract Primary"]
        G1A["Gate 1a: validate_primary_raw"]
        EP --> G1A
    end

    subgraph APIBranch ["API Branch"]
        EA["Extract API"]
        G1B["Gate 1b: validate_api_raw"]
        EA --> G1B
    end

    subgraph Convergence ["Convergence"]
        TI["transform_and_integrate"]
    end

    G1A -->|PASS| TI
    G1B -->|PASS| TI
```

1. **Gate 1a (`validate_primary_raw`):** Evaluates incoming ICFES batches before any cleaning occurs. Ensures all 9 periods exist (`DQ-03`), all 14 required columns exist (`DQ-04`), primary key duplicates stay within historical margins (`DQ-01`), numeric fields contain valid strings (`DQ-06`), and closed categorical domains are respected (`DQ-09`, `DQ-10`).
2. **Gate 1b (`validate_api_raw`):** Validates raw Socrata API downloads. Verifies complete row count matching against server counters (`DQ-15`), checks department-year uniqueness (`DQ-16`), validates DANE department codes (`DQ-21`), and enforces the conservation identity ($|Aprob + Reprob + Deser - 100| \le 0.5$) while rejecting defective columns (`DQ-22`).
3. **Branch Convergence Rule:** Downstream transformation (`transform_and_integrate`) is blocked unless **both Gate 1a and Gate 1b return a `CONTINUE` or `CONTINUE_WITH_WARNINGS` decision**. A single `Critical` failure halts the pipeline before transformation begins.

---

## 12. Transformation Strategy and Integration Contract

### 12.1 Transformation Catalog (T-01 to T-14)
*(Defined in `src/transform/rules.py`)*

| ID | Rule & Rationale | Affected Fields | Before $\to$ After Behavior | Exception Handling | Analytical / Dimensional Impact |
|---|---|---|---|---|---|
| **T-01** | Filter API records to project window (`ano > 2021`). Deferred to transformation so Gate 1b sees full download. | `ano` | 462 raw rows $\to$ 99 panel rows ($33 \text{ depts} \times 3 \text{ years}$). Rate changes computed *before* filter. | Non-numeric years halted at Gate 1b (`DQ-19`). | Establishes the active cohort of `FACT_INDICADOR_DEPARTAMENTO`. |
| **T-02** | Pad 1-digit department codes with leading zero. | `c_digo_departamento` | `'5'` $\to$ `'05'`; `'11'` remains `'11'`. | Codes outside DANE list fail `DQ-25`. | Enables joins with conformed `dim_departamento`. |
| **T-03** | Whitelist approved columns; drop 7 defective/copied API fields. | 7 defective API columns | 37 columns $\to$ 4 dimensional/measure columns. | Excluded fields blocked by `DQ-23`. | Protects Data Warehouse from corrupted or empty metrics. |
| **T-04** | Convert API text strings to numeric types. | `ano`, measures | Strings (`"3.71"`) $\to$ `INT` / `DECIMAL(5,2)`. | Unparseable text yields null; halted by `DQ-18`. | Enables quantitative aggregations for R6 and R7. |
| **T-05** | Map semester period to calendar year key. | `periodo` | `'2022-1'`, `'2022-2'` $\to$ `id_anio` of 2022. | Invalid periods halted by `DQ-03` / `DQ-27`. | Creates the shared conformed time dimension. |
| **T-06** | Normalize free-text department to DANE code. | `cole_depto_ubicacion` | Text variants (`"BOGOTA D.C."`) $\to$ DANE ID (`11`). | Unmapped text yields null; halted by `DQ-26`. | Core geographic bridge enabling R4, R6, and R7. |
| **T-07** | Harmonize area labels. | `cole_area_ubicacion` | `'URBANA'` $\to$ `'URBANO'`; `'RURAL'` unchanged. | Unexpected labels halted by `DQ-09`. | Standardizes area dimension for R2 and R3. |
| **T-08** | Flag sentinel scores without deletion. | `punt_global` | Zeros unflagged $\to$ `puntaje_sospechoso = TRUE`. | Monitored by `DQ-07`. | Excludes invalid test attempts from performance KPIs. |
| **T-09** | Deduplicate exact copies; quarantine key conflicts. | `estu_consecutivo`, `id_periodo` | Exact duplicates dropped; conflicting keys isolated to quarantine folder. | Quarantined rows halt load via `DQ-01`. | Guarantees primary key uniqueness in `FACT_RESULTADO_SABER11`. |
| **T-10** | Map unrecorded gender to `-1`. | `estu_genero` | `'F'` $\to$ `1`, `'M'` $\to$ `2`, `NULL` $\to$ `-1`. | Exceeding 0.01% flags `DQ-08`. | Isolates missing demographics without dropping test facts. |
| **T-11** | Assign `-1` only to legitimate non-school examinees (*validantes*). | `cole_codigo_icfes` | Null code $\to$ `-1`; real schools must resolve. | Real school missing ID halted by `DQ-11`. | Eliminates legacy `fillna(-1)` mapping errors. |
| **T-12** | Aggregate student scores to department-year *before* joining with MEN indicators. | `punt_global` | ~3.29M student rows $\to$ 99 departmental mean observations. | Mismatches halted by `DQ-28` / `DQ-30`. | Prevents catastrophic row multiplication and double-counting. |
| **T-13** | Resolve school attribute drift across periods. | School attributes in `dim_colegio` | Multiple historical records $\to$ single canonical profile per school code. | $> 1\%$ conflict flags `DQ-12`. | Preserves stable dimensional attributes for schools. |
| **T-14** | Parse test scores to decimal with conversion verification. | `punt_*` | Raw strings $\to$ `DECIMAL(5,1)`. Loss audited. | Conversion loss halted by `DQ-06`. | Protects score accuracy for R1–R7 calculations. |

### 12.2 Integration Contract

| Contract Element | Specification and Engineering Decision |
|---|---|
| **Integration Grain** | **Department × Calendar Year**. The sources describe different business processes at vastly different grains (student attempts vs. annual departmental administrative rates). They are connected through conformed dimensions `dim_departamento` and `dim_anio`. |
| **Joining Strategy** | **Pre-Aggregation Drill-Across**. Student-level test attempts are aggregated to `(id_departamento, id_anio)` *prior* to joining with `fact_indicador_departamento`. Joining before aggregating would multiply each MEN rate by thousands of students, corrupting averages. |
| **Integration Keys** | Conformed surrogate keys `id_departamento` (derived from 2-digit DANE codes) and `id_anio` (derived from calendar years). |
| **Temporal Alignment** | ICFES semester periods (`YYYY-1` and `YYYY-2`) map to calendar year `YYYY`. The MEN annual indicator applies to the calendar year. Analysis covers the balanced 3-year panel: 2022, 2023, and 2024. |
| **Unmatched Records** | Evaluated via full outer join (`DQ-28`). Zero tolerance for missing department-years across the 33 DANE jurisdictions. Validantes lacking school/department affiliation map to `-1` and are excluded from departmental macro-correlations. |

---

## 13. Refined Dimensional Model and Change Justification

### 13.1 Star Schema / Fact Constellation ERD

```mermaid
erDiagram
    DIM_DEPARTAMENTO ||--o{ DIM_COLEGIO : ubica
    DIM_ANIO ||--o{ DIM_PERIODO : agrupa
    DIM_COLEGIO ||--o{ FACT_RESULTADO_SABER11 : describe
    DIM_GENERO  ||--o{ FACT_RESULTADO_SABER11 : describe
    DIM_PERIODO ||--o{ FACT_RESULTADO_SABER11 : describe
    DIM_DEPARTAMENTO ||--o{ FACT_INDICADOR_DEPARTAMENTO : describe
    DIM_ANIO ||--o{ FACT_INDICADOR_DEPARTAMENTO : describe

    DIM_DEPARTAMENTO {
        int id_departamento PK
        string cod_dane_depto UK "2 digits, e.g. '05'"
        string nombre_departamento
    }

    DIM_ANIO {
        int id_anio PK
        int anio UK "2021-2025"
    }

    DIM_COLEGIO {
        int id_colegio PK
        int id_departamento FK
        string cole_codigo_icfes UK
        string cole_nombre_establecimiento
        string cole_naturaleza
        string cole_jornada
        string cole_area_ubicacion
        string cole_depto_ubicacion
        string cole_mcpio_ubicacion
    }

    DIM_GENERO {
        int id_genero PK
        string estu_genero
        string genero_descripcion
    }

    DIM_PERIODO {
        int id_periodo PK
        int id_anio FK
        string periodo UK "e.g. '2021-2'"
        int anio
        int semestre
    }

    FACT_RESULTADO_SABER11 {
        string estu_consecutivo PK
        int id_periodo PK, FK
        int id_colegio FK
        int id_genero FK
        decimal punt_lectura_critica
        decimal punt_matematicas
        decimal punt_sociales_ciudadanas
        decimal punt_c_naturales
        decimal punt_ingles
        decimal punt_global
        boolean puntaje_sospechoso
    }

    FACT_INDICADOR_DEPARTAMENTO {
        int id_departamento PK, FK
        int id_anio PK, FK
        decimal desercion_media "Percentage 0-100, NON-ADDITIVE"
        decimal repitencia_secundaria "Percentage 0-100, NON-ADDITIVE"
    }
```

### 13.2 Model Justification and Comparison of Alternatives

| Design Element | Decision | Justification & Why Alternatives Were Rejected |
|---|---|---|
| **Fact Constellation (2 Fact Tables)** | `FACT_RESULTADO_SABER11` and `FACT_INDICADOR_DEPARTAMENTO`. | **Alternative Rejected: Single Flat Fact Table.** Storing departmental dropout and repetition rates directly on student exam rows would duplicate macro-indicators millions of times, creating severe data redundancy and risking accidental additive summation of rates.<br>**Alternative Rejected: Snowflake Normalization.** Normalizing municipalities or school categories into separate snowflake tiers adds unnecessary join overhead without improving query flexibility. |
| **Declared Grains** | Exam Fact: One test attempt of one student in one semester period.<br>Indicator Fact: Annual educational statistics of one DANE department in one calendar year. | Preserves atomic detail for individual test analysis while maintaining clean, uncorrupted departmental rates. |
| **Conformed Dimensions** | `dim_departamento` and `dim_anio`. | Act as the shared dimensional bridge, enabling drill-across SQL queries and cross-functional reporting across both business processes. |
| **Non-Additive Measures** | `desercion_media` and `repitencia_secundaria` are rates (percentages). | Rates must **never be summed (`SUM`)** across departments or years. They are strictly evaluated per department-year or aggregated via weighted averages. |
| **Surrogate & Natural Keys** | Deterministic surrogate integer IDs paired with unique business keys. | Ensures fast indexed joins in MySQL and idempotent surrogate key stability across pipeline reruns. |

---

## 14. Prepared-Data Validation (Gate 2)

Gate 2 (`validate_prepared`) is executed after transformation and integration, acting as the final gatekeeper before the Data Warehouse is touched:

```mermaid
flowchart TD
    TPD["Transformed Parquet Data"]
    G2["Gate 2: validate_prepared"]
    LOAD["load_dimensional_dw"]
    HALT["PIPELINE HALTED / DW UNTOUCHED"]

    TPD --> G2
    G2 -->|PASS| LOAD
    G2 -->|CRITICAL FAIL| HALT
```

### Gate 2 Evaluated Invariants:
1. **Reconciliation Invariants (`DQ-02`):** Verifies that for every period:  
   $$\text{Raw Rows} - \text{Exact Duplicates Removed} - \text{Quarantined Rows} - \text{Prepared Rows} = 0$$
2. **Referential Integrity (`DQ-13`, `DQ-25`, `DQ-26`, `DQ-27`):** Guarantees zero null and zero orphan foreign keys in both fact tables before loading begins.
3. **Domain & Value Bounds (`DQ-05`, `DQ-19`):** Confirms that all test scores reside in $[0, 500]$ and all rates reside in $[0, 100]$.
4. **Historical Anomaly Gates (`DQ-20`):** Checks year-over-year rate changes against empirical P99 thresholds (6.72 pp for dropout; 8.78 pp for repetition).
5. **Integration Completeness (`DQ-17`, `DQ-28`, `DQ-30`):** Confirms that the integrated panel comprises exactly 99 observations ($33 \text{ departments} \times 3 \text{ years}$) with zero missing links.

---

## 15. Data Warehouse Implementation and Load

### 15.1 Implementation Technologies
- **RDBMS Engine:** **MySQL 8.0** (`InnoDB`, `utf8mb4` encoding). Host port `3307`, container internal port `3306`.
- **Database Schema:** `icfes_dw` (separate from the Airflow PostgreSQL metadata database).
- **Idempotent DDL:** `sql/schema/create_dw.sql` uses `CREATE TABLE IF NOT EXISTS`, avoiding destructive `DROP` statements.

### 15.2 Full Refresh Load Strategy
To ensure absolute repeatability and prevent partial-load corruption, `src/load/load_all.py` executes a **full transactional refresh** per batch:
1. **Schema Verification:** Ensures all tables, constraints, and indexes exist.
2. **Reverse Truncation (`empty_warehouse`):** Empties tables in reverse dependency order:
   - `fact_resultado_saber11` $\to$ `fact_indicador_departamento` $\to$ `dim_colegio` $\to$ `dim_periodo` $\to$ `dim_genero` $\to$ `dim_anio` $\to$ `dim_departamento`.
3. **Forward Dimensional Ingestion (`load_dimensions`):** Inserts conformed dimensions in forward dependency order:
   - `dim_departamento` $\to$ `dim_anio` $\to$ `dim_genero` $\to$ `dim_periodo` $\to$ `dim_colegio`.
4. **Fact Table Ingestion (`load_facts`):** Loads prepared Parquet files using batch parameter substitution. Exam facts are committed period-by-period to bound MySQL buffer pool memory consumption.

### 15.3 Post-Load Reconciliation Audit (`src/load/reconcile.py`)
Immediately following ingestion, `reconcile_dw` verifies that the Data Warehouse perfectly mirrors accepted prepared data:
- Table-by-table and period-by-period row count comparisons between MySQL tables and prepared Parquet files.
- Execution of **7 automated SQL integrity queries** verifying that orphan records equal exactly zero:
  1. `fact_resultado_orphan_colegio` $= 0$
  2. `fact_resultado_orphan_genero` $= 0$
  3. `fact_resultado_orphan_periodo` $= 0$
  4. `dim_colegio_without_departamento` $= 0$
  5. `real_schools_in_no_aplica_departamento` $= 0$
  6. `fact_indicador_orphan_departamento` $= 0$
  7. `fact_indicador_orphan_anio` $= 0$
- If any discrepancy is detected, a `ReconciliationError` is raised, failing the task and recording evidence in `data/runs/<batch>/reconciliation.json`.

---

## 16. Airflow DAG Design and Task Interfaces

The complete pipeline is orchestrated via Apache Airflow 3.1.8 (`dags/budget_to_brilliance_dag.py`) using the public TaskFlow authoring API:

```python
from airflow.sdk import dag, task
```

### 16.1 DAG Specifications
- **DAG ID:** `budget_to_brilliance_etl`
- **Schedule:** `None` (manual trigger; ICFES data publishes semiannually, MEN data annually).
- **Concurrency:** `max_active_runs = 1` (guarantees that concurrent full-refresh runs never clash in MySQL).
- **Timeout:** `dagrun_timeout = timedelta(hours=3)`

### 16.2 Task Interfaces and Dependency Graph

```mermaid
flowchart TD
    T0["prepare_gx_project"]
    
    subgraph BranchPrimary ["Primary Branch"]
        T1["extract_primary"]
        T2["validate_primary_raw"]
        T1 --> T2
    end

    subgraph BranchAPI ["API Branch"]
        T3["extract_api"]
        T4["validate_api_raw"]
        T3 --> T4
    end

    T0 --> T2
    T0 --> T4

    T5["transform_and_integrate"]
    T6["validate_prepared"]
    T7["load_dimensional_dw"]
    T8["reconcile_dw"]

    T2 --> T5
    T4 --> T5
    T5 --> T6
    T6 --> T7
    T7 --> T8
```

### 16.3 Task Interface Contracts (Airflow as Orchestrator, Not Transport)
Tasks exchange **compact metadata references** via XCom (batch IDs, record counts, execution status, evidence file paths). Bulk data remains strictly on the file system (Parquet) and in MySQL:

| Task ID | Input Contract | Output Contract (XCom) | Retries | Timeout |
|---|---|---|---|---|
| `prepare_gx_project` | Project root path | `{"checkpoints": 12}` | 0 | 5 min |
| `extract_primary` | `ICFES_SOURCE_DIR`, `batch_id` | `{"batch_id": "...", "rows": 3293842, "periods": 9}` | 0 | 1 hour |
| `validate_primary_raw`| Primary raw batch Parquet | Compact gate summary dictionary | 0 | 30 min |
| `extract_api` | Socrata API URL, `batch_id` | `{"batch_id": "...", "rows": 462, "service_count": 462}` | 3 (backoff) | 10 min |
| `validate_api_raw` | API raw Parquet | Compact gate summary dictionary | 0 | 30 min |
| `transform_and_integrate`| Gate 1a & Gate 1b summaries | `{"icfes_rows_prepared": ..., "integration_rows": 99}` | 0 | 1 hour |
| `validate_prepared` | Prepared Parquet folder | Compact gate summary dictionary | 0 | 30 min |
| `load_dimensional_dw` | Gate 2 summary | Dimension and Fact loaded row counts | 2 (db backoff)| 2 hours |
| `reconcile_dw` | Loaded DW summary | `{"checks": 17, "failed": 0, "decision": "OK"}` | 2 (db backoff)| 30 min |

---

## 17. Failure and Retry Policies

A reliable data engineering pipeline must distinguish **transient operational faults** from **deterministic data and contract violations**:

| Failure Class | Example Scenarios | Expected Pipeline Behavior | Retry Policy | Engineering Justification |
|---|---|---|---|---|
| **Transient Operational (Network / API)** | Remote Socrata API timeout, HTTP 502/503/429 rate limit, temporary network drop. | Retry task automatically with exponential backoff. | **3 retries** (30s, 60s, 120s backoff). | Transient cloud network drops or rate limit windows typically clear within seconds. Bounded retries prevent hammering external endpoints. |
| **Transient Database (Locks / Connection)** | MySQL container initial handshake delay, deadlock, lock wait timeout (`codes 2003, 2006, 2013, 1205, 1213`). | Bounded retry after delay. | **2 retries** (1 min, 2 min backoff). | The full refresh transaction allows safe replay. If the database engine was restarting, it recovers cleanly. |
| **Deterministic Data Quality Failure** | Critical rule failure (`DQ-01` duplicate keys, `DQ-05` out-of-bounds score, `DQ-18` null dropout rate). | Immediate failure; halt pipeline; serialize failure JSON evidence. | **0 retries** | Deterministic: repeating the exact same calculation on the exact same static files will always yield the exact same violation. |
| **Deterministic Contract Failure** | Missing mandatory column (`DQ-04`), unpadded DANE code (`DQ-21`), unexpected schema (`DQ-23`). | Immediate failure; halt downstream dependencies. | **0 retries** | Code or source schema must be corrected by an engineer before retrying. |
| **Reconciliation Mismatch** | MySQL row counts do not match prepared Parquet counts or orphan records detected. | Transaction rolled back; raise `ReconciliationError`. | **0 retries** | Data loading logic or constraint definitions must be debugged. |

---

## 18. Mandatory Reliability Test A: Successful Execution Evidence

### 18.1 End-to-End Success Execution Log
Test A validates the happy-path execution of the complete pipeline using the production batch:

![successful execution](docs/resources/try-good.png)

![successful execution task](docs/resources/try-good-task.png)

![successful execution graph](docs/resources/try-good-graph.png)

---

## 19. Mandatory Reliability Test B: Controlled Critical Failure Evidence

### 19.1 Failure Injection Scenario
To demonstrate that the system anticipates, controls, and isolates failures, a controlled corruption was injected into the incoming API dataset: an out-of-domain department code (`c_digo_departamento = '999'`) was introduced into the API batch.

![Controlled execution](docs/resources/try-bad.png)

![Controlled execution logs](docs/resources/try-bad-logs.png)

![Controlled execution task](docs/resources/try-bad-task.png)

![Controlled execution graph](docs/resources/try-bad-graph.png)
---

## 20. Mandatory Reliability Test C: Repeatability and Safe-Rerun Strategy

### 20.1 Idempotent Execution Strategy
A pipeline rerun must never silently corrupt the target warehouse, produce duplicate facts, or alter surrogate key relationships. The system achieves safe repeatability through:
1. **Deterministic Batch Identity:** `batch_id` is derived from the Airflow `run_id` or passed explicitly. Clearing a failed task in the Airflow UI reuses the exact same batch identity.
2. **Full Transactional Refresh:** `load_all` executes an atomic truncate-and-reload cycle. It empties all facts and dimensions before inserting the accepted batch, ensuring the database reflects exactly the last accepted run.
3. **Deterministic Key Assignment:** Surrogate keys in all dimensions are generated from sorted natural business keys (e.g. sorted DANE codes, sorted school ICFES codes). Re-running the same dataset produces identical surrogate IDs.
4. **Natural Composite Primary Keys:** Both fact tables enforce composite primary keys:
   - `fact_resultado_saber11`: `(estu_consecutivo, id_periodo)`
   - `fact_indicador_departamento`: `(id_departamento, id_anio)`

But lets se it

**Before**

![Rerun execution before](docs/resources/rerun-logs-before.png)

**After**

![Rerun execution after](docs/resources/rerun-logs-after.png)

As we can se, the DW didn't change a thing demonstrating that the rule for reruns works perfectly

---

## 21. Dashboard and Analytical Outputs

The final analytical product is a Power BI Dashboard connected **directly to the MySQL Data Warehouse (`icfes_dw`)**. Direct connections to CSVs, raw API endpoints, or intermediate staging files are strictly prohibited.

### 21.1 Dashboard Architecture and Pages
The dashboard translates SQL analytical queries (`sql/analytics/analytical_queries.sql`) into interactive visual reports:

#### Page 1: Baseline Structural Equity (Requirements R1, R2, R3)

![Dahsboard page 1](docs/resources/Visualization-1.jpeg)

#### Page 2: Longitudinal Departmental Evolution (Requirements R4, R5)

![Dahsboard page 2](docs/resources/Visualization-2.jpeg)

#### Page 3: Integrated API Analytics: Dropout vs. Performance (Requirement R6)

![Dahsboard page 3](docs/resources/Visualization-3.png)

### 21.2 Requirement to SQL Query and DW Element Mapping

| Requirement | Dashboard KPI / Visualization | Underlying SQL Query Logic | Supporting DW Tables & Columns |
|---|---|---|---|
| **R1** | Official vs. Non-official gap over time | `AVG(f.punt_global)` grouped by `p.periodo, c.cole_naturaleza` (excluding `'No aplica'`). | `fact_resultado_saber11`, `dim_colegio`, `dim_periodo` |
| **R2** | Score by shift controlling for sector | `AVG(f.punt_global)` grouped by `c.cole_naturaleza, c.cole_jornada`. | `fact_resultado_saber11`, `dim_colegio` |
| **R3** | Top 10 Urban-Rural score gap | Pivot average urban vs rural scores per department with `HAVING COUNT(*) >= 200`. | `fact_resultado_saber11`, `dim_colegio` |
| **R4** | Departmental score ranking | `AVG(f.punt_global)` grouped by `c.cole_depto_ubicacion, p.periodo` ordered descending. | `fact_resultado_saber11`, `dim_colegio`, `dim_periodo` |
| **R5** | Gender gap across subjects | `AVG(punt_global)`, `AVG(punt_matematicas)`, `AVG(punt_lectura_critica)` grouped by `g.genero_descripcion, p.periodo`. | `fact_resultado_saber11`, `dim_genero`, `dim_periodo` |
| **R6** | Upper secondary dropout vs score | CTE pre-aggregating valid examinee scores to `id_departamento, id_anio`, joined with `fact_indicador_departamento` on conformed keys. | `fact_resultado_saber11`, `fact_indicador_departamento`, `dim_departamento`, `dim_anio` |
| **R7** | Secondary repetition vs score | CTE pre-aggregating valid examinee scores to `id_departamento, id_anio`, joined with `fact_indicador_departamento` on conformed keys. | `fact_resultado_saber11`, `fact_indicador_departamento`, `dim_departamento`, `dim_anio` |

---

## 22. Requirement-to-KPI and End-to-End Traceability Matrix

The complete reasoning chain connects analytical requirements to every component of the data engineering lifecycle:

$$\text{Requirement} \to \text{Source Data} \to \text{Risk} \to \text{DQ Rule} \to \text{Severity} \to \text{GX Expectation} \to \text{Transformation} \to \text{DW Element} \to \text{KPI / Dashboard}$$

| Req ID | Required Source Data | Quality Risk | Quality Dimension & Rule | Severity | GX Expectation | Transformation & Integration | DW Element | Analytical KPI & Visualization |
|---|---|---|---|---|---|---|---|---|
| **R1** | ICFES `cole_naturaleza`, `punt_global` | Mislabeled or unrecorded school sector corrupting gap estimates. | Validity / `DQ-10` | **Critical** | `ExpectColumnValuesToBeInSet` (Naturaleza domain) | `T-11`: Exclude non-school examinees (`id_colegio = -1`). | `dim_colegio.cole_naturaleza`, `fact_resultado_saber11.punt_global` | **KPI-01**: Mean global score gap between Official and Non-Official schools per period. |
| **R2** | ICFES `cole_jornada`, `cole_naturaleza`, `punt_global` | UTF-8 encoding corruption in shift strings (e.g. `MAÑANA`). | Consistency / `DQ-10` | **Critical** | `ExpectColumnValuesToBeInSet` (Jornada domain) | `T-10`, `T-13`: Canonical school profile attribute assignment. | `dim_colegio.cole_jornada`, `fact_resultado_saber11.punt_global` | **KPI-02**: Average score by shift controlling for school sector. |
| **R3** | ICFES `cole_area_ubicacion`, `cole_depto_ubicacion` | Dual representation of urban category (`URBANA` vs `URBANO`). | Validity / `DQ-09` | **Critical** | `ExpectColumnValuesToBeInSet` (Area domain) | `T-07`: Replace `'URBANA'` with `'URBANO'`. | `dim_colegio.cole_area_ubicacion`, `dim_colegio.id_departamento` | **KPI-03**: Top 10 departments by absolute urban-rural achievement gap. |
| **R4** | ICFES `cole_depto_ubicacion`, `punt_global` | Free-text department variants preventing longitudinal grouping. | Consistency / `DQ-26` | **Critical** | `ExpectColumnValuesToNotBeNull` (`id_departamento`) | `T-06`: Normalize department text to 2-digit DANE code. | `dim_departamento.nombre_departamento`, `dim_colegio.id_departamento` | **KPI-04**: Departmental performance league table and period ranking. |
| **R5** | ICFES `estu_genero`, `punt_matematicas`, `punt_lectura_critica` | Unreported gender distorting binary equity analysis. | Completeness / `DQ-08` | **Info** | `ExpectColumnValuesToBeInSet` (`['F', 'M']`, mostly 0.9999) | `T-10`: Map missing gender to surrogate key `-1`. | `dim_genero.estu_genero`, `fact_resultado_saber11.punt_matematicas` | **KPI-05**: Longitudinal STEM and Reading gender gap evolution. |
| **R6** | ICFES `punt_global` + MEN API `desercion_media` | Row-fanout / double-counting when joining student attempts with macro rates. | Consistency / `DQ-30`, Completeness / `DQ-28` | **Critical** | `ExpectCompoundColumnsToBeUnique`, `ExpectTableRowCountToEqual` (99) | `T-01`, `T-02`, `T-04`, `T-12`: Aggregate ICFES to department-year before joining. | `dim_departamento`, `dim_anio`, `fact_indicador_departamento.desercion_media` | **KPI-06a/b**: Bivariate scatter plot of Dropout vs. Score; Spearman rank correlation ($\rho$). |
| **R7** | ICFES `punt_global` + MEN API `repitencia_secundaria` | Defective API fields or corrupted Socrata rates entering analysis. | Validity / `DQ-22`, `DQ-23` | **Critical** | `ExpectTableColumnsToMatchSet` (Whitelist), `ExpectColumnValuesToBeBetween` | `T-03`: Whitelist approved columns; drop 7 defective fields. `T-12`: Pre-aggregate. | `dim_departamento`, `dim_anio`, `fact_indicador_departamento.repitencia_secundaria` | **KPI-07a/b**: Repetition vs. Score correlation and vulnerability ranking. |

---

## 23. Setup and Execution Instructions

### 23.1 Environment Prerequisites
- **Docker Desktop:** Version 24.0+ (allocate at least 4 GB RAM and 2 CPUs in Docker settings).
- **Python:** Version 3.12 (optional; only required if executing scripts outside Docker).
- **Git:** For repository cloning and branch management.
- **Power BI Desktop:** With MySQL Connector / ODBC Driver installed.

### 23.2 Project Directory Organization
```text
Budget-To-Brilliance-2D/
├── dags/
│   └── budget_to_brilliance_dag.py        # Apache Airflow 3.1.8 DAG (TaskFlow API)
├── data/
│   ├── raw/
│   │   ├── primary/<batch_id>/            # Raw ICFES Parquet files + manifest.json
│   │   └── api/<batch_id>/                # Raw MEN Socrata Parquet + manifest.json
│   ├── prepared/<batch_id>/               # Validated Star Schema Parquet tables
│   ├── runs/<batch_id>/                   # Execution evidence (validation JSON, reports)
│   └── source/icfes/                      # 9 DataIcfes .txt source files (2021-2 to 2025-2)
├── docs/
│   ├── resources/
│   │   └── star_schema.mmd                # Mermaid ERD definition
│   └── document.md                        # Formal assignment requirements brief
├── gx/
│   └── great_expectations.yml             # Great Expectations 1.24.0 configuration
├── sql/
│   ├── analytics/
│   │   └── analytical_queries.sql         # SQL analytical queries for R1 - R7
│   └── schema/
│       └── create_dw.sql                  # Idempotent DDL for MySQL Data Warehouse
├── src/
│   ├── config.py                          # Centralized configuration, paths and domains
│   ├── policy.py                          # Severity policy engine (Critical/Warning/Info)
│   ├── run_pipeline.py                    # Standalone local pipeline runner
│   ├── utils.py                           # Cryptographic hashing, JSON and logging utils
│   ├── extract/
│   │   ├── primary_extract.py             # ICFES text streaming ingestion to Parquet
│   │   └── api_extract.py                 # Socrata API HTTP paged extraction
│   ├── transform/
│   │   ├── dimensions.py                  # Dimension table builders (dim_colegio, etc.)
│   │   ├── icfes_transform.py             # Student fact cleaning, deduplication & quarantine
│   │   ├── men_transform.py               # API cleaning, column whitelist & historical P99
│   │   └── rules.py                       # Transformation catalog (T-01 to T-14)
│   ├── integrate/
│   │   └── department_year.py             # Department-year pre-aggregation bridge
│   ├── validation/
│   │   ├── dq_rules.py                    # Single source of truth for 28 DQ rules
│   │   ├── expectations.py                # GX checkpoint expectation specifications
│   │   ├── gx_runner.py                   # Great Expectations execution engine
│   │   ├── validate_primary_raw.py        # Gate 1a execution logic
│   │   ├── validate_api_raw.py            # Gate 1b execution logic
│   │   └── validate_prepared.py           # Gate 2 execution logic
│   └── load/
│       ├── db.py                          # MySQL connection pool and DDL runner
│       ├── load_dimensions.py             # Dimension table loader
│       ├── load_facts.py                  # Fact table chunked loader
│       ├── load_all.py                    # Full refresh transactional load coordinator
│       └── reconcile.py                   # Post-load row-count & integrity query auditor
├── .env.example                           # Sample environment configuration
├── .gitignore                             # Git ignore specifications
├── docker-compose.yaml                    # Multi-container orchestration (Airflow + MySQL + Postgres)
├── Dockerfile                             # Custom Airflow 3.1.8 image with dependencies
├── requirements.txt                       # Python dependencies (polars, GX, PyMySQL, etc.)
└── README.md                              # Technical system documentation (Point 24)
```

### 23.3 Environment Setup (.env)
Copy `.env.example` to `.env` and verify configuration:
```bash
cp .env.example .env
```
Default configuration values:
```ini
DATABASE_URL=mysql+pymysql://root:changeme@localhost:3307/icfes_dw
MYSQL_ROOT_PASSWORD=changeme
MYSQL_DATABASE=icfes_dw
AIRFLOW_UID=50000
ICFES_SOURCE_DIR=data/source/icfes
MEN_API_URL=https://www.datos.gov.co/resource/ji8i-4anb.json
MEN_API_PAGE_SIZE=1000
MEN_MIN_YEAR_EXCLUSIVE=2021
COLEGIO_ATTR_POLICY=first
DUPLICATE_CONFLICT_POLICY=block
```

### 23.4 Execution via Docker and Apache Airflow
1. **Build and start the container stack:**
   ```bash
   docker compose up --build -d
   ```
2. **Verify container health:**
   ```bash
   docker compose ps
   ```
   Ensure `btb_airflow_postgres`, `btb_mysql`, `btb_airflow_apiserver`, `btb_airflow_scheduler`, and `btb_airflow_dag_processor` report `healthy` or `running`.
3. **Trigger the Pipeline:**
   - Navigate to the Airflow Web UI at [http://localhost:8080](http://localhost:8080).
   - Locate the DAG `budget_to_brilliance_etl`.
   - Unpause the DAG and trigger a manual execution via the **Trigger DAG** button.
   - Monitor live task execution in the **Grid** and **Graph** views.

### 23.5 Standalone CLI Execution (Outside Airflow)
The pipeline can also be executed directly via Python for local debugging and rapid verification:
```bash
# Activate virtual environment
python -m venv .venv
# On Windows:
.venv\Scripts\activate
# On Linux/macOS:
source .venv/bin/activate

# Install exact dependencies
pip install -r requirements.txt

# Execute end-to-end pipeline
python -m src.run_pipeline
```

### 23.6 Executing Analytical SQL Queries
Connect to the MySQL Data Warehouse to run analytical queries for R1–R7:
```bash
docker exec -it btb_mysql mysql -u root -pchangeme icfes_dw < sql/analytics/analytical_queries.sql
```

---

## 24. Assumptions and Limitations

1. **Surrogate Key Determinism:** Assigned surrogate keys in dimensions are deterministic within a full refresh cycle because natural business keys are sorted prior to row indexing.
2. **Department Free-Text Normalization:** Textual normalization in `T-06` relies on matching uppercase, accent-stripped strings against the official 33 DANE department catalog and known alias patterns. Any school whose textual municipality/department cannot be matched is blocked by `DQ-26` rather than silently assigned to member `-1`.
3. **Exclusion of Non-School Examinees (*Validantes*):** Examinees sitting for the exam independently without an institutional school code (~13.7%) map to `id_colegio = -1` and `id_departamento = -1`. They are retained in the Fact table for aggregate volume reporting, but are excluded from school-level comparisons (R1–R3) and departmental correlations (R6–R7).
4. **Temporal Granularity Alignment:** The MEN indicator dataset reports macro-statistics on an annual calendar cadence, whereas ICFES Saber 11 is administered semiannually. The integration model maps both semesters of a calendar year to the same annual indicator.
5. **Association vs. Causality:** The empirical correlations uncovered in R6 and R7 represent statistical associations across geographic territories. They do not demonstrate individual-level causal effects (e.g., repeating a grade does not necessarily cause an individual student to score lower on Saber 11).
6. **Defective Remote Columns:** As established during profiling, 7 columns in the public MEN Socrata dataset were defective (100% null or duplicate copies). The pipeline assumes that dropping these columns via `T-03` does not impair analytical capabilities, as the core measures (`desercion_media` and `repitencia_secundaria`) are complete and valid.

---

## 25. Team Members and Responsibilities

This project was engineered and defended as part of the ETL Course (G01) at the Universidad Autónoma de Occidente:

| Team Member | Academic Email | Institutional Affiliation | Core Responsibilities & Contributions |
|---|---|---|---|
| **Juan David Lasso Chaparro** | `juan_dav.lasso@uao.edu.co` | Faculty of Engineering and Basic Sciences, UAO | - Apache Airflow 3.1.8 DAG orchestration, task interfaces, and selective retry policy design.<br>- Programmatic Socrata API extraction client and pagination implementation.<br>- Data quality rule catalog (`dq_rules.py`) and Great Expectations checkpoint engineering.<br>- MySQL Data Warehouse DDL and dimensional star schema design. |
| **Johann Eduardo Gonzalez Sandoval** | `johann_edu.gonzalez@uao.edu.co` | Faculty of Engineering and Basic Sciences, UAO | - High-performance ICFES raw streaming ingestion engine with Polars.<br>- Business transformation catalog implementation (`T-01` to `T-14`).<br>- Department-year pre-aggregation integration module (`T-12`, `DQ-30`).<br>- Analytical SQL query formulation (R1–R7) and Power BI Dashboard development. |

---
*Budget To Brilliance 2D · Built with engineering rigor for UAO Data Engineering & AI.*
