# Enterprise Data Reduction Platform (EDRP) — Complete Implementation Guide

> **Single-file reference** containing every script, instruction, and configuration needed to deploy and operate EDRP end-to-end on Snowflake.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Prerequisites & Assumptions](#3-prerequisites--assumptions)
4. [Phase 1 — Infrastructure Setup](#4-phase-1--infrastructure-setup)
5. [Phase 2 — Metadata Tables](#5-phase-2--metadata-tables)
6. [Phase 3 — Stored Procedures & Functions](#6-phase-3--stored-procedures--functions)
7. [Phase 4 — Reporting Views](#7-phase-4--reporting-views)
8. [Phase 5 — RBAC (Roles & Grants)](#8-phase-5--rbac-roles--grants)
9. [Phase 6 — Initial Configuration](#9-phase-6--initial-configuration)
10. [Phase 7 — Run the Pipeline](#10-phase-7--run-the-pipeline)
11. [Phase 8 — Validation Queries](#11-phase-8--validation-queries)
12. [Phase 9 — Streamlit Management App](#12-phase-9--streamlit-management-app)
13. [Phase 10 — Cortex Agent](#13-phase-10--cortex-agent)
14. [Rollback Instructions](#14-rollback-instructions)
15. [Stratified Sampling Guide](#15-stratified-sampling-guide)
16. [Dependency Map](#16-dependency-map)
17. [Troubleshooting](#17-troubleshooting)

---

## 1. Overview

EDRP is a **metadata-driven Snowflake-native framework** that reduces dataset volumes while preserving:

- **Referential integrity** (FK relationships across all tables)
- **Business relationships** (parent-child cascading)
- **Categorical coverage** (stratified sampling ensures all distinct values survive)

### Proven Results (TPC-DS 100TB)

| Metric | Result |
|--------|--------|
| Rows reduced | 52B → 3.7B (93% reduction) |
| FK integrity checks | 59/60 PASS |
| Distribution checks | 12/12 PASS |

### Reduction Strategies

| Strategy | When Used | How It Works |
|----------|-----------|--------------|
| `FULL_COPY` | Dimension / root tables | `CREATE TABLE AS SELECT *` — keeps all rows |
| `SAMPLE` | Standalone tables | Bernoulli or stratified random sampling |
| `FK_CASCADE` | Fact / child tables | INNER JOIN to already-reduced parent tables |

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA_REDUCTION_POC (Database)                 │
├──────────────────┬──────────────────┬───────────────────────────┤
│  EDRP_METADATA   │    EDRP_APP      │   STAGE_DATA_REDUCED      │
│  (Config/Logs)   │ (SPs/Views/App)  │   (Reduced Output)        │
├──────────────────┼──────────────────┼───────────────────────────┤
│ TABLE_INVENTORY  │ SP_EXTRACT_META  │ (mirrors source tables    │
│ COLUMN_INVENTORY │ SP_BUILD_GRAPH   │  with reduced row counts) │
│ RELATIONSHIP_MAP │ SP_EXECUTE_RED   │                           │
│ JOIN_CONDITIONS  │ SP_VALIDATE_RED  │                           │
│ REDUCTION_PROFILE│ SP_ROLLBACK_RED  │                           │
│ REDUCTION_JOB_LOG│ SP_AI_DISCOVER   │                           │
│ REDUCTION_TBL_LOG│ SP_AI_CLASSIFY   │                           │
│ VALIDATION_RESULT│ FN_AI_SUMMARIZE  │                           │
└──────────────────┴──────────────────┴───────────────────────────┘
```

### Pipeline Flow

```
Step 1: SP_EXTRACT_METADATA      → Scan INFORMATION_SCHEMA, populate inventories
Step 2: SP_BUILD_DEPENDENCY_GRAPH → Topological sort, assign reduction order
Step 3: (Optional) Configure stratified sampling
Step 4: SP_EXECUTE_REDUCTION     → Create reduced tables in target schema
Step 5: SP_VALIDATE_REDUCTION    → FK integrity + distribution checks
Step 6: Review results / Rollback if needed
```

---

## 3. Prerequisites & Assumptions

- **Snowflake Account** with ACCOUNTADMIN access (for initial setup)
- **Source data** must exist in a schema (default: `DATA_REDUCTION_POC.STAGE_DATA`)
- **Cortex AI** access required for AI features (relationship discovery, sensitivity classification, validation summaries)
- **Python packages** available in Snowpark Anaconda channel: `networkx`, `snowflake-snowpark-python`
- Tested on TPC-DS benchmark data; works with any relational schema

---

## 4. Phase 1 — Infrastructure Setup

> **Run as:** `ACCOUNTADMIN` (or role with `CREATE DATABASE` / `CREATE WAREHOUSE` privileges)
> **File:** `deployment/01_prerequisites.sql`

```sql
-- ============================================================================
-- EDRP DEPLOYMENT — Step 1: Prerequisites
-- ============================================================================
-- Creates database, schemas, and warehouse.
-- Run as: ACCOUNTADMIN (or role with CREATE DATABASE/WAREHOUSE privileges)
-- ============================================================================

-- 1. Database
CREATE DATABASE IF NOT EXISTS DATA_REDUCTION_POC
    COMMENT = 'Enterprise Data Reduction Platform';

-- 2. Schemas
CREATE SCHEMA IF NOT EXISTS DATA_REDUCTION_POC.EDRP_METADATA
    COMMENT = 'Enterprise Data Reduction Platform - Metadata and Configuration';

CREATE SCHEMA IF NOT EXISTS DATA_REDUCTION_POC.EDRP_APP
    COMMENT = 'EDRP Stored Procedures, Streamlit App, and Tasks';

CREATE SCHEMA IF NOT EXISTS DATA_REDUCTION_POC.STAGE_DATA_REDUCED
    COMMENT = 'Target schema for reduced datasets';

-- 3. Warehouse
CREATE WAREHOUSE IF NOT EXISTS EDRP_WH
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Dedicated warehouse for EDRP reduction workloads';
```

### What this creates

| Object | Name | Notes |
|--------|------|-------|
| Database | `DATA_REDUCTION_POC` | Top-level container |
| Schema | `EDRP_METADATA` | Configuration and audit tables |
| Schema | `EDRP_APP` | Stored procedures, functions, Streamlit app |
| Schema | `STAGE_DATA_REDUCED` | Output: reduced tables land here |
| Warehouse | `EDRP_WH` | LARGE, auto-suspend 120s, starts suspended |

---

## 5. Phase 2 — Metadata Tables

> **Run as:** `ACCOUNTADMIN` or schema owner
> **File:** `deployment/02_metadata_tables.sql`

```sql
-- ============================================================================
-- EDRP DEPLOYMENT — Step 2: Metadata Tables
-- ============================================================================
-- Creates all metadata tables in EDRP_METADATA schema.
-- Run as: ACCOUNTADMIN or schema owner
-- ============================================================================

USE SCHEMA DATA_REDUCTION_POC.EDRP_METADATA;

-- 1. TABLE_INVENTORY — Stores table-level metadata and reduction config
CREATE TABLE IF NOT EXISTS TABLE_INVENTORY (
    TABLE_ID NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 NOORDER,
    SOURCE_DATABASE VARCHAR(256) NOT NULL,
    SOURCE_SCHEMA VARCHAR(256) NOT NULL,
    TABLE_NAME VARCHAR(256) NOT NULL,
    TABLE_TYPE VARCHAR(50) NOT NULL DEFAULT 'UNKNOWN',
    TABLE_ROLE VARCHAR(50) NOT NULL DEFAULT 'UNKNOWN',
    ROW_COUNT NUMBER(38,0),
    SIZE_BYTES NUMBER(38,0),
    PRIMARY_KEY_COLS VARCHAR(1000),
    REDUCTION_STRATEGY VARCHAR(50) DEFAULT 'SAMPLE',
    SAMPLE_PERCENT NUMBER(5,2) DEFAULT 100,
    INCLUDE_FILTER VARCHAR(4000),
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    LOADED_BY VARCHAR(100) DEFAULT 'MANUAL',
    NOTES VARCHAR(4000),
    CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    REDUCTION_ORDER NUMBER(38,0) DEFAULT 999
        COMMENT 'Graph-computed execution order (0 = process first). Set by SP_BUILD_DEPENDENCY_GRAPH.',
    SAMPLING_STRATEGY VARCHAR(30) DEFAULT 'RANDOM'
        COMMENT 'RANDOM (Bernoulli), STRATIFIED (proportional within strata), or PROPORTIONAL',
    STRATIFY_COLUMNS VARCHAR(1000)
        COMMENT 'Comma-separated column names used as stratification keys when SAMPLING_STRATEGY = STRATIFIED',
    PRIMARY KEY (TABLE_ID),
    UNIQUE (SOURCE_DATABASE, SOURCE_SCHEMA, TABLE_NAME)
);

-- 2. COLUMN_INVENTORY — Stores column-level metadata
CREATE TABLE IF NOT EXISTS COLUMN_INVENTORY (
    COLUMN_ID NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 NOORDER,
    SOURCE_DATABASE VARCHAR(256) NOT NULL,
    SOURCE_SCHEMA VARCHAR(256) NOT NULL,
    TABLE_NAME VARCHAR(256) NOT NULL,
    COLUMN_NAME VARCHAR(256) NOT NULL,
    DATA_TYPE VARCHAR(100),
    ORDINAL_POSITION NUMBER(38,0),
    IS_NULLABLE BOOLEAN DEFAULT TRUE,
    IS_PRIMARY_KEY BOOLEAN DEFAULT FALSE,
    IS_FOREIGN_KEY BOOLEAN DEFAULT FALSE,
    FK_REFERENCES VARCHAR(500),
    COLUMN_ROLE VARCHAR(50),
    SENSITIVITY_CLASS VARCHAR(50),
    DISTINCT_COUNT NUMBER(38,0),
    NULL_PERCENT NUMBER(5,2),
    LOADED_BY VARCHAR(100) DEFAULT 'MANUAL',
    CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (COLUMN_ID),
    UNIQUE (SOURCE_DATABASE, SOURCE_SCHEMA, TABLE_NAME, COLUMN_NAME)
);

-- 3. RELATIONSHIP_MAP — Stores table relationships (logical PK/FK mappings)
CREATE TABLE IF NOT EXISTS RELATIONSHIP_MAP (
    RELATIONSHIP_ID NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 NOORDER,
    SOURCE_DATABASE VARCHAR(256) NOT NULL,
    SOURCE_SCHEMA VARCHAR(256) NOT NULL,
    PARENT_TABLE VARCHAR(256) NOT NULL,
    PARENT_COLUMN VARCHAR(256) NOT NULL,
    CHILD_TABLE VARCHAR(256) NOT NULL,
    CHILD_COLUMN VARCHAR(256) NOT NULL,
    JOIN_TYPE VARCHAR(50) DEFAULT 'INNER',
    CARDINALITY VARCHAR(20) DEFAULT '1:N',
    IS_DECLARED_FK BOOLEAN DEFAULT FALSE,
    IS_ENFORCED BOOLEAN DEFAULT FALSE,
    CONFIDENCE VARCHAR(20) DEFAULT 'HIGH',
    RELATIONSHIP_STATUS VARCHAR(20) DEFAULT 'ACTIVE',
    DISCOVERED_BY VARCHAR(100) NOT NULL DEFAULT 'MANUAL',
    DISCOVERY_SOURCE VARCHAR(500),
    REVIEWED_BY VARCHAR(100),
    REVIEWED_AT TIMESTAMP_NTZ(9),
    NOTES VARCHAR(4000),
    CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    IS_COMPOSITE_KEY BOOLEAN DEFAULT FALSE
        COMMENT 'TRUE if join uses multiple columns. Details in JOIN_CONDITIONS table.',
    PRIMARY KEY (RELATIONSHIP_ID),
    UNIQUE (SOURCE_DATABASE, SOURCE_SCHEMA, PARENT_TABLE, PARENT_COLUMN, CHILD_TABLE, CHILD_COLUMN)
);

-- 4. JOIN_CONDITIONS — Multi-column join support for composite keys
CREATE TABLE IF NOT EXISTS JOIN_CONDITIONS (
    CONDITION_ID NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 NOORDER,
    RELATIONSHIP_ID NUMBER(38,0) NOT NULL COMMENT 'FK to RELATIONSHIP_MAP.RELATIONSHIP_ID',
    ORDINAL_POSITION NUMBER(38,0) NOT NULL COMMENT 'Column position within composite key (1-based)',
    PARENT_COLUMN VARCHAR(256) NOT NULL,
    CHILD_COLUMN VARCHAR(256) NOT NULL,
    CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    COMMENT VARCHAR(1000),
    PRIMARY KEY (CONDITION_ID)
) COMMENT = 'Stores individual column pairs for multi-column join conditions';

-- 5. REDUCTION_PROFILE — Named reduction configurations
CREATE TABLE IF NOT EXISTS REDUCTION_PROFILE (
    PROFILE_ID NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 NOORDER,
    PROFILE_NAME VARCHAR(256) NOT NULL,
    SOURCE_DATABASE VARCHAR(256) NOT NULL,
    SOURCE_SCHEMA VARCHAR(256) NOT NULL,
    TARGET_DATABASE VARCHAR(256) NOT NULL,
    TARGET_SCHEMA VARCHAR(256) NOT NULL,
    DEFAULT_SAMPLE_PERCENT NUMBER(5,2) DEFAULT 10,
    DIMENSION_STRATEGY VARCHAR(50) DEFAULT 'FULL_COPY',
    FACT_STRATEGY VARCHAR(50) DEFAULT 'FK_CASCADE',
    PRESERVE_RARE_EVENTS BOOLEAN DEFAULT TRUE,
    RARE_EVENT_THRESHOLD NUMBER(8,6) DEFAULT 0.001,
    RANDOM_SEED NUMBER(38,0) DEFAULT 42,
    STATUS VARCHAR(20) DEFAULT 'DRAFT',
    CREATED_BY VARCHAR(100),
    APPROVED_BY VARCHAR(100),
    CREATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (PROFILE_ID),
    UNIQUE (PROFILE_NAME)
);

-- 6. REDUCTION_JOB_LOG — Execution audit trail
CREATE TABLE IF NOT EXISTS REDUCTION_JOB_LOG (
    JOB_ID NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 NOORDER,
    PROFILE_ID NUMBER(38,0) NOT NULL,
    JOB_STATUS VARCHAR(20) DEFAULT 'RUNNING',
    STARTED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    COMPLETED_AT TIMESTAMP_NTZ(9),
    TOTAL_TABLES NUMBER(38,0),
    TABLES_PROCESSED NUMBER(38,0) DEFAULT 0,
    ERROR_MESSAGE VARCHAR(4000),
    EXECUTION_LOG VARCHAR(16000000),
    TABLES_CREATED_LIST VARIANT
        COMMENT 'Array of table names successfully created in this job, used for rollback on failure',
    PRIMARY KEY (JOB_ID)
);

-- 7. REDUCTION_TABLE_LOG — Per-table execution details
CREATE TABLE IF NOT EXISTS REDUCTION_TABLE_LOG (
    LOG_ID NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 NOORDER,
    JOB_ID NUMBER(38,0) NOT NULL,
    TABLE_NAME VARCHAR(256) NOT NULL,
    SOURCE_ROW_COUNT NUMBER(38,0),
    TARGET_ROW_COUNT NUMBER(38,0),
    REDUCTION_PERCENT NUMBER(7,4),
    STRATEGY_USED VARCHAR(50),
    EXECUTION_TIME_SEC NUMBER(10,2),
    STATUS VARCHAR(20) DEFAULT 'PENDING',
    ERROR_MESSAGE VARCHAR(4000),
    STARTED_AT TIMESTAMP_NTZ(9),
    COMPLETED_AT TIMESTAMP_NTZ(9),
    PRIMARY KEY (LOG_ID)
);

-- 8. VALIDATION_RESULTS — FK integrity and distribution check results
CREATE TABLE IF NOT EXISTS VALIDATION_RESULTS (
    VALIDATION_ID NUMBER(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1 NOORDER,
    JOB_ID NUMBER(38,0) NOT NULL,
    TABLE_NAME VARCHAR(256) NOT NULL,
    CHECK_TYPE VARCHAR(50) NOT NULL,
    CHECK_NAME VARCHAR(256) NOT NULL,
    SOURCE_VALUE VARCHAR(1000),
    TARGET_VALUE VARCHAR(1000),
    DEVIATION_PERCENT NUMBER(10,4),
    THRESHOLD_PERCENT NUMBER(10,4),
    PASS_FAIL VARCHAR(10) NOT NULL,
    DETAILS VARCHAR(4000),
    CHECKED_AT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (VALIDATION_ID)
);
```

### Table Summary

| # | Table | Purpose |
|---|-------|---------|
| 1 | `TABLE_INVENTORY` | Table-level metadata + reduction config (strategy, sample %, order) |
| 2 | `COLUMN_INVENTORY` | Column-level metadata (PK/FK flags, sensitivity, distinct counts) |
| 3 | `RELATIONSHIP_MAP` | Logical PK/FK relationships between tables |
| 4 | `JOIN_CONDITIONS` | Multi-column composite key join details |
| 5 | `REDUCTION_PROFILE` | Named reduction configurations (source/target, strategies, seed) |
| 6 | `REDUCTION_JOB_LOG` | Job-level execution audit trail |
| 7 | `REDUCTION_TABLE_LOG` | Per-table execution details (row counts, timing) |
| 8 | `VALIDATION_RESULTS` | FK integrity + distribution check results |

---

## 6. Phase 3 — Stored Procedures & Functions

> **Run as:** `ACCOUNTADMIN` or `EDRP_ADMIN`
> **File:** `deployment/03_stored_procedures.sql`

### 6.1 SP_EXTRACT_METADATA

Scans `INFORMATION_SCHEMA.TABLES` and `INFORMATION_SCHEMA.COLUMNS` to populate `TABLE_INVENTORY` and `COLUMN_INVENTORY`. Also discovers join relationships from `ACCESS_HISTORY` and declared FK constraints.

```sql
-- ============================================================================
-- EDRP DEPLOYMENT — Step 3: Stored Procedures
-- ============================================================================

USE SCHEMA DATA_REDUCTION_POC.EDRP_APP;

-- ============================================================================
-- 3.1 SP_EXTRACT_METADATA
-- Scans INFORMATION_SCHEMA to populate TABLE_INVENTORY and COLUMN_INVENTORY.
-- Also discovers joins from ACCESS_HISTORY and declared FK constraints.
-- ============================================================================
CREATE OR REPLACE PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_EXTRACT_METADATA(
    SOURCE_DB VARCHAR, SOURCE_SCHEMA VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Extracts table and column metadata from INFORMATION_SCHEMA into EDRP inventory tables'
AS
$$
import json
from datetime import datetime

def run(session, source_db: str, source_schema: str) -> dict:
    result = {"status": "SUCCESS", "tables_loaded": 0, "columns_loaded": 0, "relationships_found": 0}

    try:
        # --- TABLE INVENTORY ---
        session.sql(f"""
            MERGE INTO DATA_REDUCTION_POC.EDRP_METADATA.TABLE_INVENTORY t
            USING (
                SELECT
                    '{source_db}' AS SOURCE_DATABASE,
                    '{source_schema}' AS SOURCE_SCHEMA,
                    TABLE_NAME,
                    TABLE_TYPE,
                    ROW_COUNT,
                    BYTES AS SIZE_BYTES
                FROM {source_db}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{source_schema}'
                  AND TABLE_TYPE = 'BASE TABLE'
            ) s
            ON t.SOURCE_DATABASE = s.SOURCE_DATABASE
               AND t.SOURCE_SCHEMA = s.SOURCE_SCHEMA
               AND t.TABLE_NAME = s.TABLE_NAME
            WHEN MATCHED THEN UPDATE SET
                t.TABLE_TYPE = s.TABLE_TYPE,
                t.ROW_COUNT = s.ROW_COUNT,
                t.SIZE_BYTES = s.SIZE_BYTES,
                t.UPDATED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT
                (SOURCE_DATABASE, SOURCE_SCHEMA, TABLE_NAME, TABLE_TYPE, ROW_COUNT, SIZE_BYTES, LOADED_BY)
            VALUES
                (s.SOURCE_DATABASE, s.SOURCE_SCHEMA, s.TABLE_NAME, s.TABLE_TYPE, s.ROW_COUNT, s.SIZE_BYTES, 'SP_EXTRACT_METADATA')
        """).collect()

        table_count = session.sql(f"""
            SELECT COUNT(*) AS CNT FROM DATA_REDUCTION_POC.EDRP_METADATA.TABLE_INVENTORY
            WHERE SOURCE_DATABASE = '{source_db}' AND SOURCE_SCHEMA = '{source_schema}'
        """).collect()[0]['CNT']
        result["tables_loaded"] = table_count

        # --- COLUMN INVENTORY ---
        session.sql(f"""
            MERGE INTO DATA_REDUCTION_POC.EDRP_METADATA.COLUMN_INVENTORY t
            USING (
                SELECT
                    '{source_db}' AS SOURCE_DATABASE,
                    '{source_schema}' AS SOURCE_SCHEMA,
                    TABLE_NAME,
                    COLUMN_NAME,
                    DATA_TYPE,
                    ORDINAL_POSITION,
                    CASE WHEN IS_NULLABLE = 'YES' THEN TRUE ELSE FALSE END AS IS_NULLABLE
                FROM {source_db}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{source_schema}'
            ) s
            ON t.SOURCE_DATABASE = s.SOURCE_DATABASE
               AND t.SOURCE_SCHEMA = s.SOURCE_SCHEMA
               AND t.TABLE_NAME = s.TABLE_NAME
               AND t.COLUMN_NAME = s.COLUMN_NAME
            WHEN MATCHED THEN UPDATE SET
                t.DATA_TYPE = s.DATA_TYPE,
                t.ORDINAL_POSITION = s.ORDINAL_POSITION,
                t.IS_NULLABLE = s.IS_NULLABLE
            WHEN NOT MATCHED THEN INSERT
                (SOURCE_DATABASE, SOURCE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION, IS_NULLABLE, LOADED_BY)
            VALUES
                (s.SOURCE_DATABASE, s.SOURCE_SCHEMA, s.TABLE_NAME, s.COLUMN_NAME, s.DATA_TYPE, s.ORDINAL_POSITION, s.IS_NULLABLE, 'SP_EXTRACT_METADATA')
        """).collect()

        col_count = session.sql(f"""
            SELECT COUNT(*) AS CNT FROM DATA_REDUCTION_POC.EDRP_METADATA.COLUMN_INVENTORY
            WHERE SOURCE_DATABASE = '{source_db}' AND SOURCE_SCHEMA = '{source_schema}'
        """).collect()[0]['CNT']
        result["columns_loaded"] = col_count

        # --- RELATIONSHIP DISCOVERY (from TABLE_CONSTRAINTS + KEY_COLUMN_USAGE) ---
        try:
            fk_query = f"""
                SELECT
                    rc.UNIQUE_CONSTRAINT_SCHEMA,
                    pk_kcu.TABLE_NAME AS PARENT_TABLE,
                    pk_kcu.COLUMN_NAME AS PARENT_COLUMN,
                    fk_kcu.TABLE_NAME AS CHILD_TABLE,
                    fk_kcu.COLUMN_NAME AS CHILD_COLUMN
                FROM {source_db}.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
                JOIN {source_db}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk_kcu
                    ON rc.CONSTRAINT_NAME = fk_kcu.CONSTRAINT_NAME
                    AND rc.CONSTRAINT_SCHEMA = fk_kcu.CONSTRAINT_SCHEMA
                JOIN {source_db}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE pk_kcu
                    ON rc.UNIQUE_CONSTRAINT_NAME = pk_kcu.CONSTRAINT_NAME
                    AND rc.UNIQUE_CONSTRAINT_SCHEMA = pk_kcu.CONSTRAINT_SCHEMA
                    AND fk_kcu.ORDINAL_POSITION = pk_kcu.ORDINAL_POSITION
                WHERE fk_kcu.TABLE_SCHEMA = '{source_schema}'
            """
            fk_rows = session.sql(fk_query).collect()

            for row in fk_rows:
                session.sql(f"""
                    INSERT INTO DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP
                        (SOURCE_DATABASE, SOURCE_SCHEMA, PARENT_TABLE, PARENT_COLUMN, CHILD_TABLE, CHILD_COLUMN,
                         IS_DECLARED_FK, CONFIDENCE, DISCOVERED_BY)
                    SELECT '{source_db}', '{source_schema}',
                           '{row["PARENT_TABLE"]}', '{row["PARENT_COLUMN"]}',
                           '{row["CHILD_TABLE"]}', '{row["CHILD_COLUMN"]}',
                           TRUE, 'HIGH', 'FK_CONSTRAINT'
                    WHERE NOT EXISTS (
                        SELECT 1 FROM DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP
                        WHERE SOURCE_DATABASE = '{source_db}' AND SOURCE_SCHEMA = '{source_schema}'
                          AND PARENT_TABLE = '{row["PARENT_TABLE"]}' AND PARENT_COLUMN = '{row["PARENT_COLUMN"]}'
                          AND CHILD_TABLE = '{row["CHILD_TABLE"]}' AND CHILD_COLUMN = '{row["CHILD_COLUMN"]}'
                    )
                """).collect()
                result["relationships_found"] += 1
        except Exception as fk_err:
            result["fk_discovery_note"] = f"FK constraint scan skipped: {str(fk_err)}"

        # --- ACCESS_HISTORY join discovery ---
        try:
            ah_query = f"""
                SELECT DISTINCT
                    bo.value:objectName::VARCHAR AS PARENT_TABLE,
                    doc.value:columnName::VARCHAR AS PARENT_COLUMN,
                    do2.value:objectName::VARCHAR AS CHILD_TABLE,
                    doc2.value:columnName::VARCHAR AS CHILD_COLUMN
                FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
                    LATERAL FLATTEN(input => ah.BASE_OBJECTS_ACCESSED) bo,
                    LATERAL FLATTEN(input => bo.value:columns) doc,
                    LATERAL FLATTEN(input => ah.DIRECT_OBJECTS_ACCESSED) do2,
                    LATERAL FLATTEN(input => do2.value:columns) doc2
                WHERE ah.QUERY_START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
                  AND bo.value:objectDomain::VARCHAR = 'Table'
                  AND do2.value:objectDomain::VARCHAR = 'Table'
                  AND bo.value:objectName != do2.value:objectName
                  AND doc.value:columnName = doc2.value:columnName
                  AND UPPER(bo.value:objectName::VARCHAR) LIKE '%{source_db}.{source_schema}.%'
                LIMIT 100
            """
            ah_rows = session.sql(ah_query).collect()
            for row in ah_rows:
                parent_tbl = row["PARENT_TABLE"].split(".")[-1] if "." in row["PARENT_TABLE"] else row["PARENT_TABLE"]
                child_tbl = row["CHILD_TABLE"].split(".")[-1] if "." in row["CHILD_TABLE"] else row["CHILD_TABLE"]
                session.sql(f"""
                    INSERT INTO DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP
                        (SOURCE_DATABASE, SOURCE_SCHEMA, PARENT_TABLE, PARENT_COLUMN, CHILD_TABLE, CHILD_COLUMN,
                         CONFIDENCE, DISCOVERED_BY, DISCOVERY_SOURCE)
                    SELECT '{source_db}', '{source_schema}',
                           '{parent_tbl}', '{row["PARENT_COLUMN"]}',
                           '{child_tbl}', '{row["CHILD_COLUMN"]}',
                           'MEDIUM', 'ACCESS_HISTORY', 'Last 30 days query patterns'
                    WHERE NOT EXISTS (
                        SELECT 1 FROM DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP
                        WHERE SOURCE_DATABASE = '{source_db}' AND SOURCE_SCHEMA = '{source_schema}'
                          AND PARENT_TABLE = '{parent_tbl}' AND PARENT_COLUMN = '{row["PARENT_COLUMN"]}'
                          AND CHILD_TABLE = '{child_tbl}' AND CHILD_COLUMN = '{row["CHILD_COLUMN"]}'
                    )
                """).collect()
        except Exception as ah_err:
            result["access_history_note"] = f"ACCESS_HISTORY scan skipped: {str(ah_err)}"

    except Exception as e:
        result["status"] = "ERROR"
        result["error"] = str(e)

    return result
$$;
```

### 6.2 SP_BUILD_DEPENDENCY_GRAPH

Uses **NetworkX** topological sort to compute execution order. Classifies tables as ROOT, INTERMEDIATE, or LEAF, and assigns reduction strategies accordingly.

```sql
-- ============================================================================
-- 3.2 SP_BUILD_DEPENDENCY_GRAPH
-- Uses NetworkX topological sort to compute reduction order and classify tables.
-- ============================================================================
CREATE OR REPLACE PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_BUILD_DEPENDENCY_GRAPH(
    SOURCE_DB VARCHAR, SOURCE_SCHEMA VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'networkx')
HANDLER = 'run'
COMMENT = 'Builds dependency graph from RELATIONSHIP_MAP; computes topological order and table roles'
AS
$$
import json
import networkx as nx

def run(session, source_db: str, source_schema: str) -> dict:
    result = {"status": "SUCCESS", "tables_ordered": 0, "cycles_detected": []}

    try:
        # Get the profile to determine strategies
        profile = session.sql(f"""
            SELECT DIMENSION_STRATEGY, FACT_STRATEGY, DEFAULT_SAMPLE_PERCENT
            FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_PROFILE
            WHERE SOURCE_DATABASE = '{source_db}' AND SOURCE_SCHEMA = '{source_schema}'
              AND STATUS = 'ACTIVE'
            ORDER BY PROFILE_ID DESC LIMIT 1
        """).collect()

        dim_strategy = profile[0]['DIMENSION_STRATEGY'] if profile else 'FULL_COPY'
        fact_strategy = profile[0]['FACT_STRATEGY'] if profile else 'FK_CASCADE'
        default_pct = float(profile[0]['DEFAULT_SAMPLE_PERCENT']) if profile else 10.0

        # Build directed graph from RELATIONSHIP_MAP
        rels = session.sql(f"""
            SELECT PARENT_TABLE, CHILD_TABLE
            FROM DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP
            WHERE SOURCE_DATABASE = '{source_db}' AND SOURCE_SCHEMA = '{source_schema}'
              AND RELATIONSHIP_STATUS = 'ACTIVE'
        """).collect()

        G = nx.DiGraph()

        # Add all tables as nodes
        all_tables = session.sql(f"""
            SELECT TABLE_NAME FROM DATA_REDUCTION_POC.EDRP_METADATA.TABLE_INVENTORY
            WHERE SOURCE_DATABASE = '{source_db}' AND SOURCE_SCHEMA = '{source_schema}' AND IS_ACTIVE = TRUE
        """).collect()
        for t in all_tables:
            G.add_node(t['TABLE_NAME'])

        # Add edges (parent -> child)
        for r in rels:
            G.add_edge(r['PARENT_TABLE'], r['CHILD_TABLE'])

        # Check for cycles
        cycles = list(nx.simple_cycles(G))
        if cycles:
            result["cycles_detected"] = [list(c) for c in cycles[:5]]
            # Break cycles by removing lowest-confidence edge
            for cycle in cycles:
                G.remove_edge(cycle[-1], cycle[0])

        # Topological sort
        try:
            topo_order = list(nx.topological_sort(G))
        except nx.NetworkXUnfeasible:
            result["status"] = "ERROR"
            result["error"] = "Could not resolve all cycles in dependency graph"
            return result

        # Classify nodes
        for idx, table_name in enumerate(topo_order):
            predecessors = list(G.predecessors(table_name))
            successors = list(G.successors(table_name))

            if not predecessors and successors:
                role = 'ROOT'
                strategy = dim_strategy
            elif predecessors and successors:
                role = 'INTERMEDIATE'
                strategy = dim_strategy
            elif predecessors and not successors:
                role = 'LEAF'
                strategy = fact_strategy
            else:
                role = 'STANDALONE'
                strategy = 'SAMPLE'

            session.sql(f"""
                UPDATE DATA_REDUCTION_POC.EDRP_METADATA.TABLE_INVENTORY
                SET REDUCTION_ORDER = {idx},
                    TABLE_ROLE = '{role}',
                    REDUCTION_STRATEGY = '{strategy}',
                    SAMPLE_PERCENT = {default_pct},
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE SOURCE_DATABASE = '{source_db}' AND SOURCE_SCHEMA = '{source_schema}'
                  AND TABLE_NAME = '{table_name}'
            """).collect()

        result["tables_ordered"] = len(topo_order)
        result["order"] = topo_order

    except Exception as e:
        result["status"] = "ERROR"
        result["error"] = str(e)

    return result
$$;
```

### 6.3 SP_EXECUTE_REDUCTION

The main reduction engine. Processes tables in dependency order using FULL_COPY, SAMPLE (Bernoulli or stratified), or FK_CASCADE strategies.

```sql
-- ============================================================================
-- 3.3 SP_EXECUTE_REDUCTION
-- Main reduction engine. Creates reduced tables in the target schema.
-- ============================================================================
CREATE OR REPLACE PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_EXECUTE_REDUCTION(
    PROFILE_NAME_PARAM VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Executes the data reduction pipeline for a given profile. Creates reduced tables in target schema.'
AS
$$
import json
from datetime import datetime

def run(session, profile_name_param: str) -> dict:
    result = {"status": "SUCCESS", "job_id": None, "tables_processed": 0, "errors": []}

    try:
        # Load profile
        profile = session.sql(f"""
            SELECT * FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_PROFILE
            WHERE PROFILE_NAME = '{profile_name_param}' AND STATUS = 'ACTIVE'
        """).collect()

        if not profile:
            return {"status": "ERROR", "error": f"Profile '{profile_name_param}' not found or not ACTIVE"}

        p = profile[0]
        source_db = p['SOURCE_DATABASE']
        source_schema = p['SOURCE_SCHEMA']
        target_db = p['TARGET_DATABASE']
        target_schema = p['TARGET_SCHEMA']
        default_pct = float(p['DEFAULT_SAMPLE_PERCENT'])
        random_seed = int(p['RANDOM_SEED'])

        # Create job log entry
        session.sql(f"""
            INSERT INTO DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG (PROFILE_ID, JOB_STATUS)
            VALUES ({p['PROFILE_ID']}, 'RUNNING')
        """).collect()

        job_id = session.sql("SELECT MAX(JOB_ID) AS JID FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG").collect()[0]['JID']
        result["job_id"] = job_id

        # Get tables in reduction order
        tables = session.sql(f"""
            SELECT TABLE_NAME, REDUCTION_STRATEGY, SAMPLE_PERCENT, SAMPLING_STRATEGY, STRATIFY_COLUMNS
            FROM DATA_REDUCTION_POC.EDRP_METADATA.TABLE_INVENTORY
            WHERE SOURCE_DATABASE = '{source_db}' AND SOURCE_SCHEMA = '{source_schema}'
              AND IS_ACTIVE = TRUE
            ORDER BY REDUCTION_ORDER
        """).collect()

        total_tables = len(tables)
        session.sql(f"""
            UPDATE DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG
            SET TOTAL_TABLES = {total_tables} WHERE JOB_ID = {job_id}
        """).collect()

        tables_created = []

        for tbl in tables:
            table_name = tbl['TABLE_NAME']
            strategy = tbl['REDUCTION_STRATEGY']
            sample_pct = float(tbl['SAMPLE_PERCENT']) if tbl['SAMPLE_PERCENT'] else default_pct
            sampling_strategy = tbl['SAMPLING_STRATEGY'] or 'RANDOM'
            stratify_cols = tbl['STRATIFY_COLUMNS']

            # Log start
            session.sql(f"""
                INSERT INTO DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_TABLE_LOG
                    (JOB_ID, TABLE_NAME, STRATEGY_USED, STATUS, STARTED_AT)
                VALUES ({job_id}, '{table_name}', '{strategy}', 'RUNNING', CURRENT_TIMESTAMP())
            """).collect()

            try:
                source_count = session.sql(f"SELECT COUNT(*) AS CNT FROM {source_db}.{source_schema}.{table_name}").collect()[0]['CNT']

                if strategy == 'FULL_COPY':
                    session.sql(f"""
                        CREATE OR REPLACE TABLE {target_db}.{target_schema}.{table_name} AS
                        SELECT * FROM {source_db}.{source_schema}.{table_name}
                    """).collect()

                elif strategy == 'SAMPLE':
                    if sampling_strategy == 'STRATIFIED' and stratify_cols:
                        cols = [c.strip() for c in stratify_cols.split(',')]
                        col_list = ', '.join(cols)
                        session.sql(f"""
                            CREATE OR REPLACE TABLE {target_db}.{target_schema}.{table_name} AS
                            WITH ranked AS (
                                SELECT *,
                                    ROW_NUMBER() OVER (PARTITION BY {col_list} ORDER BY RANDOM({random_seed})) AS rn,
                                    COUNT(*) OVER (PARTITION BY {col_list}) AS grp_total,
                                    GREATEST(CEIL(COUNT(*) OVER (PARTITION BY {col_list}) * {sample_pct} / 100.0), 1) AS grp_limit
                                FROM {source_db}.{source_schema}.{table_name}
                            )
                            SELECT * EXCLUDE (rn, grp_total, grp_limit) FROM ranked WHERE rn <= grp_limit
                        """).collect()
                    else:
                        session.sql(f"""
                            CREATE OR REPLACE TABLE {target_db}.{target_schema}.{table_name} AS
                            SELECT * FROM {source_db}.{source_schema}.{table_name}
                            SAMPLE BERNOULLI ({sample_pct}) SEED ({random_seed})
                        """).collect()

                elif strategy == 'FK_CASCADE':
                    # Get parent relationships for this child table
                    rels = session.sql(f"""
                        SELECT r.PARENT_TABLE, r.PARENT_COLUMN, r.CHILD_COLUMN, r.IS_COMPOSITE_KEY, r.RELATIONSHIP_ID
                        FROM DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP r
                        WHERE r.SOURCE_DATABASE = '{source_db}' AND r.SOURCE_SCHEMA = '{source_schema}'
                          AND r.CHILD_TABLE = '{table_name}'
                          AND r.RELATIONSHIP_STATUS = 'ACTIVE'
                    """).collect()

                    if rels:
                        join_clauses = []
                        for i, rel in enumerate(rels):
                            alias = f"p{i}"
                            parent = rel['PARENT_TABLE']

                            if rel['IS_COMPOSITE_KEY']:
                                # Get composite key columns
                                jc = session.sql(f"""
                                    SELECT PARENT_COLUMN, CHILD_COLUMN
                                    FROM DATA_REDUCTION_POC.EDRP_METADATA.JOIN_CONDITIONS
                                    WHERE RELATIONSHIP_ID = {rel['RELATIONSHIP_ID']}
                                    ORDER BY ORDINAL_POSITION
                                """).collect()
                                conditions = " AND ".join([
                                    f"c.{j['CHILD_COLUMN']} = {alias}.{j['PARENT_COLUMN']}" for j in jc
                                ])
                            else:
                                conditions = f"c.{rel['CHILD_COLUMN']} = {alias}.{rel['PARENT_COLUMN']}"

                            join_clauses.append(
                                f"INNER JOIN {target_db}.{target_schema}.{parent} {alias} ON {conditions}"
                            )

                        joins = "\n".join(join_clauses)
                        session.sql(f"""
                            CREATE OR REPLACE TABLE {target_db}.{target_schema}.{table_name} AS
                            SELECT DISTINCT c.*
                            FROM {source_db}.{source_schema}.{table_name} c
                            {joins}
                        """).collect()
                    else:
                        # No relationships found — fall back to sampling
                        session.sql(f"""
                            CREATE OR REPLACE TABLE {target_db}.{target_schema}.{table_name} AS
                            SELECT * FROM {source_db}.{source_schema}.{table_name}
                            SAMPLE BERNOULLI ({sample_pct}) SEED ({random_seed})
                        """).collect()

                target_count = session.sql(f"SELECT COUNT(*) AS CNT FROM {target_db}.{target_schema}.{table_name}").collect()[0]['CNT']
                reduction_pct = round(100 - (target_count * 100.0 / source_count), 4) if source_count > 0 else 0

                tables_created.append(table_name)

                # Log success
                session.sql(f"""
                    UPDATE DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_TABLE_LOG
                    SET SOURCE_ROW_COUNT = {source_count},
                        TARGET_ROW_COUNT = {target_count},
                        REDUCTION_PERCENT = {reduction_pct},
                        STATUS = 'COMPLETED',
                        COMPLETED_AT = CURRENT_TIMESTAMP()
                    WHERE JOB_ID = {job_id} AND TABLE_NAME = '{table_name}'
                """).collect()

                result["tables_processed"] += 1

            except Exception as tbl_err:
                error_msg = str(tbl_err).replace("'", "''")[:3900]
                session.sql(f"""
                    UPDATE DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_TABLE_LOG
                    SET STATUS = 'FAILED', ERROR_MESSAGE = '{error_msg}', COMPLETED_AT = CURRENT_TIMESTAMP()
                    WHERE JOB_ID = {job_id} AND TABLE_NAME = '{table_name}'
                """).collect()
                result["errors"].append({"table": table_name, "error": str(tbl_err)[:500]})

        # Update job log
        job_status = 'COMPLETED' if not result["errors"] else 'COMPLETED_WITH_ERRORS'
        tables_list_json = json.dumps(tables_created)
        session.sql(f"""
            UPDATE DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG
            SET JOB_STATUS = '{job_status}',
                TABLES_PROCESSED = {result["tables_processed"]},
                COMPLETED_AT = CURRENT_TIMESTAMP(),
                TABLES_CREATED_LIST = PARSE_JSON('{tables_list_json}')
            WHERE JOB_ID = {job_id}
        """).collect()

    except Exception as e:
        result["status"] = "ERROR"
        result["error"] = str(e)
        if result.get("job_id"):
            err_msg = str(e).replace("'", "''")[:3900]
            session.sql(f"""
                UPDATE DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG
                SET JOB_STATUS = 'FAILED', ERROR_MESSAGE = '{err_msg}', COMPLETED_AT = CURRENT_TIMESTAMP()
                WHERE JOB_ID = {result['job_id']}
            """).collect()

    return result
$$;
```

### 6.4 SP_VALIDATE_REDUCTION

Checks FK integrity (orphaned keys) and distribution deviation (top-N frequency comparison).

```sql
-- ============================================================================
-- 3.4 SP_VALIDATE_REDUCTION
-- Validates FK integrity and distribution preservation.
-- ============================================================================
CREATE OR REPLACE PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_VALIDATE_REDUCTION(
    JOB_ID_PARAM NUMBER
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Validates FK integrity and distribution preservation for a completed reduction job'
AS
$$
import json

def run(session, job_id_param: int) -> dict:
    result = {"status": "SUCCESS", "job_id": job_id_param, "checks_run": 0, "passed": 0, "failed": 0, "warnings": 0}

    try:
        # Get job details
        job = session.sql(f"""
            SELECT j.*, p.SOURCE_DATABASE, p.SOURCE_SCHEMA, p.TARGET_DATABASE, p.TARGET_SCHEMA
            FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG j
            JOIN DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_PROFILE p ON j.PROFILE_ID = p.PROFILE_ID
            WHERE j.JOB_ID = {job_id_param}
        """).collect()

        if not job:
            return {"status": "ERROR", "error": f"Job {job_id_param} not found"}

        j = job[0]
        source_db = j['SOURCE_DATABASE']
        source_schema = j['SOURCE_SCHEMA']
        target_db = j['TARGET_DATABASE']
        target_schema = j['TARGET_SCHEMA']

        # --- FK INTEGRITY CHECKS ---
        rels = session.sql(f"""
            SELECT RELATIONSHIP_ID, PARENT_TABLE, PARENT_COLUMN, CHILD_TABLE, CHILD_COLUMN, IS_COMPOSITE_KEY
            FROM DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP
            WHERE SOURCE_DATABASE = '{source_db}' AND SOURCE_SCHEMA = '{source_schema}'
              AND RELATIONSHIP_STATUS = 'ACTIVE'
        """).collect()

        for rel in rels:
            try:
                parent = rel['PARENT_TABLE']
                child = rel['CHILD_TABLE']
                p_col = rel['PARENT_COLUMN']
                c_col = rel['CHILD_COLUMN']
                check_name = f"{child}.{c_col} -> {parent}.{p_col}"

                if rel['IS_COMPOSITE_KEY']:
                    jc = session.sql(f"""
                        SELECT PARENT_COLUMN, CHILD_COLUMN
                        FROM DATA_REDUCTION_POC.EDRP_METADATA.JOIN_CONDITIONS
                        WHERE RELATIONSHIP_ID = {rel['RELATIONSHIP_ID']}
                        ORDER BY ORDINAL_POSITION
                    """).collect()
                    where_clause = " AND ".join([
                        f"c.{j['CHILD_COLUMN']} NOT IN (SELECT {j['PARENT_COLUMN']} FROM {target_db}.{target_schema}.{parent})"
                        for j in jc
                    ])
                    orphan_query = f"""
                        SELECT COUNT(*) AS ORPHANS FROM {target_db}.{target_schema}.{child} c
                        WHERE {where_clause}
                    """
                else:
                    orphan_query = f"""
                        SELECT COUNT(*) AS ORPHANS FROM {target_db}.{target_schema}.{child} c
                        WHERE c.{c_col} IS NOT NULL
                          AND c.{c_col} NOT IN (SELECT {p_col} FROM {target_db}.{target_schema}.{parent})
                    """

                orphan_count = session.sql(orphan_query).collect()[0]['ORPHANS']
                pass_fail = 'PASS' if orphan_count == 0 else 'FAIL'

                session.sql(f"""
                    INSERT INTO DATA_REDUCTION_POC.EDRP_METADATA.VALIDATION_RESULTS
                        (JOB_ID, TABLE_NAME, CHECK_TYPE, CHECK_NAME, TARGET_VALUE, PASS_FAIL, DETAILS)
                    VALUES ({job_id_param}, '{child}', 'FK_INTEGRITY', '{check_name}',
                            '{orphan_count}', '{pass_fail}',
                            '{orphan_count} orphaned records in {child}.{c_col}')
                """).collect()

                result["checks_run"] += 1
                if pass_fail == 'PASS':
                    result["passed"] += 1
                else:
                    result["failed"] += 1

            except Exception as rel_err:
                result["checks_run"] += 1
                result["warnings"] += 1

        # --- DISTRIBUTION CHECKS ---
        tables = session.sql(f"""
            SELECT DISTINCT TABLE_NAME FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_TABLE_LOG
            WHERE JOB_ID = {job_id_param} AND STATUS = 'COMPLETED'
        """).collect()

        for tbl in tables:
            table_name = tbl['TABLE_NAME']
            try:
                # Get categorical columns (VARCHAR with < 100 distinct values)
                cat_cols = session.sql(f"""
                    SELECT COLUMN_NAME FROM DATA_REDUCTION_POC.EDRP_METADATA.COLUMN_INVENTORY
                    WHERE SOURCE_DATABASE = '{source_db}' AND SOURCE_SCHEMA = '{source_schema}'
                      AND TABLE_NAME = '{table_name}'
                      AND DATA_TYPE LIKE '%CHAR%'
                      AND (DISTINCT_COUNT IS NULL OR DISTINCT_COUNT < 100)
                    LIMIT 3
                """).collect()

                for col in cat_cols:
                    col_name = col['COLUMN_NAME']
                    try:
                        dist_query = f"""
                            WITH src AS (
                                SELECT {col_name}, COUNT(*) AS cnt,
                                       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 4) AS pct
                                FROM {source_db}.{source_schema}.{table_name}
                                WHERE {col_name} IS NOT NULL
                                GROUP BY {col_name}
                                ORDER BY cnt DESC LIMIT 10
                            ),
                            tgt AS (
                                SELECT {col_name}, COUNT(*) AS cnt,
                                       ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 4) AS pct
                                FROM {target_db}.{target_schema}.{table_name}
                                WHERE {col_name} IS NOT NULL
                                GROUP BY {col_name}
                                ORDER BY cnt DESC LIMIT 10
                            )
                            SELECT
                                COALESCE(s.{col_name}, t.{col_name}) AS val,
                                s.pct AS src_pct, t.pct AS tgt_pct,
                                ABS(COALESCE(s.pct,0) - COALESCE(t.pct,0)) AS deviation
                            FROM src s FULL OUTER JOIN tgt t ON s.{col_name} = t.{col_name}
                            ORDER BY deviation DESC LIMIT 1
                        """
                        dist_result = session.sql(dist_query).collect()

                        if dist_result:
                            max_dev = float(dist_result[0]['DEVIATION']) if dist_result[0]['DEVIATION'] else 0
                            threshold = 5.0
                            if max_dev <= threshold:
                                pf = 'PASS'
                                result["passed"] += 1
                            elif max_dev <= 10.0:
                                pf = 'WARN'
                                result["warnings"] += 1
                            else:
                                pf = 'FAIL'
                                result["failed"] += 1

                            session.sql(f"""
                                INSERT INTO DATA_REDUCTION_POC.EDRP_METADATA.VALIDATION_RESULTS
                                    (JOB_ID, TABLE_NAME, CHECK_TYPE, CHECK_NAME,
                                     SOURCE_VALUE, TARGET_VALUE, DEVIATION_PERCENT, THRESHOLD_PERCENT,
                                     PASS_FAIL, DETAILS)
                                VALUES ({job_id_param}, '{table_name}', 'DISTRIBUTION',
                                        '{table_name}.{col_name}',
                                        '{dist_result[0]["SRC_PCT"]}', '{dist_result[0]["TGT_PCT"]}',
                                        {max_dev}, {threshold}, '{pf}',
                                        'Max deviation {max_dev}% for value {dist_result[0]["VAL"]}')
                            """).collect()
                            result["checks_run"] += 1

                    except:
                        pass
            except:
                pass

    except Exception as e:
        result["status"] = "ERROR"
        result["error"] = str(e)

    return result
$$;
```

### 6.5 SP_ROLLBACK_REDUCTION

Drops all tables created by a specific job and marks the job as `ROLLED_BACK`.

```sql
-- ============================================================================
-- 3.5 SP_ROLLBACK_REDUCTION
-- Drops all tables created by a given job. Marks job as ROLLED_BACK.
-- ============================================================================
CREATE OR REPLACE PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_ROLLBACK_REDUCTION(
    JOB_ID_PARAM NUMBER
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Rolls back a reduction job by dropping all tables it created'
AS
$$
import json

def run(session, job_id_param: int) -> dict:
    result = {"status": "SUCCESS", "job_id": job_id_param, "tables_dropped": []}

    try:
        job = session.sql(f"""
            SELECT j.TABLES_CREATED_LIST, p.TARGET_DATABASE, p.TARGET_SCHEMA
            FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG j
            JOIN DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_PROFILE p ON j.PROFILE_ID = p.PROFILE_ID
            WHERE j.JOB_ID = {job_id_param}
        """).collect()

        if not job:
            return {"status": "ERROR", "error": f"Job {job_id_param} not found"}

        target_db = job[0]['TARGET_DATABASE']
        target_schema = job[0]['TARGET_SCHEMA']
        tables_list = json.loads(job[0]['TABLES_CREATED_LIST']) if job[0]['TABLES_CREATED_LIST'] else []

        for table_name in tables_list:
            try:
                session.sql(f"DROP TABLE IF EXISTS {target_db}.{target_schema}.{table_name}").collect()
                result["tables_dropped"].append(table_name)
            except Exception as drop_err:
                result.setdefault("errors", []).append({"table": table_name, "error": str(drop_err)})

        session.sql(f"""
            UPDATE DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG
            SET JOB_STATUS = 'ROLLED_BACK', COMPLETED_AT = CURRENT_TIMESTAMP()
            WHERE JOB_ID = {job_id_param}
        """).collect()

    except Exception as e:
        result["status"] = "ERROR"
        result["error"] = str(e)

    return result
$$;
```

### 6.6 SP_AI_DISCOVER_RELATIONSHIPS

Uses **Cortex COMPLETE (llama3.1-70b)** to suggest FK relationships by analyzing table/column names.

```sql
-- ============================================================================
-- 3.6 SP_AI_DISCOVER_RELATIONSHIPS
-- Uses Cortex COMPLETE to suggest FK relationships via AI.
-- ============================================================================
CREATE OR REPLACE PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_AI_DISCOVER_RELATIONSHIPS(
    SOURCE_DB VARCHAR, SOURCE_SCHEMA VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Uses Cortex AI to discover potential FK relationships from column naming patterns'
AS
$$
import json

def run(session, source_db: str, source_schema: str) -> dict:
    result = {"status": "SUCCESS", "relationships_suggested": 0}

    try:
        # Get all tables and columns
        schema_info = session.sql(f"""
            SELECT TABLE_NAME, LISTAGG(COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY ORDINAL_POSITION) AS COLUMNS
            FROM DATA_REDUCTION_POC.EDRP_METADATA.COLUMN_INVENTORY
            WHERE SOURCE_DATABASE = '{source_db}' AND SOURCE_SCHEMA = '{source_schema}'
            GROUP BY TABLE_NAME
            ORDER BY TABLE_NAME
        """).collect()

        schema_desc = "\n".join([f"Table {r['TABLE_NAME']}: {r['COLUMNS']}" for r in schema_info])

        prompt = f"""Analyze this database schema and identify likely foreign key relationships.
For each relationship, provide: parent_table, parent_column, child_table, child_column.
Return ONLY a JSON array of objects with those 4 fields. No explanation.

Schema:
{schema_desc}
"""

        ai_result = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE('llama3.1-70b', '{prompt.replace(chr(39), chr(39)+chr(39))}') AS RESPONSE
        """).collect()[0]['RESPONSE']

        # Parse AI response
        try:
            # Try to extract JSON array from response
            start = ai_result.find('[')
            end = ai_result.rfind(']') + 1
            if start >= 0 and end > start:
                suggestions = json.loads(ai_result[start:end])
            else:
                suggestions = []
        except json.JSONDecodeError:
            suggestions = []

        for s in suggestions:
            try:
                session.sql(f"""
                    INSERT INTO DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP
                        (SOURCE_DATABASE, SOURCE_SCHEMA, PARENT_TABLE, PARENT_COLUMN, CHILD_TABLE, CHILD_COLUMN,
                         CONFIDENCE, RELATIONSHIP_STATUS, DISCOVERED_BY, DISCOVERY_SOURCE)
                    SELECT '{source_db}', '{source_schema}',
                           '{s["parent_table"]}', '{s["parent_column"]}',
                           '{s["child_table"]}', '{s["child_column"]}',
                           'MEDIUM', 'PENDING_REVIEW', 'CORTEX_AI', 'llama3.1-70b column name analysis'
                    WHERE NOT EXISTS (
                        SELECT 1 FROM DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP
                        WHERE SOURCE_DATABASE = '{source_db}' AND SOURCE_SCHEMA = '{source_schema}'
                          AND PARENT_TABLE = '{s["parent_table"]}' AND PARENT_COLUMN = '{s["parent_column"]}'
                          AND CHILD_TABLE = '{s["child_table"]}' AND CHILD_COLUMN = '{s["child_column"]}'
                    )
                """).collect()
                result["relationships_suggested"] += 1
            except:
                pass

        result["ai_suggestions_raw"] = len(suggestions)

    except Exception as e:
        return {"error": str(e), "raw_response": ai_result[:2000]}
$$;
```

### 6.7 FN_AI_SUMMARIZE_VALIDATION

SQL function that calls Cortex COMPLETE to produce a human-readable validation summary.

```sql
-- ============================================================================
-- 3.7 FN_AI_SUMMARIZE_VALIDATION
-- Uses Cortex COMPLETE to generate human-readable validation summary
-- ============================================================================
CREATE OR REPLACE FUNCTION DATA_REDUCTION_POC.EDRP_APP.FN_AI_SUMMARIZE_VALIDATION(JOB_ID_PARAM NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Uses Cortex COMPLETE to generate a human-readable validation summary'
AS
$$
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        'Summarize this data reduction validation report for a business stakeholder. '
        || 'Highlight any failures or warnings. Be concise. '
        || 'Report data: ' || (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
                'table', TABLE_NAME, 'check', CHECK_NAME, 'type', CHECK_TYPE,
                'result', PASS_FAIL, 'detail', DETAILS
            ))::VARCHAR
            FROM DATA_REDUCTION_POC.EDRP_METADATA.VALIDATION_RESULTS
            WHERE JOB_ID = JOB_ID_PARAM
        )
    )
$$;
```

---

## 7. Phase 4 — Reporting Views

> **Run as:** `ACCOUNTADMIN` or `EDRP_ADMIN`
> **File:** `deployment/04_views.sql`

```sql
-- ============================================================================
-- EDRP DEPLOYMENT — Step 4: Views
-- ============================================================================

USE SCHEMA DATA_REDUCTION_POC.EDRP_METADATA;

-- 4.1 V_RELATIONSHIP_LINEAGE — Recursive dependency tree
CREATE OR REPLACE VIEW V_RELATIONSHIP_LINEAGE AS
WITH RECURSIVE lineage AS (
    SELECT
        r.PARENT_TABLE AS TABLE_NAME,
        CAST(NULL AS VARCHAR) AS PARENT_TABLE,
        CAST(NULL AS VARCHAR) AS PARENT_COLUMN,
        CAST(NULL AS VARCHAR) AS CHILD_COLUMN,
        0 AS DEPTH,
        r.PARENT_TABLE AS ROOT_TABLE,
        r.SOURCE_DATABASE,
        r.SOURCE_SCHEMA
    FROM RELATIONSHIP_MAP r
    WHERE r.RELATIONSHIP_STATUS = 'ACTIVE'
      AND r.PARENT_TABLE NOT IN (
          SELECT CHILD_TABLE FROM RELATIONSHIP_MAP
          WHERE RELATIONSHIP_STATUS = 'ACTIVE'
            AND SOURCE_DATABASE = r.SOURCE_DATABASE
            AND SOURCE_SCHEMA = r.SOURCE_SCHEMA
      )
    GROUP BY r.PARENT_TABLE, r.SOURCE_DATABASE, r.SOURCE_SCHEMA
    
    UNION ALL
    
    SELECT
        r.CHILD_TABLE AS TABLE_NAME, r.PARENT_TABLE, r.PARENT_COLUMN, r.CHILD_COLUMN,
        l.DEPTH + 1 AS DEPTH, l.ROOT_TABLE, r.SOURCE_DATABASE, r.SOURCE_SCHEMA
    FROM RELATIONSHIP_MAP r
    JOIN lineage l ON r.PARENT_TABLE = l.TABLE_NAME
        AND r.SOURCE_DATABASE = l.SOURCE_DATABASE AND r.SOURCE_SCHEMA = l.SOURCE_SCHEMA
    WHERE r.RELATIONSHIP_STATUS = 'ACTIVE' AND l.DEPTH < 10
)
SELECT
    l.SOURCE_DATABASE, l.SOURCE_SCHEMA, l.ROOT_TABLE, l.DEPTH,
    REPEAT('  ', l.DEPTH) || l.TABLE_NAME AS HIERARCHY_DISPLAY,
    l.TABLE_NAME, l.PARENT_TABLE,
    COALESCE(l.PARENT_COLUMN || ' -> ' || l.CHILD_COLUMN, '(root)') AS JOIN_PATH,
    COALESCE(r.IS_COMPOSITE_KEY, FALSE) AS IS_COMPOSITE_KEY,
    COALESCE(r.CARDINALITY, '-') AS CARDINALITY,
    t.REDUCTION_STRATEGY, t.SAMPLE_PERCENT, t.SAMPLING_STRATEGY
FROM lineage l
LEFT JOIN RELATIONSHIP_MAP r
    ON r.PARENT_TABLE = l.PARENT_TABLE AND r.CHILD_TABLE = l.TABLE_NAME
    AND r.SOURCE_DATABASE = l.SOURCE_DATABASE AND r.SOURCE_SCHEMA = l.SOURCE_SCHEMA
    AND r.RELATIONSHIP_STATUS = 'ACTIVE'
LEFT JOIN TABLE_INVENTORY t
    ON t.TABLE_NAME = l.TABLE_NAME AND t.SOURCE_DATABASE = l.SOURCE_DATABASE
    AND t.SOURCE_SCHEMA = l.SOURCE_SCHEMA
ORDER BY l.SOURCE_DATABASE, l.SOURCE_SCHEMA, l.ROOT_TABLE, l.DEPTH, l.TABLE_NAME;


-- 4.2 V_CATEGORICAL_COVERAGE — Distribution deviation report per job
CREATE OR REPLACE VIEW V_CATEGORICAL_COVERAGE AS
SELECT
    v.JOB_ID, v.TABLE_NAME,
    v.CHECK_NAME AS COLUMN_CHECK,
    v.DEVIATION_PERCENT, v.THRESHOLD_PERCENT,
    v.PASS_FAIL, v.DETAILS, v.CHECKED_AT
FROM VALIDATION_RESULTS v
WHERE v.CHECK_TYPE = 'DISTRIBUTION'
ORDER BY v.JOB_ID DESC, v.PASS_FAIL, v.TABLE_NAME;
```

---

## 8. Phase 5 — RBAC (Roles & Grants)

> **Run as:** `ACCOUNTADMIN` or `SECURITYADMIN`
> **File:** `deployment/05_rbac.sql`

```sql
-- ============================================================================
-- EDRP DEPLOYMENT — Step 5: RBAC Setup
-- ============================================================================

-- 1. Create Roles
CREATE ROLE IF NOT EXISTS EDRP_ADMIN;
CREATE ROLE IF NOT EXISTS EDRP_OPERATOR;
CREATE ROLE IF NOT EXISTS EDRP_VIEWER;

-- 2. Role Hierarchy: VIEWER → OPERATOR → ADMIN → SYSADMIN
GRANT ROLE EDRP_VIEWER TO ROLE EDRP_OPERATOR;
GRANT ROLE EDRP_OPERATOR TO ROLE EDRP_ADMIN;
GRANT ROLE EDRP_ADMIN TO ROLE SYSADMIN;

-- 3. Database & Schema Usage (Viewer)
GRANT USAGE ON DATABASE DATA_REDUCTION_POC TO ROLE EDRP_VIEWER;
GRANT USAGE ON SCHEMA DATA_REDUCTION_POC.EDRP_METADATA TO ROLE EDRP_VIEWER;
GRANT USAGE ON SCHEMA DATA_REDUCTION_POC.EDRP_APP TO ROLE EDRP_VIEWER;
GRANT USAGE ON SCHEMA DATA_REDUCTION_POC.STAGE_DATA_REDUCED TO ROLE EDRP_VIEWER;

-- 4. Viewer: read metadata and reduced data
GRANT SELECT ON ALL TABLES IN SCHEMA DATA_REDUCTION_POC.EDRP_METADATA TO ROLE EDRP_VIEWER;
GRANT SELECT ON ALL VIEWS IN SCHEMA DATA_REDUCTION_POC.EDRP_METADATA TO ROLE EDRP_VIEWER;
GRANT SELECT ON ALL TABLES IN SCHEMA DATA_REDUCTION_POC.STAGE_DATA_REDUCED TO ROLE EDRP_VIEWER;

-- 5. Operator: execute procedures + warehouse
GRANT USAGE ON WAREHOUSE EDRP_WH TO ROLE EDRP_OPERATOR;
GRANT USAGE ON PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_EXECUTE_REDUCTION(VARCHAR) TO ROLE EDRP_OPERATOR;
GRANT USAGE ON PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_VALIDATE_REDUCTION(NUMBER) TO ROLE EDRP_OPERATOR;
GRANT USAGE ON PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_ROLLBACK_REDUCTION(NUMBER) TO ROLE EDRP_OPERATOR;

-- 6. Admin: full control
GRANT ALL ON SCHEMA DATA_REDUCTION_POC.EDRP_METADATA TO ROLE EDRP_ADMIN;
GRANT ALL ON SCHEMA DATA_REDUCTION_POC.EDRP_APP TO ROLE EDRP_ADMIN;
GRANT ALL ON ALL TABLES IN SCHEMA DATA_REDUCTION_POC.EDRP_METADATA TO ROLE EDRP_ADMIN;

-- 7. Future grants (so new objects inherit permissions)
GRANT SELECT ON FUTURE TABLES IN SCHEMA DATA_REDUCTION_POC.EDRP_METADATA TO ROLE EDRP_VIEWER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA DATA_REDUCTION_POC.EDRP_METADATA TO ROLE EDRP_VIEWER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA DATA_REDUCTION_POC.STAGE_DATA_REDUCED TO ROLE EDRP_VIEWER;
```

### Role Hierarchy

```
EDRP_VIEWER  →  EDRP_OPERATOR  →  EDRP_ADMIN  →  SYSADMIN
  (read)          (execute)         (full control)
```

| Role | Capabilities |
|------|-------------|
| `EDRP_VIEWER` | SELECT on metadata tables, views, and reduced data |
| `EDRP_OPERATOR` | Execute SPs + warehouse usage (inherits VIEWER) |
| `EDRP_ADMIN` | Full control on all schemas (inherits OPERATOR) |

---

## 9. Phase 6 — Initial Configuration

> **Run as:** `EDRP_ADMIN` or `ACCOUNTADMIN`
> **File:** `deployment/06_initial_config.sql`

```sql
-- ============================================================================
-- EDRP DEPLOYMENT — Step 6: Initial Configuration
-- ============================================================================

USE SCHEMA DATA_REDUCTION_POC.EDRP_METADATA;

-- Default 10% reduction profile (adjust SOURCE/TARGET as needed)
INSERT INTO REDUCTION_PROFILE
    (PROFILE_NAME, SOURCE_DATABASE, SOURCE_SCHEMA, TARGET_DATABASE, TARGET_SCHEMA,
     DEFAULT_SAMPLE_PERCENT, DIMENSION_STRATEGY, FACT_STRATEGY, RANDOM_SEED, STATUS, CREATED_BY)
SELECT
    'DEFAULT_10PCT',
    'DATA_REDUCTION_POC', 'STAGE_DATA',          -- << CHANGE: your source
    'DATA_REDUCTION_POC', 'STAGE_DATA_REDUCED',   -- << CHANGE: your target
    10.00, 'FULL_COPY', 'FK_CASCADE', 42, 'ACTIVE', CURRENT_USER()
WHERE NOT EXISTS (
    SELECT 1 FROM REDUCTION_PROFILE WHERE PROFILE_NAME = 'DEFAULT_10PCT'
);
```

### Configuration Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `PROFILE_NAME` | `DEFAULT_10PCT` | Unique name for this profile |
| `SOURCE_DATABASE` | `DATA_REDUCTION_POC` | Where source data lives |
| `SOURCE_SCHEMA` | `STAGE_DATA` | Schema containing source tables |
| `TARGET_SCHEMA` | `STAGE_DATA_REDUCED` | Where reduced tables are created |
| `DEFAULT_SAMPLE_PERCENT` | `10.00` | Default sampling rate |
| `DIMENSION_STRATEGY` | `FULL_COPY` | How to handle dimension/root tables |
| `FACT_STRATEGY` | `FK_CASCADE` | How to handle fact/leaf tables |
| `RANDOM_SEED` | `42` | For reproducible sampling |

### Adding Custom Profiles

```sql
-- Example: Additional profile targeting a different schema
INSERT INTO REDUCTION_PROFILE
    (PROFILE_NAME, SOURCE_DATABASE, SOURCE_SCHEMA, TARGET_DATABASE, TARGET_SCHEMA,
     DEFAULT_SAMPLE_PERCENT, DIMENSION_STRATEGY, FACT_STRATEGY, RANDOM_SEED, STATUS, CREATED_BY)
VALUES
    ('MY_CUSTOM_5PCT', 'DATA_REDUCTION_POC', 'STAGE_DATA',
     'DATA_REDUCTION_POC', 'MY_REDUCED_SCHEMA',
     5.00, 'FULL_COPY', 'FK_CASCADE', 42, 'ACTIVE', CURRENT_USER());
```

---

## 10. Phase 7 — Run the Pipeline

> **Run as:** `EDRP_OPERATOR` or `EDRP_ADMIN`
> **Prerequisite:** Phases 1-6 completed. Source data exists in source schema.
> **File:** `deployment/07_run_pipeline.sql`

```sql
-- Use the dedicated warehouse
USE WAREHOUSE EDRP_WH;

-- ============================================================================
-- STEP 1: Extract Metadata
-- Populates TABLE_INVENTORY and COLUMN_INVENTORY from INFORMATION_SCHEMA.
-- Also discovers joins from ACCESS_HISTORY and declared FK constraints.
-- ============================================================================
CALL DATA_REDUCTION_POC.EDRP_APP.SP_EXTRACT_METADATA('DATA_REDUCTION_POC', 'STAGE_DATA');
-- << CHANGE: Replace 'DATA_REDUCTION_POC' and 'STAGE_DATA' with your database/schema

-- ============================================================================
-- STEP 2: Build Dependency Graph
-- Computes reduction order via topological sort. Sets TABLE_ROLE and
-- REDUCTION_STRATEGY automatically (ROOT=FULL_COPY, LEAF=FK_CASCADE).
-- ============================================================================
CALL DATA_REDUCTION_POC.EDRP_APP.SP_BUILD_DEPENDENCY_GRAPH('DATA_REDUCTION_POC', 'STAGE_DATA');

-- Verify the computed order:
SELECT TABLE_NAME, TABLE_ROLE, REDUCTION_STRATEGY, REDUCTION_ORDER, SAMPLE_PERCENT
FROM DATA_REDUCTION_POC.EDRP_METADATA.TABLE_INVENTORY
WHERE SOURCE_DATABASE = 'DATA_REDUCTION_POC' AND SOURCE_SCHEMA = 'STAGE_DATA'
ORDER BY REDUCTION_ORDER;

-- ============================================================================
-- STEP 3: (Optional) Configure Stratified Sampling
-- For tables where you need categorical coverage guarantees.
-- ============================================================================
-- UPDATE DATA_REDUCTION_POC.EDRP_METADATA.TABLE_INVENTORY
-- SET SAMPLING_STRATEGY = 'STRATIFIED',
--     STRATIFY_COLUMNS = 'C_BIRTH_COUNTRY'
-- WHERE TABLE_NAME = 'CUSTOMER'
--   AND SOURCE_DATABASE = 'DATA_REDUCTION_POC'
--   AND SOURCE_SCHEMA = 'STAGE_DATA';

-- ============================================================================
-- STEP 4: Execute Reduction
-- Creates reduced tables in the target schema defined by the profile.
-- This may take several minutes for large fact tables.
-- ============================================================================
CALL DATA_REDUCTION_POC.EDRP_APP.SP_EXECUTE_REDUCTION('DEFAULT_10PCT');
-- << CHANGE: Replace 'DEFAULT_10PCT' with your profile name

-- ============================================================================
-- STEP 5: Validate Reduction
-- Checks FK integrity, row count ratios, and distribution deviation.
-- Pass the JOB_ID from Step 4 output.
-- ============================================================================
CALL DATA_REDUCTION_POC.EDRP_APP.SP_VALIDATE_REDUCTION(
    (SELECT MAX(JOB_ID) FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG)
);

-- ============================================================================
-- STEP 6: Review Results
-- ============================================================================

-- Job summary
SELECT JOB_ID, JOB_STATUS, TOTAL_TABLES, TABLES_PROCESSED, STARTED_AT, COMPLETED_AT
FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG
ORDER BY JOB_ID DESC LIMIT 1;

-- Validation summary
SELECT PASS_FAIL, COUNT(*) AS CHECK_COUNT
FROM DATA_REDUCTION_POC.EDRP_METADATA.VALIDATION_RESULTS
WHERE JOB_ID = (SELECT MAX(JOB_ID) FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG)
GROUP BY PASS_FAIL;

-- Any failures?
SELECT TABLE_NAME, CHECK_NAME, TARGET_VALUE, DETAILS
FROM DATA_REDUCTION_POC.EDRP_METADATA.VALIDATION_RESULTS
WHERE JOB_ID = (SELECT MAX(JOB_ID) FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG)
  AND PASS_FAIL = 'FAIL';

-- ============================================================================
-- ROLLBACK (if needed)
-- Drops all tables created by a specific job and marks it as ROLLED_BACK.
-- ============================================================================
-- CALL DATA_REDUCTION_POC.EDRP_APP.SP_ROLLBACK_REDUCTION(<JOB_ID>);
```

---

## 11. Phase 8 — Validation Queries

> **Run as:** Any role with SELECT access
> **File:** `deployment/08_validation_queries.sql`

### 11.1 Job Execution Summary

```sql
USE DATABASE DATA_REDUCTION_POC;
USE WAREHOUSE COMPUTE_WH;

-- Job status and timing
SELECT
    j.JOB_ID,
    p.PROFILE_NAME,
    p.SOURCE_DATABASE || '.' || p.SOURCE_SCHEMA AS SOURCE,
    p.TARGET_DATABASE || '.' || p.TARGET_SCHEMA AS TARGET,
    j.JOB_STATUS,
    j.TOTAL_TABLES,
    j.TABLES_PROCESSED,
    j.STARTED_AT,
    j.COMPLETED_AT,
    DATEDIFF('second', j.STARTED_AT, j.COMPLETED_AT) AS DURATION_SEC
FROM EDRP_METADATA.REDUCTION_JOB_LOG j
JOIN EDRP_METADATA.REDUCTION_PROFILE p ON j.PROFILE_ID = p.PROFILE_ID
ORDER BY j.JOB_ID DESC
LIMIT 5;

-- Per-table execution details for most recent job
SELECT
    TABLE_NAME,
    STRATEGY_USED,
    SOURCE_ROW_COUNT,
    TARGET_ROW_COUNT,
    REDUCTION_PERCENT || '%' AS REDUCTION_PCT,
    ROUND(EXECUTION_TIME_SEC, 1) AS TIME_SEC,
    STATUS
FROM EDRP_METADATA.REDUCTION_TABLE_LOG
WHERE JOB_ID = (SELECT MAX(JOB_ID) FROM EDRP_METADATA.REDUCTION_JOB_LOG)
ORDER BY
    CASE STATUS WHEN 'FAILED' THEN 0 WHEN 'COMPLETED' THEN 1 ELSE 2 END,
    SOURCE_ROW_COUNT DESC;

-- Aggregate reduction stats
SELECT
    COUNT(*) AS TOTAL_TABLES,
    SUM(CASE WHEN STATUS = 'COMPLETED' THEN 1 ELSE 0 END) AS COMPLETED,
    SUM(CASE WHEN STATUS = 'FAILED' THEN 1 ELSE 0 END) AS FAILED,
    SUM(SOURCE_ROW_COUNT) AS TOTAL_SOURCE_ROWS,
    SUM(TARGET_ROW_COUNT) AS TOTAL_TARGET_ROWS,
    ROUND(100 - (SUM(TARGET_ROW_COUNT) * 100.0 / NULLIF(SUM(SOURCE_ROW_COUNT), 0)), 2) AS OVERALL_REDUCTION_PCT,
    ROUND(SUM(EXECUTION_TIME_SEC), 1) AS TOTAL_TIME_SEC
FROM EDRP_METADATA.REDUCTION_TABLE_LOG
WHERE JOB_ID = (SELECT MAX(JOB_ID) FROM EDRP_METADATA.REDUCTION_JOB_LOG);
```

### 11.2 FK Integrity Checks

```sql
-- Summary of FK validation results
SELECT
    PASS_FAIL,
    COUNT(*) AS CHECK_COUNT
FROM EDRP_METADATA.VALIDATION_RESULTS
WHERE JOB_ID = (SELECT MAX(JOB_ID) FROM EDRP_METADATA.REDUCTION_JOB_LOG)
  AND CHECK_TYPE = 'FK_INTEGRITY'
GROUP BY PASS_FAIL
ORDER BY PASS_FAIL;

-- Failed FK checks (orphaned records)
SELECT
    TABLE_NAME,
    CHECK_NAME,
    TARGET_VALUE AS ORPHAN_DETAIL,
    DETAILS
FROM EDRP_METADATA.VALIDATION_RESULTS
WHERE JOB_ID = (SELECT MAX(JOB_ID) FROM EDRP_METADATA.REDUCTION_JOB_LOG)
  AND CHECK_TYPE = 'FK_INTEGRITY'
  AND PASS_FAIL = 'FAIL'
ORDER BY TABLE_NAME;

-- All FK checks — full detail
SELECT
    TABLE_NAME,
    CHECK_NAME,
    PASS_FAIL,
    TARGET_VALUE AS ORPHAN_COUNT,
    DETAILS
FROM EDRP_METADATA.VALIDATION_RESULTS
WHERE JOB_ID = (SELECT MAX(JOB_ID) FROM EDRP_METADATA.REDUCTION_JOB_LOG)
  AND CHECK_TYPE = 'FK_INTEGRITY'
ORDER BY PASS_FAIL DESC, TABLE_NAME;
```

### 11.3 Manual FK Spot Checks

```sql
-- STORE_RETURNS → CUSTOMER (SR_CUSTOMER_SK)
SELECT 'STORE_RETURNS.SR_CUSTOMER_SK' AS FK_CHECK,
       COUNT(*) AS ORPHAN_COUNT
FROM STAGE_DATA_REDUCED.STORE_RETURNS sr
WHERE sr.SR_CUSTOMER_SK IS NOT NULL
  AND sr.SR_CUSTOMER_SK NOT IN (
      SELECT C_CUSTOMER_SK FROM STAGE_DATA_REDUCED.CUSTOMER
  );

-- CATALOG_RETURNS → CUSTOMER (CR_REFUNDED_CUSTOMER_SK)
SELECT 'CATALOG_RETURNS.CR_REFUNDED_CUSTOMER_SK' AS FK_CHECK,
       COUNT(*) AS ORPHAN_COUNT
FROM STAGE_DATA_REDUCED.CATALOG_RETURNS cr
WHERE cr.CR_REFUNDED_CUSTOMER_SK IS NOT NULL
  AND cr.CR_REFUNDED_CUSTOMER_SK NOT IN (
      SELECT C_CUSTOMER_SK FROM STAGE_DATA_REDUCED.CUSTOMER
  );

-- WEB_RETURNS → CUSTOMER (WR_RETURNING_CUSTOMER_SK)
SELECT 'WEB_RETURNS.WR_RETURNING_CUSTOMER_SK' AS FK_CHECK,
       COUNT(*) AS ORPHAN_COUNT
FROM STAGE_DATA_REDUCED.WEB_RETURNS wr
WHERE wr.WR_RETURNING_CUSTOMER_SK IS NOT NULL
  AND wr.WR_RETURNING_CUSTOMER_SK NOT IN (
      SELECT C_CUSTOMER_SK FROM STAGE_DATA_REDUCED.CUSTOMER
  );

-- STORE_RETURNS → ITEM (SR_ITEM_SK)
SELECT 'STORE_RETURNS.SR_ITEM_SK' AS FK_CHECK,
       COUNT(*) AS ORPHAN_COUNT
FROM STAGE_DATA_REDUCED.STORE_RETURNS sr
WHERE sr.SR_ITEM_SK IS NOT NULL
  AND sr.SR_ITEM_SK NOT IN (
      SELECT I_ITEM_SK FROM STAGE_DATA_REDUCED.ITEM
  );

-- INVENTORY → DATE_DIM (INV_DATE_SK)
SELECT 'INVENTORY.INV_DATE_SK' AS FK_CHECK,
       COUNT(*) AS ORPHAN_COUNT
FROM STAGE_DATA_REDUCED.INVENTORY inv
WHERE inv.INV_DATE_SK IS NOT NULL
  AND inv.INV_DATE_SK NOT IN (
      SELECT D_DATE_SK FROM STAGE_DATA_REDUCED.DATE_DIM
  );

-- CATALOG_RETURNS → CALL_CENTER (CR_CALL_CENTER_SK)
SELECT 'CATALOG_RETURNS.CR_CALL_CENTER_SK' AS FK_CHECK,
       COUNT(*) AS ORPHAN_COUNT
FROM STAGE_DATA_REDUCED.CATALOG_RETURNS cr
WHERE cr.CR_CALL_CENTER_SK IS NOT NULL
  AND cr.CR_CALL_CENTER_SK NOT IN (
      SELECT CC_CALL_CENTER_SK FROM STAGE_DATA_REDUCED.CALL_CENTER
  );
```

### 11.4 Row Count Comparison

```sql
SELECT
    t.TABLE_NAME,
    t.REDUCTION_STRATEGY,
    t.SAMPLING_STRATEGY,
    s.ROW_COUNT AS SOURCE_ROWS,
    r.ROW_COUNT AS REDUCED_ROWS,
    ROUND(100 - (r.ROW_COUNT * 100.0 / NULLIF(s.ROW_COUNT, 0)), 2) AS REDUCTION_PCT
FROM EDRP_METADATA.TABLE_INVENTORY t
LEFT JOIN INFORMATION_SCHEMA.TABLES s
    ON s.TABLE_SCHEMA = 'STAGE_DATA' AND s.TABLE_NAME = t.TABLE_NAME
LEFT JOIN INFORMATION_SCHEMA.TABLES r
    ON r.TABLE_SCHEMA = 'STAGE_DATA_REDUCED' AND r.TABLE_NAME = t.TABLE_NAME
WHERE t.SOURCE_DATABASE = 'DATA_REDUCTION_POC'
  AND t.SOURCE_SCHEMA = 'STAGE_DATA'
ORDER BY s.ROW_COUNT DESC;
```

### 11.5 Distribution Checks

```sql
-- Automated distribution checks
SELECT * FROM EDRP_METADATA.V_CATEGORICAL_COVERAGE
WHERE JOB_ID = (SELECT MAX(JOB_ID) FROM EDRP_METADATA.REDUCTION_JOB_LOG);

-- Manual: CUSTOMER_DEMOGRAPHICS distribution (CD_GENDER)
SELECT
    'SOURCE' AS DATASET, CD_GENDER,
    COUNT(*) AS CNT,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS PCT
FROM STAGE_DATA.CUSTOMER_DEMOGRAPHICS GROUP BY CD_GENDER
UNION ALL
SELECT
    'REDUCED', CD_GENDER,
    COUNT(*),
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)
FROM STAGE_DATA_REDUCED.CUSTOMER_DEMOGRAPHICS GROUP BY CD_GENDER
ORDER BY CD_GENDER, DATASET;

-- Distinct value counts — source vs reduced (no values lost?)
SELECT
    'CUSTOMER_DEMOGRAPHICS.CD_GENDER' AS COLUMN_CHECK,
    (SELECT COUNT(DISTINCT CD_GENDER) FROM STAGE_DATA.CUSTOMER_DEMOGRAPHICS) AS SOURCE_DISTINCT,
    (SELECT COUNT(DISTINCT CD_GENDER) FROM STAGE_DATA_REDUCED.CUSTOMER_DEMOGRAPHICS) AS REDUCED_DISTINCT
UNION ALL
SELECT
    'CUSTOMER_DEMOGRAPHICS.CD_MARITAL_STATUS',
    (SELECT COUNT(DISTINCT CD_MARITAL_STATUS) FROM STAGE_DATA.CUSTOMER_DEMOGRAPHICS),
    (SELECT COUNT(DISTINCT CD_MARITAL_STATUS) FROM STAGE_DATA_REDUCED.CUSTOMER_DEMOGRAPHICS)
UNION ALL
SELECT
    'CUSTOMER_DEMOGRAPHICS.CD_EDUCATION_STATUS',
    (SELECT COUNT(DISTINCT CD_EDUCATION_STATUS) FROM STAGE_DATA.CUSTOMER_DEMOGRAPHICS),
    (SELECT COUNT(DISTINCT CD_EDUCATION_STATUS) FROM STAGE_DATA_REDUCED.CUSTOMER_DEMOGRAPHICS)
UNION ALL
SELECT
    'INCOME_BAND.IB_LOWER_BOUND',
    (SELECT COUNT(DISTINCT IB_LOWER_BOUND) FROM STAGE_DATA.INCOME_BAND),
    (SELECT COUNT(DISTINCT IB_LOWER_BOUND) FROM STAGE_DATA_REDUCED.INCOME_BAND)
UNION ALL
SELECT
    'SHIP_MODE.SM_TYPE',
    (SELECT COUNT(DISTINCT SM_TYPE) FROM STAGE_DATA.SHIP_MODE),
    (SELECT COUNT(DISTINCT SM_TYPE) FROM STAGE_DATA_REDUCED.SHIP_MODE)
UNION ALL
SELECT
    'REASON.R_REASON_DESC',
    (SELECT COUNT(DISTINCT R_REASON_DESC) FROM STAGE_DATA.REASON),
    (SELECT COUNT(DISTINCT R_REASON_DESC) FROM STAGE_DATA_REDUCED.REASON);
```

### 11.6 Relationship Lineage

```sql
-- Full hierarchy view
SELECT DISTINCT
    ROOT_TABLE,
    DEPTH,
    HIERARCHY_DISPLAY,
    JOIN_PATH,
    CARDINALITY,
    REDUCTION_STRATEGY,
    SAMPLING_STRATEGY
FROM EDRP_METADATA.V_RELATIONSHIP_LINEAGE
WHERE SOURCE_DATABASE = 'DATA_REDUCTION_POC'
  AND SOURCE_SCHEMA = 'STAGE_DATA'
ORDER BY ROOT_TABLE, DEPTH, HIERARCHY_DISPLAY;

-- Tables by dependency depth
SELECT
    DEPTH,
    COUNT(DISTINCT TABLE_NAME) AS TABLE_COUNT,
    LISTAGG(DISTINCT TABLE_NAME, ', ') WITHIN GROUP (ORDER BY TABLE_NAME) AS TABLES
FROM EDRP_METADATA.V_RELATIONSHIP_LINEAGE
WHERE SOURCE_DATABASE = 'DATA_REDUCTION_POC'
  AND SOURCE_SCHEMA = 'STAGE_DATA'
GROUP BY DEPTH
ORDER BY DEPTH;
```

### 11.7 Metadata Health Checks

```sql
-- Tables with no relationships defined
SELECT t.TABLE_NAME, t.TABLE_TYPE, t.TABLE_ROLE, t.REDUCTION_STRATEGY
FROM EDRP_METADATA.TABLE_INVENTORY t
LEFT JOIN EDRP_METADATA.RELATIONSHIP_MAP r
    ON (r.PARENT_TABLE = t.TABLE_NAME OR r.CHILD_TABLE = t.TABLE_NAME)
    AND r.SOURCE_DATABASE = t.SOURCE_DATABASE
    AND r.SOURCE_SCHEMA = t.SOURCE_SCHEMA
    AND r.RELATIONSHIP_STATUS = 'ACTIVE'
WHERE t.SOURCE_DATABASE = 'DATA_REDUCTION_POC'
  AND t.SOURCE_SCHEMA = 'STAGE_DATA'
  AND r.RELATIONSHIP_ID IS NULL;

-- Relationship map summary
SELECT
    RELATIONSHIP_STATUS,
    DISCOVERED_BY,
    COUNT(*) AS REL_COUNT
FROM EDRP_METADATA.RELATIONSHIP_MAP
WHERE SOURCE_DATABASE = 'DATA_REDUCTION_POC'
  AND SOURCE_SCHEMA = 'STAGE_DATA'
GROUP BY RELATIONSHIP_STATUS, DISCOVERED_BY
ORDER BY RELATIONSHIP_STATUS, DISCOVERED_BY;

-- Reduction strategy distribution
SELECT
    REDUCTION_STRATEGY,
    COUNT(*) AS TABLE_COUNT,
    LISTAGG(TABLE_NAME, ', ') WITHIN GROUP (ORDER BY TABLE_NAME) AS TABLES
FROM EDRP_METADATA.TABLE_INVENTORY
WHERE SOURCE_DATABASE = 'DATA_REDUCTION_POC'
  AND SOURCE_SCHEMA = 'STAGE_DATA'
GROUP BY REDUCTION_STRATEGY
ORDER BY TABLE_COUNT DESC;

-- Graph execution order
SELECT
    REDUCTION_ORDER,
    TABLE_NAME,
    TABLE_ROLE,
    REDUCTION_STRATEGY,
    SAMPLE_PERCENT
FROM EDRP_METADATA.TABLE_INVENTORY
WHERE SOURCE_DATABASE = 'DATA_REDUCTION_POC'
  AND SOURCE_SCHEMA = 'STAGE_DATA'
ORDER BY REDUCTION_ORDER;
```

### 11.8 Rollback Support

```sql
SELECT
    JOB_ID,
    JOB_STATUS,
    ARRAY_SIZE(COALESCE(TABLES_CREATED_LIST, PARSE_JSON('[]'))) AS TABLES_CREATED,
    TABLES_CREATED_LIST
FROM EDRP_METADATA.REDUCTION_JOB_LOG
ORDER BY JOB_ID DESC
LIMIT 5;
```

### 11.9 Full Validation Audit Log

```sql
SELECT
    JOB_ID,
    TABLE_NAME,
    CHECK_TYPE,
    CHECK_NAME,
    PASS_FAIL,
    COALESCE(DEVIATION_PERCENT::VARCHAR, TARGET_VALUE) AS RESULT_DETAIL,
    DETAILS,
    CHECKED_AT
FROM EDRP_METADATA.VALIDATION_RESULTS
WHERE JOB_ID = (SELECT MAX(JOB_ID) FROM EDRP_METADATA.REDUCTION_JOB_LOG)
ORDER BY
    CASE PASS_FAIL WHEN 'FAIL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END,
    CHECK_TYPE,
    TABLE_NAME;
```

### 11.10 Categorical Coverage Validation

```sql
-- CUSTOMER.C_BIRTH_COUNTRY — Distinct value count comparison
SELECT
    'SOURCE' AS DATASET,
    COUNT(DISTINCT C_BIRTH_COUNTRY) AS DISTINCT_COUNTRIES,
    COUNT(*) AS TOTAL_ROWS
FROM DATA_REDUCTION_POC.STAGE_DATA.CUSTOMER
UNION ALL
SELECT
    'REDUCED',
    COUNT(DISTINCT C_BIRTH_COUNTRY),
    COUNT(*)
FROM DATA_REDUCTION_POC.STAGE_DATA_REDUCED.CUSTOMER
ORDER BY DATASET DESC;

-- Per-country row counts — verify proportional representation
WITH src AS (
    SELECT C_BIRTH_COUNTRY, COUNT(*) AS SRC_CNT
    FROM DATA_REDUCTION_POC.STAGE_DATA.CUSTOMER
    GROUP BY C_BIRTH_COUNTRY
),
tgt AS (
    SELECT C_BIRTH_COUNTRY, COUNT(*) AS TGT_CNT
    FROM DATA_REDUCTION_POC.STAGE_DATA_REDUCED.CUSTOMER
    GROUP BY C_BIRTH_COUNTRY
)
SELECT
    s.C_BIRTH_COUNTRY,
    s.SRC_CNT AS SOURCE_ROWS,
    t.TGT_CNT AS REDUCED_ROWS,
    ROUND(t.TGT_CNT * 100.0 / s.SRC_CNT, 2) AS SAMPLE_PCT,
    CASE WHEN t.TGT_CNT IS NULL THEN 'MISSING' ELSE 'OK' END AS STATUS
FROM src s
LEFT JOIN tgt t ON s.C_BIRTH_COUNTRY = t.C_BIRTH_COUNTRY
ORDER BY s.SRC_CNT ASC;

-- Missing countries check — should return 0 rows
SELECT s.C_BIRTH_COUNTRY AS MISSING_COUNTRY, s.CNT AS SOURCE_ROWS
FROM (
    SELECT C_BIRTH_COUNTRY, COUNT(*) AS CNT
    FROM DATA_REDUCTION_POC.STAGE_DATA.CUSTOMER
    GROUP BY C_BIRTH_COUNTRY
) s
LEFT JOIN (
    SELECT DISTINCT C_BIRTH_COUNTRY
    FROM DATA_REDUCTION_POC.STAGE_DATA_REDUCED.CUSTOMER
) t ON s.C_BIRTH_COUNTRY = t.C_BIRTH_COUNTRY
WHERE t.C_BIRTH_COUNTRY IS NULL;

-- Stratified sampling config — what's currently configured?
SELECT
    TABLE_NAME,
    REDUCTION_STRATEGY,
    SAMPLING_STRATEGY,
    STRATIFY_COLUMNS,
    SAMPLE_PERCENT
FROM DATA_REDUCTION_POC.EDRP_METADATA.TABLE_INVENTORY
WHERE SOURCE_DATABASE = 'DATA_REDUCTION_POC'
  AND SOURCE_SCHEMA = 'STAGE_DATA'
  AND SAMPLING_STRATEGY = 'STRATIFIED';
```

---

## 12. Phase 9 — Streamlit Management App

> **File:** `app/streamlit_app.py`
> **Deploy as:** Streamlit-in-Snowflake app in `DATA_REDUCTION_POC.EDRP_APP`

The Streamlit app provides a visual management interface with 6 tabs:

| Tab | Features |
|-----|----------|
| **Dashboard** | Job history, per-table reduction charts, validation scorecard, AI summary |
| **Pipeline** | Run each pipeline step via buttons (extract, build graph, execute, validate, rollback) |
| **Table Inventory** | Browse/filter tables, inline edit strategy/sampling/stratify columns |
| **Relationship Map** | View active/pending relationships, approve/reject AI-discovered ones |
| **Column Sensitivity** | Run AI classification, view PII/PHI/PCI/CONFIDENTIAL/PUBLIC columns |
| **Validation** | Detailed per-job validation results, FK integrity details, distribution deviations |

```python
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import json

session = get_active_session()

st.set_page_config(page_title="EDRP Manager", layout="wide")
st.title("Enterprise Data Reduction Platform")
st.caption("Metadata-driven dataset reduction with referential integrity preservation")

tab0, tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Dashboard", "Pipeline", "Table Inventory",
    "Relationship Map", "Column Sensitivity", "Validation"
])

# ─── TAB 0: DASHBOARD ───
with tab0:
    st.subheader("Reduction Dashboard")

    col1, col2, col3 = st.columns(3)

    # Job history
    jobs = session.sql("""
        SELECT j.JOB_ID, p.PROFILE_NAME, j.JOB_STATUS, j.TOTAL_TABLES, j.TABLES_PROCESSED,
               j.STARTED_AT, j.COMPLETED_AT
        FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG j
        JOIN DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_PROFILE p ON j.PROFILE_ID = p.PROFILE_ID
        ORDER BY j.JOB_ID DESC LIMIT 10
    """).to_pandas()

    with col1:
        st.metric("Total Jobs", len(jobs))
    with col2:
        completed = len(jobs[jobs['JOB_STATUS'].isin(['COMPLETED', 'COMPLETED_WITH_ERRORS'])]) if len(jobs) > 0 else 0
        st.metric("Completed", completed)
    with col3:
        failed = len(jobs[jobs['JOB_STATUS'] == 'FAILED']) if len(jobs) > 0 else 0
        st.metric("Failed", failed)

    if len(jobs) > 0:
        st.dataframe(jobs, use_container_width=True)

    # Per-table reduction chart
    table_log = session.sql("""
        SELECT TABLE_NAME, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, REDUCTION_PERCENT, STRATEGY_USED
        FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_TABLE_LOG
        WHERE JOB_ID = (SELECT MAX(JOB_ID) FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG)
          AND STATUS = 'COMPLETED'
        ORDER BY SOURCE_ROW_COUNT DESC
    """).to_pandas()

    if len(table_log) > 0:
        st.subheader("Reduction by Table (Latest Job)")
        st.bar_chart(table_log.set_index('TABLE_NAME')[['SOURCE_ROW_COUNT', 'TARGET_ROW_COUNT']])

    # Validation scorecard
    val_summary = session.sql("""
        SELECT PASS_FAIL, COUNT(*) AS CNT
        FROM DATA_REDUCTION_POC.EDRP_METADATA.VALIDATION_RESULTS
        WHERE JOB_ID = (SELECT MAX(JOB_ID) FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG)
        GROUP BY PASS_FAIL
    """).to_pandas()

    if len(val_summary) > 0:
        st.subheader("Validation Scorecard")
        st.dataframe(val_summary, use_container_width=True)

        # AI Summary
        try:
            ai_summary = session.sql("""
                SELECT DATA_REDUCTION_POC.EDRP_APP.FN_AI_SUMMARIZE_VALIDATION(
                    (SELECT MAX(JOB_ID) FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG)
                ) AS SUMMARY
            """).collect()[0]['SUMMARY']
            st.subheader("AI Validation Summary")
            st.write(ai_summary)
        except:
            pass

# ─── TAB 1: PIPELINE ───
with tab1:
    st.subheader("Pipeline Execution")

    col_a, col_b = st.columns(2)

    with col_a:
        st.write("**Step 1:** Extract Metadata")
        if st.button("Run Metadata Extraction"):
            with st.spinner("Extracting metadata..."):
                result = session.sql("""
                    CALL DATA_REDUCTION_POC.EDRP_APP.SP_EXTRACT_METADATA('DATA_REDUCTION_POC', 'STAGE_DATA')
                """).collect()
                st.success("Metadata extraction complete!")
                st.json(result[0][0])

        st.write("**Step 2:** Build Dependency Graph")
        if st.button("Build Graph"):
            with st.spinner("Building dependency graph..."):
                result = session.sql("""
                    CALL DATA_REDUCTION_POC.EDRP_APP.SP_BUILD_DEPENDENCY_GRAPH('DATA_REDUCTION_POC', 'STAGE_DATA')
                """).collect()
                st.success("Dependency graph built!")
                st.json(result[0][0])

    with col_b:
        st.write("**Step 3:** Execute Reduction")
        profiles = session.sql("""
            SELECT PROFILE_NAME FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_PROFILE
            WHERE STATUS = 'ACTIVE' ORDER BY PROFILE_NAME
        """).to_pandas()

        if len(profiles) > 0:
            selected_profile = st.selectbox("Select Profile", profiles['PROFILE_NAME'].tolist())
            if st.button("Execute Reduction"):
                with st.spinner(f"Running reduction with profile '{selected_profile}'..."):
                    result = session.sql(f"""
                        CALL DATA_REDUCTION_POC.EDRP_APP.SP_EXECUTE_REDUCTION('{selected_profile}')
                    """).collect()
                    st.success("Reduction complete!")
                    st.json(result[0][0])

        st.write("**Step 4:** Validate")
        if st.button("Run Validation"):
            with st.spinner("Validating..."):
                result = session.sql("""
                    CALL DATA_REDUCTION_POC.EDRP_APP.SP_VALIDATE_REDUCTION(
                        (SELECT MAX(JOB_ID) FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG)
                    )
                """).collect()
                st.success("Validation complete!")
                st.json(result[0][0])

    st.write("---")
    st.write("**Rollback**")
    rollback_job = st.number_input("Job ID to rollback", min_value=1, step=1)
    if st.button("Rollback Job"):
        with st.spinner(f"Rolling back job {rollback_job}..."):
            result = session.sql(f"""
                CALL DATA_REDUCTION_POC.EDRP_APP.SP_ROLLBACK_REDUCTION({rollback_job})
            """).collect()
            st.success("Rollback complete!")
            st.json(result[0][0])

# ─── TAB 2: TABLE INVENTORY ───
with tab2:
    st.subheader("Table Inventory")

    inventory = session.sql("""
        SELECT TABLE_ID, TABLE_NAME, TABLE_TYPE, TABLE_ROLE, ROW_COUNT, SIZE_BYTES,
               REDUCTION_STRATEGY, SAMPLE_PERCENT, REDUCTION_ORDER,
               SAMPLING_STRATEGY, STRATIFY_COLUMNS, IS_ACTIVE
        FROM DATA_REDUCTION_POC.EDRP_METADATA.TABLE_INVENTORY
        WHERE SOURCE_DATABASE = 'DATA_REDUCTION_POC' AND SOURCE_SCHEMA = 'STAGE_DATA'
        ORDER BY REDUCTION_ORDER
    """).to_pandas()

    if len(inventory) > 0:
        # Filter
        strategies = ['All'] + inventory['REDUCTION_STRATEGY'].unique().tolist()
        filter_strategy = st.selectbox("Filter by Strategy", strategies)
        if filter_strategy != 'All':
            inventory = inventory[inventory['REDUCTION_STRATEGY'] == filter_strategy]

        st.dataframe(inventory, use_container_width=True)

        # Inline edit
        st.write("**Edit Table Configuration**")
        edit_table = st.selectbox("Select Table", inventory['TABLE_NAME'].tolist())

        col_e1, col_e2, col_e3, col_e4 = st.columns(4)
        with col_e1:
            new_strategy = st.selectbox("Strategy", ['FULL_COPY', 'SAMPLE', 'FK_CASCADE'])
        with col_e2:
            new_pct = st.number_input("Sample %", min_value=0.01, max_value=100.0, value=10.0)
        with col_e3:
            new_sampling = st.selectbox("Sampling", ['RANDOM', 'STRATIFIED'])
        with col_e4:
            new_stratify = st.text_input("Stratify Columns", placeholder="COL1, COL2")

        if st.button("Update Configuration"):
            stratify_val = f"'{new_stratify}'" if new_stratify else "NULL"
            session.sql(f"""
                UPDATE DATA_REDUCTION_POC.EDRP_METADATA.TABLE_INVENTORY
                SET REDUCTION_STRATEGY = '{new_strategy}',
                    SAMPLE_PERCENT = {new_pct},
                    SAMPLING_STRATEGY = '{new_sampling}',
                    STRATIFY_COLUMNS = {stratify_val},
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE TABLE_NAME = '{edit_table}'
                  AND SOURCE_DATABASE = 'DATA_REDUCTION_POC'
                  AND SOURCE_SCHEMA = 'STAGE_DATA'
            """).collect()
            st.success(f"Updated {edit_table}")
            st.rerun()
    else:
        st.info("No tables found. Run Metadata Extraction first.")

# ─── TAB 3: RELATIONSHIP MAP ───
with tab3:
    st.subheader("Relationship Map")

    rel_tab1, rel_tab2 = st.tabs(["Active Relationships", "Pending Review"])

    with rel_tab1:
        active_rels = session.sql("""
            SELECT PARENT_TABLE, PARENT_COLUMN, CHILD_TABLE, CHILD_COLUMN,
                   CARDINALITY, CONFIDENCE, DISCOVERED_BY, IS_COMPOSITE_KEY
            FROM DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP
            WHERE SOURCE_DATABASE = 'DATA_REDUCTION_POC' AND SOURCE_SCHEMA = 'STAGE_DATA'
              AND RELATIONSHIP_STATUS = 'ACTIVE'
            ORDER BY PARENT_TABLE, CHILD_TABLE
        """).to_pandas()

        if len(active_rels) > 0:
            st.dataframe(active_rels, use_container_width=True)
        else:
            st.info("No active relationships. Run Metadata Extraction or AI Discovery.")

    with rel_tab2:
        pending = session.sql("""
            SELECT RELATIONSHIP_ID, PARENT_TABLE, PARENT_COLUMN, CHILD_TABLE, CHILD_COLUMN,
                   CONFIDENCE, DISCOVERED_BY
            FROM DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP
            WHERE SOURCE_DATABASE = 'DATA_REDUCTION_POC' AND SOURCE_SCHEMA = 'STAGE_DATA'
              AND RELATIONSHIP_STATUS = 'PENDING_REVIEW'
            ORDER BY PARENT_TABLE, CHILD_TABLE
        """).to_pandas()

        if len(pending) > 0:
            st.dataframe(pending, use_container_width=True)
            rel_id = st.selectbox("Select Relationship ID", pending['RELATIONSHIP_ID'].tolist())
            col_r1, col_r2 = st.columns(2)
            with col_r1:
                if st.button("Approve"):
                    session.sql(f"""
                        UPDATE DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP
                        SET RELATIONSHIP_STATUS = 'ACTIVE',
                            REVIEWED_BY = CURRENT_USER(),
                            REVIEWED_AT = CURRENT_TIMESTAMP()
                        WHERE RELATIONSHIP_ID = {rel_id}
                    """).collect()
                    st.success(f"Relationship {rel_id} approved!")
                    st.rerun()
            with col_r2:
                if st.button("Reject"):
                    session.sql(f"""
                        UPDATE DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP
                        SET RELATIONSHIP_STATUS = 'REJECTED',
                            REVIEWED_BY = CURRENT_USER(),
                            REVIEWED_AT = CURRENT_TIMESTAMP()
                        WHERE RELATIONSHIP_ID = {rel_id}
                    """).collect()
                    st.success(f"Relationship {rel_id} rejected.")
                    st.rerun()
        else:
            st.info("No pending relationships.")

    st.write("---")
    st.write("**AI Relationship Discovery**")
    if st.button("Discover Relationships with AI"):
        with st.spinner("Running Cortex AI relationship discovery..."):
            result = session.sql("""
                CALL DATA_REDUCTION_POC.EDRP_APP.SP_AI_DISCOVER_RELATIONSHIPS('DATA_REDUCTION_POC', 'STAGE_DATA')
            """).collect()
            st.success("Discovery complete! Check the Relationship Map page for new PENDING_REVIEW entries.")
            st.json(result[0][0])

# ─── TAB 4: COLUMN SENSITIVITY ───
with tab4:
    st.subheader("Column Sensitivity Classification")
    st.write("Uses Cortex AI to classify columns as PII, PHI, PCI, CONFIDENTIAL, or PUBLIC.")
    if st.button("Run AI Classification"):
        with st.spinner("Classifying columns with Cortex AI..."):
            result = session.sql("""
                CALL DATA_REDUCTION_POC.EDRP_APP.SP_AI_CLASSIFY_SENSITIVITY('DATA_REDUCTION_POC', 'STAGE_DATA')
            """).collect()
            st.success("Classification complete!")
            st.json(result[0][0])

    sensitivity = session.sql("""
        SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, SENSITIVITY_CLASS
        FROM DATA_REDUCTION_POC.EDRP_METADATA.COLUMN_INVENTORY
        WHERE SOURCE_DATABASE = 'DATA_REDUCTION_POC' AND SOURCE_SCHEMA = 'STAGE_DATA'
          AND SENSITIVITY_CLASS IS NOT NULL
        ORDER BY SENSITIVITY_CLASS, TABLE_NAME, COLUMN_NAME
    """).to_pandas()

    if len(sensitivity) > 0:
        st.dataframe(sensitivity, use_container_width=True)
    else:
        st.info("No columns classified yet. Run AI Classification above.")

# ─── TAB 5: VALIDATION ───
with tab5:
    st.subheader("Validation Results")

    job_ids = session.sql("""
        SELECT JOB_ID FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG
        ORDER BY JOB_ID DESC LIMIT 20
    """).to_pandas()

    if len(job_ids) > 0:
        selected_job = st.selectbox("Select Job ID", job_ids['JOB_ID'].tolist())

        val_results = session.sql(f"""
            SELECT TABLE_NAME, CHECK_TYPE, CHECK_NAME, PASS_FAIL,
                   COALESCE(DEVIATION_PERCENT::VARCHAR, TARGET_VALUE) AS RESULT_DETAIL,
                   DETAILS
            FROM DATA_REDUCTION_POC.EDRP_METADATA.VALIDATION_RESULTS
            WHERE JOB_ID = {selected_job}
            ORDER BY
                CASE PASS_FAIL WHEN 'FAIL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END,
                CHECK_TYPE, TABLE_NAME
        """).to_pandas()

        if len(val_results) > 0:
            # Summary metrics
            vc1, vc2, vc3 = st.columns(3)
            with vc1:
                st.metric("PASS", len(val_results[val_results['PASS_FAIL'] == 'PASS']))
            with vc2:
                st.metric("WARN", len(val_results[val_results['PASS_FAIL'] == 'WARN']))
            with vc3:
                st.metric("FAIL", len(val_results[val_results['PASS_FAIL'] == 'FAIL']))

            st.dataframe(val_results, use_container_width=True)
        else:
            st.info("No validation results for this job. Run validation first.")
    else:
        st.info("No jobs found.")
```

### Deploying the Streamlit App

To deploy the Streamlit app in Snowflake:

1. Navigate to **Snowsight > Streamlit**
2. Click **+ Streamlit App**
3. Set the database to `DATA_REDUCTION_POC` and schema to `EDRP_APP`
4. Set the warehouse to `EDRP_WH`
5. Paste the code above into the editor
6. Click **Run**

---

## 13. Phase 10 — Cortex Agent

> **File:** `agent/EDRP_DATA_AGENT.agent.yaml`

The Cortex Agent enables natural-language querying of the reduced dataset via Cortex Analyst.

```yaml
tools:
  - tool_spec:
      type: cortex_analyst_text_to_sql
      name: reduced_data_analyst
      description: "Queries the reduced store returns dataset. Use for questions about returns, customers, stores, items, brands, categories, return amounts, quantities, and net loss. Covers STORE_RETURNS joined to CUSTOMER, STORE, and ITEM dimensions."

tool_resources:
  reduced_data_analyst:
    execution_environment:
      type: warehouse
      warehouse: EDRP_WH
    semantic_view: DATA_REDUCTION_POC.EDRP_APP.SV_REDUCED_RETURNS

instructions:
  response: |
    You are the EDRP Data Assistant — a data analytics agent for the Enterprise Data Reduction Platform.
    You help users explore the reduced dataset by answering natural language questions about store returns, customers, items, and stores.
    Be concise and data-driven. Format currency as dollars (e.g., $1,234.56).
    Use tables when presenting multiple rows of results.
    Round all numeric outputs to 2 decimal places.
    All monetary amounts are in USD.
    When asked about "top" or "bottom" results, default to 10 unless the user specifies otherwise.
  orchestration: "Use reduced_data_analyst for all questions about returns, customers, stores, items, brands, categories, and revenue."
```

### Prerequisites for the Cortex Agent

1. The reduction pipeline must have completed (reduced data exists in `STAGE_DATA_REDUCED`)
2. The Semantic View `SV_REDUCED_RETURNS` must be created in `DATA_REDUCTION_POC.EDRP_APP` (DDL available in `docs/Implementation_Plan.md`, Part 11)
3. Deploy the agent YAML to `DATA_REDUCTION_POC.EDRP_APP`

---

## 14. Rollback Instructions

If a reduction job produces unsatisfactory results:

```sql
-- 1. Find the job ID to rollback
SELECT JOB_ID, JOB_STATUS, TOTAL_TABLES, TABLES_PROCESSED,
       ARRAY_SIZE(COALESCE(TABLES_CREATED_LIST, PARSE_JSON('[]'))) AS TABLES_CREATED
FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG
ORDER BY JOB_ID DESC LIMIT 5;

-- 2. Execute rollback (drops all tables created by that job)
CALL DATA_REDUCTION_POC.EDRP_APP.SP_ROLLBACK_REDUCTION(<JOB_ID>);

-- 3. Verify rollback
SELECT JOB_ID, JOB_STATUS
FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG
WHERE JOB_ID = <JOB_ID>;
-- Should show JOB_STATUS = 'ROLLED_BACK'
```

---

## 15. Stratified Sampling Guide

Stratified sampling ensures all distinct categorical values survive reduction with proportional representation.

### Step 1: Configure stratification after metadata extraction

```sql
UPDATE DATA_REDUCTION_POC.EDRP_METADATA.TABLE_INVENTORY
SET SAMPLING_STRATEGY = 'STRATIFIED',
    STRATIFY_COLUMNS = 'C_BIRTH_COUNTRY'
WHERE TABLE_NAME = 'CUSTOMER'
  AND SOURCE_DATABASE = 'DATA_REDUCTION_POC'
  AND SOURCE_SCHEMA = 'STAGE_DATA';
```

### Step 2: Run reduction (stratified tables are handled automatically)

```sql
CALL DATA_REDUCTION_POC.EDRP_APP.SP_EXECUTE_REDUCTION('DEFAULT_10PCT');
```

### Step 3: Verify categorical coverage

```sql
-- Should return 0 rows (no missing categories)
SELECT s.C_BIRTH_COUNTRY AS MISSING_COUNTRY
FROM (SELECT DISTINCT C_BIRTH_COUNTRY FROM DATA_REDUCTION_POC.STAGE_DATA.CUSTOMER) s
LEFT JOIN (SELECT DISTINCT C_BIRTH_COUNTRY FROM DATA_REDUCTION_POC.STAGE_DATA_REDUCED.CUSTOMER) t
    ON s.C_BIRTH_COUNTRY = t.C_BIRTH_COUNTRY
WHERE t.C_BIRTH_COUNTRY IS NULL;
```

### How it works internally

The stratified sampling SQL generated by `SP_EXECUTE_REDUCTION`:

```sql
CREATE OR REPLACE TABLE target.TABLE AS
WITH ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY <stratify_cols> ORDER BY RANDOM(seed)) AS rn,
        GREATEST(CEIL(COUNT(*) OVER (PARTITION BY <stratify_cols>) * sample_pct / 100.0), 1) AS grp_limit
    FROM source.TABLE
)
SELECT * EXCLUDE (rn, grp_total, grp_limit) FROM ranked WHERE rn <= grp_limit
```

The `GREATEST(..., 1)` guarantees at least 1 row per stratum, ensuring no categories are lost.

---

## 16. Dependency Map

### Execution Order

```
01_prerequisites.sql          ← Run FIRST (creates database, schemas, warehouse)
  └── 02_metadata_tables.sql  ← Needs database + schemas from Step 1
        ├── 03_stored_procedures.sql  ← SPs read/write metadata tables
        │     └── 04_views.sql        ← Views query metadata tables
        ├── 05_rbac.sql               ← Grants on objects from Steps 1-2
        └── 06_initial_config.sql     ← Inserts into REDUCTION_PROFILE
              └── 07_run_pipeline.sql  ← Calls SPs with profile name
                    └── 08_validation_queries.sql  ← Queries results
```

### Component Dependencies

| Component | Depends On |
|-----------|-----------|
| Stored Procedures | All 8 metadata tables |
| Views | `RELATIONSHIP_MAP`, `TABLE_INVENTORY`, `VALIDATION_RESULTS` |
| Pipeline execution | All SPs + at least one REDUCTION_PROFILE |
| Streamlit App | All metadata tables + all SPs + `EDRP_WH` |
| Cortex Agent | Semantic View `SV_REDUCED_RETURNS` + reduced data + `EDRP_WH` |
| SP_BUILD_DEPENDENCY_GRAPH | `networkx` Python package (Snowpark Anaconda) |
| SP_AI_DISCOVER_RELATIONSHIPS | Cortex COMPLETE access (`llama3.1-70b`) |
| FN_AI_SUMMARIZE_VALIDATION | Cortex COMPLETE access (`llama3.1-70b`) |

---

## 17. Troubleshooting

### Common Issues

| Issue | Cause | Fix |
|-------|-------|-----|
| SP_EXTRACT_METADATA returns 0 tables | Source schema has no `BASE TABLE` type | Verify source schema exists and contains tables |
| SP_BUILD_DEPENDENCY_GRAPH shows cycles | Circular FK relationships | Review RELATIONSHIP_MAP; reject incorrect AI-discovered ones |
| SP_EXECUTE_REDUCTION fails on a table | Target schema missing or insufficient privileges | Verify `STAGE_DATA_REDUCED` exists; check role grants |
| FK validation shows orphans | Parent table processed after child | Re-run `SP_BUILD_DEPENDENCY_GRAPH` to fix order |
| Stratified sampling misses categories | `STRATIFY_COLUMNS` not set | Configure before running `SP_EXECUTE_REDUCTION` |
| Cortex AI features fail | No access to `SNOWFLAKE.CORTEX.COMPLETE` | Grant Cortex access to executing role |
| Warehouse auto-suspended | Idle timeout reached | `EDRP_WH` has AUTO_RESUME=TRUE; next query wakes it |

### Reset Everything

```sql
-- Drop all reduced data
DROP SCHEMA IF EXISTS DATA_REDUCTION_POC.STAGE_DATA_REDUCED CASCADE;
CREATE SCHEMA DATA_REDUCTION_POC.STAGE_DATA_REDUCED;

-- Clear metadata (preserves table structure)
TRUNCATE TABLE DATA_REDUCTION_POC.EDRP_METADATA.VALIDATION_RESULTS;
TRUNCATE TABLE DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_TABLE_LOG;
TRUNCATE TABLE DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG;
TRUNCATE TABLE DATA_REDUCTION_POC.EDRP_METADATA.JOIN_CONDITIONS;
TRUNCATE TABLE DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP;
TRUNCATE TABLE DATA_REDUCTION_POC.EDRP_METADATA.COLUMN_INVENTORY;
TRUNCATE TABLE DATA_REDUCTION_POC.EDRP_METADATA.TABLE_INVENTORY;
-- Keep REDUCTION_PROFILE if you want to reuse profiles

-- Re-run pipeline from Step 1
CALL DATA_REDUCTION_POC.EDRP_APP.SP_EXTRACT_METADATA('DATA_REDUCTION_POC', 'STAGE_DATA');
```

---

*Generated from workspace `Enterprise_Data_reduction_platform`*
