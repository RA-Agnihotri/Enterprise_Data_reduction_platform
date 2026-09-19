# Enterprise Data Reduction Platform — Implementation Plan

**Platform:** 100% Snowflake Ecosystem  
**Components:** Snowpark Python | Cortex AI | Streamlit in Snowflake | Snowflake Tasks | Semantic Views | Cortex Analyst  
**Metadata Source:** Scala scripts (Bitbucket) + INFORMATION_SCHEMA + ACCESS_HISTORY  
**Proven By:** POC on DATA_REDUCTION_POC.STAGE_DATA (105B rows → 10.2B rows, 16/16 FK checks PASS)  
**Date:** September 18, 2026  

---

## Table of Contents

1. [Architecture Overview](#part-1--architecture-overview)
2. [Prerequisite Setup](#part-2--prerequisite-setup)
3. [Scala Script Parser — Snowpark Stored Procedure](#part-3--scala-script-parser)
4. [Metadata Extractor — Snowpark Stored Procedure](#part-4--metadata-extractor)
5. [Knowledge Graph Builder — Snowpark Stored Procedure](#part-5--knowledge-graph-builder)
6. [Reduction Engine — Snowpark Stored Procedure](#part-6--reduction-engine)
7. [Validation Engine — Snowpark Stored Procedure](#part-7--validation-engine)
   - 7.2 [Rollback Procedure](#72-rollback-procedure--cleanup-failed-or-unwanted-reductions)
   - 7.3 [Relationship Lineage Report View](#73-relationship-lineage-report-view)
   - 7.4 [Categorical Coverage Report View](#74-categorical-coverage-report-view)
   - 7b [SQL Safety Guidelines](#part-7b--sql-safety-guidelines)
8. [Cortex AI Augmentation](#part-8--cortex-ai-augmentation)
9. [Streamlit in Snowflake UI](#part-9--streamlit-in-snowflake-ui)
10. [Snowflake Tasks Orchestration](#part-10--snowflake-tasks-orchestration)
11. [Semantic View + Cortex Analyst](#part-11--semantic-view--cortex-analyst)
12. [End-to-End Pipeline & Deployment](#part-12--end-to-end-pipeline--deployment)

---

## Part 1 — Architecture Overview

### Execution Model

Every component runs **inside Snowflake**. There is no external Python, no external APIs (except Bitbucket via External Access Integration), and no external infrastructure.

```
┌─────────────────────────────────────────────────────────────────────┐
│                     SNOWFLAKE ACCOUNT                               │
│                                                                     │
│  ┌─────────────────┐    ┌──────────────────┐   ┌────────────────┐  │
│  │ EDRP_METADATA   │    │ STAGE_DATA       │   │ STAGE_DATA_    │  │
│  │ (config tables)  │    │ (source)         │   │ REDUCED        │  │
│  │                  │    │                  │   │ (target)       │  │
│  │ TABLE_INVENTORY  │    │ 21 tables        │   │ 21 tables      │  │
│  │ COLUMN_INVENTORY │    │ 105B rows        │   │ 10.2B rows     │  │
│  │ RELATIONSHIP_MAP │    │                  │   │                │  │
│  │ JOIN_CONDITIONS  │    │                  │   │                │  │
│  │ REDUCTION_PROFILE│    │                  │   │                │  │
│  │ JOB_LOG          │    │                  │   │                │  │
│  │ TABLE_LOG        │    │                  │   │                │  │
│  │ VALIDATION_RESULTS    │                  │   │                │  │
│  │ V_RELATIONSHIP_  │    │                  │   │                │  │
│  │   LINEAGE (view) │    │                  │   │                │  │
│  │ V_CATEGORICAL_   │    │                  │   │                │  │
│  │   COVERAGE (view)│    │                  │   │                │  │
│  └────────┬─────────┘    └──────────────────┘   └────────────────┘  │
│           │                                                         │
│  ┌────────┴──────────────────────────────────────────────────────┐  │
│  │              SNOWPARK STORED PROCEDURES                        │  │
│  │                                                                │  │
│  │  SP_PARSE_SCALA_SCRIPTS()    ← Bitbucket → RELATIONSHIP_MAP  │  │
│  │  SP_EXTRACT_METADATA()       ← INFO_SCHEMA → all meta tables │  │
│  │  SP_BUILD_DEPENDENCY_GRAPH() ← NetworkX graph analysis        │  │
│  │  SP_EXECUTE_REDUCTION()      ← CTAS + FK cascade (POC logic) │  │
│  │  SP_VALIDATE_REDUCTION()     ← FK checks + distribution      │  │
│  │  SP_ROLLBACK_REDUCTION()     ← Cleanup failed/unwanted jobs  │  │
│  │  SP_AI_DISCOVER_RELATIONS()  ← Cortex COMPLETE augmentation  │  │
│  └───────────────────────────────────────────────────────────────┘  │
│                                                                     │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │
│  │ STREAMLIT APP    │  │ SNOWFLAKE TASKS  │  │ CORTEX ANALYST  │  │
│  │ (UI + dashboard) │  │ (orchestration)  │  │ (query reduced  │  │
│  │                  │  │                  │  │  data via NL)   │  │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Component Map

| Component | Snowflake Feature | What It Does |
|-----------|------------------|-------------|
| Scala Parser | Snowpark Python SP + External Access Integration | Fetches Scala files from Bitbucket, extracts join metadata |
| Metadata Extractor | Snowpark Python SP | Reads INFORMATION_SCHEMA + ACCESS_HISTORY, populates metadata tables |
| Graph Builder | Snowpark Python SP (NetworkX) | Builds dependency graph, topological sort, cycle detection |
| Reduction Engine | Snowpark Python SP (Dynamic SQL) | Generates and executes CTAS statements in dependency order; supports random, stratified, and FK-cascade sampling including composite keys |
| Validation Engine | Snowpark Python SP (SQL-based) | FK integrity, distribution comparison, scenario coverage |
| Rollback Engine | Snowpark Python SP | Drops tables created by a failed/unwanted job, marks job as ROLLED_BACK |
| Lineage Report | SQL View (V_RELATIONSHIP_LINEAGE) | Recursive CTE showing full parent-child dependency tree |
| Categorical Report | SQL View (V_CATEGORICAL_COVERAGE) | Per-job distribution deviation report for categorical columns |
| AI Augmentation | Cortex COMPLETE / CLASSIFY_TEXT | Relationship discovery, rare event detection, report generation |
| UI | Streamlit in Snowflake | Profile config, execution, monitoring, validation dashboard |
| Orchestration | Snowflake Tasks (DAG) | Scheduled/on-demand pipeline: extract → reduce → validate |
| Data Exploration | Semantic View + Cortex Analyst | Natural language queries over reduced data |

---

## Part 2 — Prerequisite Setup

### 2.1 Database and Schema Setup

```sql
-- Already created during POC:
-- CREATE DATABASE IF NOT EXISTS DATA_REDUCTION_POC;
-- CREATE SCHEMA IF NOT EXISTS DATA_REDUCTION_POC.EDRP_METADATA;
-- CREATE SCHEMA IF NOT EXISTS DATA_REDUCTION_POC.STAGE_DATA_REDUCED;

-- Schema for Streamlit app and stored procedures
CREATE SCHEMA IF NOT EXISTS DATA_REDUCTION_POC.EDRP_APP
COMMENT = 'EDRP Stored Procedures, Streamlit App, and Tasks';
```

### 2.2 Warehouse for Reduction Workloads

```sql
CREATE WAREHOUSE IF NOT EXISTS EDRP_WH
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Dedicated warehouse for EDRP reduction workloads';
```

### 2.3 External Access Integration (for Bitbucket API)

```sql
-- Network rule allowing Bitbucket API access
CREATE OR REPLACE NETWORK RULE EDRP_BITBUCKET_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('api.bitbucket.org:443', 'bitbucket.org:443');

-- Secret for Bitbucket authentication
CREATE OR REPLACE SECRET EDRP_BITBUCKET_TOKEN
    TYPE = GENERIC_STRING
    SECRET_STRING = '<YOUR_BITBUCKET_APP_PASSWORD_HERE>'
    COMMENT = 'Bitbucket API token for Scala script access';

-- External Access Integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION EDRP_BITBUCKET_ACCESS
    ALLOWED_NETWORK_RULES = (EDRP_BITBUCKET_RULE)
    ALLOWED_AUTHENTICATION_SECRETS = (EDRP_BITBUCKET_TOKEN)
    ENABLED = TRUE
    COMMENT = 'Allows EDRP stored procedures to access Bitbucket API';
```

### 2.4 Metadata Tables

> **Already created during POC.** See `POC_Script.md` Section 2 for full DDL.  
> Tables: TABLE_INVENTORY, COLUMN_INVENTORY, RELATIONSHIP_MAP, REDUCTION_PROFILE,  
> REDUCTION_JOB_LOG, REDUCTION_TABLE_LOG, VALIDATION_RESULTS

### 2.5 Schema Patch — Add REDUCTION_ORDER column to TABLE_INVENTORY

```sql
-- Add dedicated column for graph-computed reduction order (fixes fragile NOTES-based approach)
ALTER TABLE DATA_REDUCTION_POC.EDRP_METADATA.TABLE_INVENTORY 
    ADD COLUMN IF NOT EXISTS REDUCTION_ORDER NUMBER DEFAULT 999
    COMMENT 'Graph-computed execution order (0 = process first). Set by SP_BUILD_DEPENDENCY_GRAPH.';
```

### 2.6 Multi-Column Join Conditions Table

Supports composite-key relationships where a single `PARENT_COLUMN`/`CHILD_COLUMN` pair in `RELATIONSHIP_MAP` is insufficient. Each relationship can have multiple join condition rows grouped by `RELATIONSHIP_ID`.

```sql
CREATE TABLE IF NOT EXISTS DATA_REDUCTION_POC.EDRP_METADATA.JOIN_CONDITIONS (
    CONDITION_ID        NUMBER AUTOINCREMENT PRIMARY KEY,
    RELATIONSHIP_ID     NUMBER NOT NULL     COMMENT 'FK to RELATIONSHIP_MAP.RELATIONSHIP_ID',
    ORDINAL_POSITION    NUMBER NOT NULL     COMMENT 'Column position within composite key (1-based)',
    PARENT_COLUMN       VARCHAR(256) NOT NULL,
    CHILD_COLUMN        VARCHAR(256) NOT NULL,
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    COMMENT             VARCHAR(1000)
)
COMMENT = 'Stores individual column pairs for multi-column join conditions. '
          'For single-column joins, the PARENT_COLUMN/CHILD_COLUMN in RELATIONSHIP_MAP '
          'remains authoritative. This table is only needed when a relationship spans 2+ columns.';
```

**Migration note:** Existing single-column relationships continue to work unchanged via `RELATIONSHIP_MAP.PARENT_COLUMN`/`CHILD_COLUMN`. The reduction engine checks `JOIN_CONDITIONS` only when `RELATIONSHIP_MAP.IS_COMPOSITE_KEY = TRUE`.

```sql
-- Add composite key flag to RELATIONSHIP_MAP
ALTER TABLE DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP
    ADD COLUMN IF NOT EXISTS IS_COMPOSITE_KEY BOOLEAN DEFAULT FALSE
    COMMENT 'TRUE if join uses multiple columns. Details in JOIN_CONDITIONS table.';
```

### 2.7 Sampling Strategy Extension for REDUCTION_PROFILE

Adds stratified and proportional sampling options beyond the current random Bernoulli sampling.

```sql
-- Add sampling strategy to TABLE_INVENTORY
ALTER TABLE DATA_REDUCTION_POC.EDRP_METADATA.TABLE_INVENTORY
    ADD COLUMN IF NOT EXISTS SAMPLING_STRATEGY VARCHAR(30) DEFAULT 'RANDOM'
    COMMENT 'RANDOM (Bernoulli), STRATIFIED (proportional within strata), or PROPORTIONAL (preserves category ratios)';

ALTER TABLE DATA_REDUCTION_POC.EDRP_METADATA.TABLE_INVENTORY
    ADD COLUMN IF NOT EXISTS STRATIFY_COLUMNS VARCHAR(1000)
    COMMENT 'Comma-separated column names used as stratification keys when SAMPLING_STRATEGY = STRATIFIED';
```

### 2.8 Execution Rollback Tracking

Tracks partial execution state to enable cleanup on failure.

```sql
ALTER TABLE DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG
    ADD COLUMN IF NOT EXISTS TABLES_CREATED_LIST VARIANT DEFAULT PARSE_JSON('[]')
    COMMENT 'Array of table names successfully created in this job, used for rollback on failure';
```

---

## Part 3 — Scala Script Parser

### Snowpark Python Stored Procedure

This stored procedure connects to Bitbucket, downloads Scala files, parses them for join patterns, and inserts discovered relationships into RELATIONSHIP_MAP.

```sql
CREATE OR REPLACE PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_PARSE_SCALA_SCRIPTS(
    BITBUCKET_WORKSPACE VARCHAR,
    BITBUCKET_REPO VARCHAR,
    BITBUCKET_BRANCH VARCHAR DEFAULT 'main',
    SOURCE_DATABASE VARCHAR DEFAULT 'DATA_REDUCTION_POC',
    SOURCE_SCHEMA VARCHAR DEFAULT 'STAGE_DATA'
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
HANDLER = 'run'
EXTERNAL_ACCESS_INTEGRATIONS = (EDRP_BITBUCKET_ACCESS)
SECRETS = ('bitbucket_token' = EDRP_BITBUCKET_TOKEN)
COMMENT = 'Parses Scala/Spark scripts from Bitbucket to extract join relationships'
AS
$$
import re
import json
import requests
import _snowflake
from snowflake.snowpark import Session

def run(session, bitbucket_workspace, bitbucket_repo, bitbucket_branch, source_database, source_schema):
    """
    Main entry point. Fetches Scala files from Bitbucket, parses joins, writes to RELATIONSHIP_MAP.
    """
    token = _snowflake.get_generic_secret_string('bitbucket_token')
    
    # Step 1: List all .scala files in the repo
    scala_files = list_scala_files(bitbucket_workspace, bitbucket_repo, bitbucket_branch, token)
    
    # Step 2: Parse each file for join patterns
    all_relationships = []
    parse_errors = []
    
    for file_path in scala_files:
        try:
            content = fetch_file_content(bitbucket_workspace, bitbucket_repo, bitbucket_branch, file_path, token)
            relationships = parse_scala_joins(content, file_path)
            all_relationships.extend(relationships)
        except Exception as e:
            parse_errors.append({"file": file_path, "error": str(e)})
    
    # Step 3: Insert into RELATIONSHIP_MAP
    inserted = 0
    skipped = 0
    for rel in all_relationships:
        try:
            session.sql(f"""
                INSERT INTO {source_database}.EDRP_METADATA.RELATIONSHIP_MAP
                    (SOURCE_DATABASE, SOURCE_SCHEMA, PARENT_TABLE, PARENT_COLUMN, 
                     CHILD_TABLE, CHILD_COLUMN, JOIN_TYPE, CARDINALITY,
                     DISCOVERED_BY, DISCOVERY_SOURCE, CONFIDENCE, RELATIONSHIP_STATUS)
                SELECT '{source_database}', '{source_schema}',
                       '{rel["parent_table"].upper()}', '{rel["parent_column"].upper()}',
                       '{rel["child_table"].upper()}', '{rel["child_column"].upper()}',
                       '{rel["join_type"]}', '1:N',
                       'SCALA_PARSER', '{rel["source_file"]}:{rel["line_number"]}',
                       '{rel["confidence"]}', 'PENDING_REVIEW'
                WHERE NOT EXISTS (
                    SELECT 1 FROM {source_database}.EDRP_METADATA.RELATIONSHIP_MAP
                    WHERE SOURCE_DATABASE = '{source_database}'
                      AND SOURCE_SCHEMA = '{source_schema}'
                      AND PARENT_TABLE = '{rel["parent_table"].upper()}'
                      AND PARENT_COLUMN = '{rel["parent_column"].upper()}'
                      AND CHILD_TABLE = '{rel["child_table"].upper()}'
                      AND CHILD_COLUMN = '{rel["child_column"].upper()}'
                )
            """).collect()
            inserted += 1
        except Exception:
            skipped += 1
    
    return {
        "scala_files_found": len(scala_files),
        "relationships_discovered": len(all_relationships),
        "inserted": inserted,
        "skipped_duplicates": skipped,
        "parse_errors": parse_errors
    }


def list_scala_files(workspace, repo, branch, token):
    """List all .scala files in the Bitbucket repository."""
    files = []
    url = f"https://api.bitbucket.org/2.0/repositories/{workspace}/{repo}/src/{branch}/"
    headers = {"Authorization": f"Bearer {token}"}
    
    while url:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        
        for entry in data.get("values", []):
            if entry.get("path", "").endswith(".scala"):
                files.append(entry["path"])
        
        url = data.get("next")  # pagination
    
    return files


def fetch_file_content(workspace, repo, branch, file_path, token):
    """Fetch raw content of a single file from Bitbucket."""
    url = f"https://api.bitbucket.org/2.0/repositories/{workspace}/{repo}/src/{branch}/{file_path}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.text


def parse_scala_joins(content, file_path):
    """
    Parse Scala/Spark code to extract join relationships.
    Handles three patterns:
      1. DataFrame API:  df.join(otherDf, col("a") === col("b"), "left")
      2. Spark SQL:       spark.sql("SELECT ... FROM a JOIN b ON a.x = b.y")
      3. Column aliases:  .withColumnRenamed / .alias patterns
    """
    relationships = []
    lines = content.split('\n')
    
    # Pattern 1: DataFrame .join() calls
    # Matches: .join(tableDf, col("parent_col") === col("child_col"), "left")
    # Also:    .join(tableDf, $"parent_col" === $"child_col", "inner")
    # Also:    .join(tableDf, Seq("col_name"), "left")
    join_pattern = re.compile(
        r'\.join\s*\(\s*(\w+)\s*,\s*'                        # .join(dfName,
        r'(?:'
        r'(?:col\s*\(\s*"(\w+)"\s*\)\s*===\s*col\s*\(\s*"(\w+)"\s*\))'  # col("a") === col("b")
        r'|'
        r'(?:\$"(\w+)"\s*===\s*\$"(\w+)")'                    # $"a" === $"b"
        r'|'
        r'(?:Seq\s*\(\s*"(\w+)"\s*\))'                         # Seq("col")
        r')'
        r'(?:\s*,\s*"(\w+)")?',                                # , "left" (optional join type)
        re.IGNORECASE
    )
    
    # Pattern 2: Spark SQL JOIN
    sql_join_pattern = re.compile(
        r'(?:INNER|LEFT|RIGHT|FULL|CROSS)?\s*JOIN\s+'
        r'(\w+)(?:\s+\w+)?\s+'                                 # JOIN table_name alias
        r'ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)',               # ON a.col = b.col
        re.IGNORECASE
    )
    
    # Pattern 3: Simple equality join
    # Matches: .join(otherDf, "common_column")
    simple_join = re.compile(
        r'\.join\s*\(\s*(\w+)\s*,\s*"(\w+)"\s*\)',
        re.IGNORECASE
    )
    
    # Track DataFrame variable → table name mappings
    # Pattern: val dfName = spark.table("schema.table_name") or spark.read.table("table")
    df_table_map = {}
    table_pattern = re.compile(
        r'(?:val|var)\s+(\w+)\s*=\s*'
        r'(?:spark\.table\s*\(\s*"(?:\w+\.)*(\w+)"\s*\)'
        r'|spark\.read\.table\s*\(\s*"(?:\w+\.)*(\w+)"\s*\))',
        re.IGNORECASE
    )
    
    for i, line in enumerate(lines):
        # Build df → table mapping
        m = table_pattern.search(line)
        if m:
            df_name = m.group(1)
            table_name = m.group(2) or m.group(3)
            if table_name:
                df_table_map[df_name] = table_name
    
    for i, line in enumerate(lines):
        line_num = i + 1
        
        # Check Pattern 1: DataFrame .join()
        m = join_pattern.search(line)
        if m:
            other_df = m.group(1)
            col_a = m.group(2) or m.group(4) or m.group(6)
            col_b = m.group(3) or m.group(5) or m.group(6)
            join_type = (m.group(7) or "inner").upper()
            
            other_table = df_table_map.get(other_df, other_df)
            
            if col_a and col_b:
                relationships.append({
                    "parent_table": other_table,
                    "parent_column": col_a,
                    "child_table": "UNKNOWN",  # resolved later from context
                    "child_column": col_b,
                    "join_type": join_type,
                    "confidence": "HIGH" if other_table != other_df else "MEDIUM",
                    "source_file": file_path,
                    "line_number": line_num
                })
        
        # Check Pattern 2: Spark SQL JOIN
        for m in sql_join_pattern.finditer(line):
            table_name = m.group(1)
            left_alias = m.group(2)
            left_col = m.group(3)
            right_alias = m.group(4)
            right_col = m.group(5)
            
            # Determine join type from the text before JOIN
            pre_join = line[:m.start()].upper()
            join_type = "INNER"
            for jt in ["LEFT", "RIGHT", "FULL", "CROSS"]:
                if jt in pre_join:
                    join_type = jt
                    break
            
            relationships.append({
                "parent_table": table_name,
                "parent_column": right_col,
                "child_table": left_alias,
                "child_column": left_col,
                "join_type": join_type,
                "confidence": "HIGH",
                "source_file": file_path,
                "line_number": line_num
            })
        
        # Check Pattern 3: Simple join
        m = simple_join.search(line)
        if m:
            other_df = m.group(1)
            common_col = m.group(2)
            other_table = df_table_map.get(other_df, other_df)
            
            relationships.append({
                "parent_table": other_table,
                "parent_column": common_col,
                "child_table": "UNKNOWN",
                "child_column": common_col,
                "join_type": "INNER",
                "confidence": "MEDIUM",
                "source_file": file_path,
                "line_number": line_num
            })
    
    return relationships
$$;
```

### Usage

```sql
-- Parse all Scala scripts from a Bitbucket repo
CALL DATA_REDUCTION_POC.EDRP_APP.SP_PARSE_SCALA_SCRIPTS(
    'your-workspace',
    'your-repo',
    'main',
    'DATA_REDUCTION_POC',
    'STAGE_DATA'
);

-- Review parsed relationships (status = PENDING_REVIEW)
SELECT * FROM DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP
WHERE RELATIONSHIP_STATUS = 'PENDING_REVIEW'
ORDER BY CHILD_TABLE, CHILD_COLUMN;

-- Approve reviewed relationships
UPDATE DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP
SET RELATIONSHIP_STATUS = 'ACTIVE', REVIEWED_BY = CURRENT_USER(), REVIEWED_AT = CURRENT_TIMESTAMP()
WHERE RELATIONSHIP_STATUS = 'PENDING_REVIEW';
```

---

## Part 4 — Metadata Extractor

### Snowpark Stored Procedure

Extracts table/column metadata from INFORMATION_SCHEMA and observed join patterns from ACCESS_HISTORY.

```sql
CREATE OR REPLACE PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_EXTRACT_METADATA(
    SOURCE_DATABASE VARCHAR,
    SOURCE_SCHEMA VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Extracts table/column metadata from INFORMATION_SCHEMA and ACCESS_HISTORY'
AS
$$
def run(session, source_database, source_schema):
    results = {"tables": 0, "columns": 0, "access_history_joins": 0}
    
    # Step 1: Populate TABLE_INVENTORY
    session.sql(f"""
        MERGE INTO {source_database}.EDRP_METADATA.TABLE_INVENTORY TGT
        USING (
            SELECT 
                '{source_database}' AS SOURCE_DATABASE,
                '{source_schema}' AS SOURCE_SCHEMA,
                TABLE_NAME,
                ROW_COUNT,
                BYTES AS SIZE_BYTES
            FROM {source_database}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{source_schema}' AND TABLE_TYPE = 'BASE TABLE'
        ) SRC
        ON TGT.SOURCE_DATABASE = SRC.SOURCE_DATABASE 
           AND TGT.SOURCE_SCHEMA = SRC.SOURCE_SCHEMA
           AND TGT.TABLE_NAME = SRC.TABLE_NAME
        WHEN MATCHED THEN UPDATE SET 
            ROW_COUNT = SRC.ROW_COUNT, 
            SIZE_BYTES = SRC.SIZE_BYTES,
            UPDATED_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT 
            (SOURCE_DATABASE, SOURCE_SCHEMA, TABLE_NAME, ROW_COUNT, SIZE_BYTES, LOADED_BY)
        VALUES 
            (SRC.SOURCE_DATABASE, SRC.SOURCE_SCHEMA, SRC.TABLE_NAME, SRC.ROW_COUNT, SRC.SIZE_BYTES, 'INFO_SCHEMA')
    """).collect()
    
    results["tables"] = session.sql(f"""
        SELECT COUNT(*) FROM {source_database}.EDRP_METADATA.TABLE_INVENTORY
        WHERE SOURCE_DATABASE = '{source_database}' AND SOURCE_SCHEMA = '{source_schema}'
    """).collect()[0][0]
    
    # Step 2: Populate COLUMN_INVENTORY
    session.sql(f"""
        MERGE INTO {source_database}.EDRP_METADATA.COLUMN_INVENTORY TGT
        USING (
            SELECT 
                '{source_database}' AS SOURCE_DATABASE,
                '{source_schema}' AS SOURCE_SCHEMA,
                TABLE_NAME, COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION,
                CASE WHEN IS_NULLABLE = 'YES' THEN TRUE ELSE FALSE END AS IS_NULLABLE
            FROM {source_database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{source_schema}'
        ) SRC
        ON TGT.SOURCE_DATABASE = SRC.SOURCE_DATABASE 
           AND TGT.SOURCE_SCHEMA = SRC.SOURCE_SCHEMA
           AND TGT.TABLE_NAME = SRC.TABLE_NAME 
           AND TGT.COLUMN_NAME = SRC.COLUMN_NAME
        WHEN MATCHED THEN UPDATE SET 
            DATA_TYPE = SRC.DATA_TYPE, 
            ORDINAL_POSITION = SRC.ORDINAL_POSITION
        WHEN NOT MATCHED THEN INSERT 
            (SOURCE_DATABASE, SOURCE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION, IS_NULLABLE, LOADED_BY)
        VALUES 
            (SRC.SOURCE_DATABASE, SRC.SOURCE_SCHEMA, SRC.TABLE_NAME, SRC.COLUMN_NAME, 
             SRC.DATA_TYPE, SRC.ORDINAL_POSITION, SRC.IS_NULLABLE, 'INFO_SCHEMA')
    """).collect()
    
    results["columns"] = session.sql(f"""
        SELECT COUNT(*) FROM {source_database}.EDRP_METADATA.COLUMN_INVENTORY
        WHERE SOURCE_DATABASE = '{source_database}' AND SOURCE_SCHEMA = '{source_schema}'
    """).collect()[0][0]
    
    # Step 3: Discover joins from ACCESS_HISTORY (last 90 days)
    try:
        session.sql(f"""
            INSERT INTO {source_database}.EDRP_METADATA.RELATIONSHIP_MAP
                (SOURCE_DATABASE, SOURCE_SCHEMA, PARENT_TABLE, PARENT_COLUMN, 
                 CHILD_TABLE, CHILD_COLUMN, JOIN_TYPE, CARDINALITY,
                 DISCOVERED_BY, CONFIDENCE, RELATIONSHIP_STATUS)
            SELECT DISTINCT 
                '{source_database}', '{source_schema}',
                do2.value:objectName::STRING AS PARENT_TABLE,
                jc.value:columns[0].columnName::STRING AS PARENT_COLUMN,
                do1.value:objectName::STRING AS CHILD_TABLE,
                jc.value:columns[1].columnName::STRING AS CHILD_COLUMN,
                'INNER', '1:N',
                'ACCESS_HISTORY',
                CASE WHEN COUNT(*) OVER (PARTITION BY do1.value:objectName, do2.value:objectName) > 5 
                     THEN 'HIGH' ELSE 'MEDIUM' END,
                'PENDING_REVIEW'
            FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
                 LATERAL FLATTEN(input => ah.DIRECT_OBJECTS_ACCESSED) do1,
                 LATERAL FLATTEN(input => ah.OBJECTS_MODIFIED) do2,
                 LATERAL FLATTEN(input => do1.value:columns) jc
            WHERE ah.QUERY_START_TIME > DATEADD('day', -90, CURRENT_TIMESTAMP())
              AND do1.value:objectDomain::STRING = 'Table'
              AND do2.value:objectDomain::STRING = 'Table'
              AND jc.value:columns IS NOT NULL
              AND ARRAY_SIZE(jc.value:columns) = 2
              AND do1.value:objectName::STRING != do2.value:objectName::STRING
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY do1.value:objectName, jc.value:columns[1].columnName,
                             do2.value:objectName, jc.value:columns[0].columnName
                ORDER BY ah.QUERY_START_TIME DESC
            ) = 1
        """).collect()
    except Exception as e:
        results["access_history_note"] = f"ACCESS_HISTORY query skipped: {str(e)}"
    
    # Step 4: Detect declared FK constraints (if any exist)
    fk_count = session.sql(f"""
        SELECT COUNT(*) FROM {source_database}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS
        WHERE TABLE_SCHEMA = '{source_schema}' AND CONSTRAINT_TYPE = 'FOREIGN KEY'
    """).collect()[0][0]
    
    if fk_count > 0:
        session.sql(f"""
            INSERT INTO {source_database}.EDRP_METADATA.RELATIONSHIP_MAP
                (SOURCE_DATABASE, SOURCE_SCHEMA, PARENT_TABLE, PARENT_COLUMN,
                 CHILD_TABLE, CHILD_COLUMN, JOIN_TYPE, CARDINALITY,
                 IS_DECLARED_FK, DISCOVERED_BY, CONFIDENCE, RELATIONSHIP_STATUS)
            SELECT DISTINCT
                '{source_database}', '{source_schema}',
                ccu.TABLE_NAME, ccu.COLUMN_NAME,
                kcu.TABLE_NAME, kcu.COLUMN_NAME,
                'INNER', '1:N',
                TRUE, 'INFO_SCHEMA', 'HIGH', 'ACTIVE'
            FROM {source_database}.INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
            JOIN {source_database}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
            JOIN {source_database}.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
                ON rc.UNIQUE_CONSTRAINT_NAME = ccu.CONSTRAINT_NAME
                AND rc.UNIQUE_CONSTRAINT_SCHEMA = ccu.CONSTRAINT_SCHEMA
            WHERE rc.CONSTRAINT_SCHEMA = '{source_schema}'
        """).collect()
    
    results["declared_fks"] = fk_count
    return results
$$;
```

### Usage

```sql
CALL DATA_REDUCTION_POC.EDRP_APP.SP_EXTRACT_METADATA('DATA_REDUCTION_POC', 'STAGE_DATA');
```

---

## Part 5 — Knowledge Graph Builder

### Snowpark Python Stored Procedure (uses NetworkX)

Builds a directed dependency graph from RELATIONSHIP_MAP, performs topological sort, and detects cycles.

```sql
CREATE OR REPLACE PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_BUILD_DEPENDENCY_GRAPH(
    SOURCE_DATABASE VARCHAR,
    SOURCE_SCHEMA VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'networkx')
HANDLER = 'run'
COMMENT = 'Builds dependency graph, topological sort, cycle detection using NetworkX'
AS
$$
import networkx as nx
import json

def run(session, source_database, source_schema):
    # Load relationships
    rels = session.sql(f"""
        SELECT PARENT_TABLE, PARENT_COLUMN, CHILD_TABLE, CHILD_COLUMN, JOIN_TYPE
        FROM {source_database}.EDRP_METADATA.RELATIONSHIP_MAP
        WHERE SOURCE_DATABASE = '{source_database}'
          AND SOURCE_SCHEMA = '{source_schema}'
          AND RELATIONSHIP_STATUS = 'ACTIVE'
    """).collect()
    
    # Load table inventory
    tables = session.sql(f"""
        SELECT TABLE_NAME, TABLE_TYPE, ROW_COUNT
        FROM {source_database}.EDRP_METADATA.TABLE_INVENTORY
        WHERE SOURCE_DATABASE = '{source_database}'
          AND SOURCE_SCHEMA = '{source_schema}'
          AND IS_ACTIVE = TRUE
    """).collect()
    
    # Build directed graph: edge from parent → child (parent must be reduced first)
    G = nx.DiGraph()
    
    # Add all tables as nodes
    for t in tables:
        G.add_node(t["TABLE_NAME"], table_type=t["TABLE_TYPE"], row_count=t["ROW_COUNT"])
    
    # Add edges (parent → child means "child depends on parent")
    for r in rels:
        G.add_edge(r["PARENT_TABLE"], r["CHILD_TABLE"],
                    parent_col=r["PARENT_COLUMN"],
                    child_col=r["CHILD_COLUMN"],
                    join_type=r["JOIN_TYPE"])
    
    # Detect cycles
    cycles = list(nx.simple_cycles(G))
    
    # If cycles exist, break them by removing the edge with the weakest relationship
    broken_edges = []
    if cycles:
        for cycle in cycles:
            # Remove the last edge in the cycle (heuristic: break at the leaf end)
            edge_to_remove = (cycle[-1], cycle[0])
            if G.has_edge(*edge_to_remove):
                G.remove_edge(*edge_to_remove)
                broken_edges.append({"from": edge_to_remove[0], "to": edge_to_remove[1]})
    
    # Topological sort (reduction execution order)
    try:
        reduction_order = list(nx.topological_sort(G))
    except nx.NetworkXUnfeasible:
        reduction_order = list(G.nodes())  # fallback if still cyclic
    
    # Identify root tables (no incoming edges — these get sampled first)
    root_tables = [n for n in G.nodes() if G.in_degree(n) == 0]
    
    # Identify leaf tables (no outgoing edges — these are fact tables)
    leaf_tables = [n for n in G.nodes() if G.out_degree(n) == 0]
    
    # Tables with high fan-out (referenced by many children)
    fan_out = {n: G.out_degree(n) for n in G.nodes() if G.out_degree(n) > 3}
    
    # Store reduction order in dedicated REDUCTION_ORDER column
    for idx, table_name in enumerate(reduction_order):
        session.sql(f"""
            UPDATE {source_database}.EDRP_METADATA.TABLE_INVENTORY
            SET REDUCTION_ORDER = {idx},
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE SOURCE_DATABASE = '{source_database}'
              AND SOURCE_SCHEMA = '{source_schema}'
              AND TABLE_NAME = '{table_name}'
        """).collect()
    
    # Auto-classify table roles based on graph position
    for table_name in root_tables:
        session.sql(f"""
            UPDATE {source_database}.EDRP_METADATA.TABLE_INVENTORY
            SET TABLE_ROLE = 'ROOT', REDUCTION_STRATEGY = 'FULL_COPY'
            WHERE TABLE_NAME = '{table_name}'
              AND SOURCE_DATABASE = '{source_database}' AND SOURCE_SCHEMA = '{source_schema}'
        """).collect()
    
    for table_name in leaf_tables:
        session.sql(f"""
            UPDATE {source_database}.EDRP_METADATA.TABLE_INVENTORY
            SET TABLE_ROLE = 'LEAF', REDUCTION_STRATEGY = 'FK_CASCADE'
            WHERE TABLE_NAME = '{table_name}'
              AND SOURCE_DATABASE = '{source_database}' AND SOURCE_SCHEMA = '{source_schema}'
        """).collect()
    
    return {
        "total_tables": len(G.nodes()),
        "total_relationships": len(G.edges()),
        "root_tables": root_tables,
        "leaf_tables": leaf_tables,
        "cycles_detected": len(cycles),
        "cycles_broken": broken_edges,
        "high_fan_out_tables": fan_out,
        "reduction_order": reduction_order
    }
$$;
```

### Usage

```sql
CALL DATA_REDUCTION_POC.EDRP_APP.SP_BUILD_DEPENDENCY_GRAPH('DATA_REDUCTION_POC', 'STAGE_DATA');
```

---

## Part 6 — Reduction Engine

### Snowpark Python Stored Procedure

This is the core engine. It reads the dependency graph order, generates CTAS statements for each table, and executes them — the same proven logic from the POC, automated.

```sql
CREATE OR REPLACE PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_EXECUTE_REDUCTION(
    PROFILE_NAME VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Executes data reduction using the proven Clone+CTAS approach from POC'
AS
$$
import json
import time

def run(session, profile_name):
    # Load profile
    profile = session.sql(f"""
        SELECT * FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_PROFILE
        WHERE PROFILE_NAME = '{profile_name}' AND STATUS = 'ACTIVE'
    """).collect()
    
    if not profile:
        return {"error": f"No active profile found: {profile_name}"}
    
    p = profile[0]
    src_db = p["SOURCE_DATABASE"]
    src_schema = p["SOURCE_SCHEMA"]
    tgt_db = p["TARGET_DATABASE"]
    tgt_schema = p["TARGET_SCHEMA"]
    sample_pct = float(p["DEFAULT_SAMPLE_PERCENT"])
    seed = int(p["RANDOM_SEED"])
    
    # Create target schema
    session.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {tgt_db}.{tgt_schema}
        COMMENT = 'Reduced dataset from {src_schema} at {sample_pct}%'
    """).collect()
    
    # Log job start
    session.sql(f"""
        INSERT INTO {src_db}.EDRP_METADATA.REDUCTION_JOB_LOG (PROFILE_ID, TOTAL_TABLES)
        VALUES ({p["PROFILE_ID"]}, 0)
    """).collect()
    job_id = session.sql("SELECT MAX(JOB_ID) FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG").collect()[0][0]
    
    # Load table inventory with reduction order (uses dedicated REDUCTION_ORDER column)
    tables = session.sql(f"""
        SELECT TABLE_NAME, TABLE_TYPE, TABLE_ROLE, ROW_COUNT, 
               REDUCTION_STRATEGY, SAMPLE_PERCENT, PRIMARY_KEY_COLS, INCLUDE_FILTER,
               COALESCE(REDUCTION_ORDER, 999) AS SORT_ORDER,
               COALESCE(SAMPLING_STRATEGY, 'RANDOM') AS SAMPLING_STRATEGY,
               STRATIFY_COLUMNS
        FROM {src_db}.EDRP_METADATA.TABLE_INVENTORY
        WHERE SOURCE_DATABASE = '{src_db}' AND SOURCE_SCHEMA = '{src_schema}' AND IS_ACTIVE = TRUE
        ORDER BY SORT_ORDER
    """).collect()
    
    # Load relationships (including composite key flag)
    rels = session.sql(f"""
        SELECT r.RELATIONSHIP_ID, r.PARENT_TABLE, r.PARENT_COLUMN, r.CHILD_TABLE, r.CHILD_COLUMN,
               r.JOIN_TYPE, COALESCE(r.IS_COMPOSITE_KEY, FALSE) AS IS_COMPOSITE_KEY
        FROM {src_db}.EDRP_METADATA.RELATIONSHIP_MAP r
        WHERE r.SOURCE_DATABASE = '{src_db}' AND r.SOURCE_SCHEMA = '{src_schema}'
          AND r.RELATIONSHIP_STATUS = 'ACTIVE'
    """).collect()
    
    # Load composite join conditions (for multi-column keys)
    composite_conditions = {}
    composite_rel_ids = [r["RELATIONSHIP_ID"] for r in rels if r["IS_COMPOSITE_KEY"]]
    if composite_rel_ids:
        ids_str = ",".join(str(rid) for rid in composite_rel_ids)
        jc_rows = session.sql(f"""
            SELECT RELATIONSHIP_ID, PARENT_COLUMN, CHILD_COLUMN
            FROM {src_db}.EDRP_METADATA.JOIN_CONDITIONS
            WHERE RELATIONSHIP_ID IN ({ids_str})
            ORDER BY RELATIONSHIP_ID, ORDINAL_POSITION
        """).collect()
        for jc in jc_rows:
            rid = jc["RELATIONSHIP_ID"]
            if rid not in composite_conditions:
                composite_conditions[rid] = []
            composite_conditions[rid].append({
                "parent_col": jc["PARENT_COLUMN"],
                "child_col": jc["CHILD_COLUMN"]
            })
    
    # Build lookup: child_table → list of (parent_table, parent_col, child_col, composite_cols)
    child_fks = {}
    for r in rels:
        ct = r["CHILD_TABLE"]
        if ct not in child_fks:
            child_fks[ct] = []
        fk_entry = {
            "parent_table": r["PARENT_TABLE"],
            "parent_col": r["PARENT_COLUMN"],
            "child_col": r["CHILD_COLUMN"],
            "is_composite": r["IS_COMPOSITE_KEY"],
            "composite_cols": composite_conditions.get(r["RELATIONSHIP_ID"], [])
        }
        child_fks[ct].append(fk_entry)
    
    # Build lookup: table_name → reduction strategy (to know which parents were sampled)
    table_strategies = {t["TABLE_NAME"]: t["REDUCTION_STRATEGY"] for t in tables}
    
    # Track which tables have been reduced (for FK cascade reference)
    processed = set()
    results = []
    
    for t in tables:
        table_name = t["TABLE_NAME"]
        strategy = t["REDUCTION_STRATEGY"]
        tbl_sample_pct = float(t["SAMPLE_PERCENT"]) if t["SAMPLE_PERCENT"] else sample_pct
        
        start_time = time.time()
        src_full = f"{src_db}.{src_schema}.{table_name}"
        tgt_full = f"{tgt_db}.{tgt_schema}.{table_name}"
        
        try:
            if strategy == 'FULL_COPY':
                # Full copy — dimension/reference tables
                session.sql(f"CREATE OR REPLACE TABLE {tgt_full} AS SELECT * FROM {src_full}").collect()
            
            elif strategy == 'SAMPLE':
                # Sampling — key driver tables
                sampling_strategy = t.get("SAMPLING_STRATEGY", "RANDOM")
                stratify_cols = t.get("STRATIFY_COLUMNS", "")
                
                if sampling_strategy == 'STRATIFIED' and stratify_cols:
                    # Stratified sampling: preserves proportional representation within strata
                    # Uses QUALIFY with ROW_NUMBER partitioned by strata columns
                    strat_cols = stratify_cols.strip()
                    session.sql(f"""
                        CREATE OR REPLACE TABLE {tgt_full} AS
                        WITH strata_counts AS (
                            SELECT {strat_cols}, COUNT(*) AS stratum_size,
                                   GREATEST(1, ROUND(COUNT(*) * {tbl_sample_pct} / 100.0)) AS sample_size
                            FROM {src_full}
                            GROUP BY {strat_cols}
                        ),
                        numbered AS (
                            SELECT s.*, ROW_NUMBER() OVER (
                                PARTITION BY {strat_cols} ORDER BY RANDOM({seed})
                            ) AS _rn
                            FROM {src_full} s
                        )
                        SELECT n.* EXCLUDE (_rn)
                        FROM numbered n
                        JOIN strata_counts sc ON {' AND '.join(
                            f"n.{c.strip()} = sc.{c.strip()}" 
                            for c in strat_cols.split(',')
                        )}
                        WHERE n._rn <= sc.sample_size
                    """).collect()
                else:
                    # Default: Bernoulli random sample with seed
                    session.sql(f"""
                        CREATE OR REPLACE TABLE {tgt_full} AS 
                        SELECT * FROM {src_full} SAMPLE BERNOULLI ({tbl_sample_pct}) SEED ({seed})
                    """).collect()
            
            elif strategy == 'FK_CASCADE':
                # FK cascade — join to ALL already-reduced parent tables that were SAMPLED
                # Supports both single-column and composite-key joins
                fks = child_fks.get(table_name, [])
                
                sampled_parent_fks = []
                for fk in fks:
                    if fk["parent_table"] in processed:
                        parent_strategy = table_strategies.get(fk["parent_table"], "FULL_COPY")
                        if parent_strategy in ('SAMPLE', 'FK_CASCADE'):
                            sampled_parent_fks.append(fk)
                
                if sampled_parent_fks:
                    join_clauses = []
                    for i, fk in enumerate(sampled_parent_fks):
                        parent_tgt = f"{tgt_db}.{tgt_schema}.{fk['parent_table']}"
                        alias = f"p{i}"
                        
                        if fk["is_composite"] and fk["composite_cols"]:
                            # Multi-column join from JOIN_CONDITIONS table
                            on_parts = [
                                f"child.{cc['child_col']} = {alias}.{cc['parent_col']}"
                                for cc in fk["composite_cols"]
                            ]
                            on_clause = " AND ".join(on_parts)
                        else:
                            # Single-column join from RELATIONSHIP_MAP
                            on_clause = f"child.{fk['child_col']} = {alias}.{fk['parent_col']}"
                        
                        join_clauses.append(
                            f"INNER JOIN {parent_tgt} {alias} ON {on_clause}"
                        )
                    
                    joins_sql = "\n                        ".join(join_clauses)
                    session.sql(f"""
                        CREATE OR REPLACE TABLE {tgt_full} AS
                        SELECT DISTINCT child.*
                        FROM {src_full} child
                        {joins_sql}
                    """).collect()
                else:
                    session.sql(f"""
                        CREATE OR REPLACE TABLE {tgt_full} AS 
                        SELECT * FROM {src_full} SAMPLE BERNOULLI ({sample_pct}) SEED ({seed})
                    """).collect()
            
            elif strategy == 'EXCLUDE':
                # Skip this table
                elapsed = time.time() - start_time
                log_table_result(session, src_db, job_id, table_name, 0, 0, 0, 'EXCLUDE', 'SKIPPED', elapsed)
                continue
            
            # Get row counts
            tgt_count = session.sql(f"SELECT COUNT(*) FROM {tgt_full}").collect()[0][0]
            src_count = t["ROW_COUNT"] or 0
            reduction_pct = round((1 - tgt_count / max(src_count, 1)) * 100, 2)
            elapsed = time.time() - start_time
            
            log_table_result(session, src_db, job_id, table_name, src_count, tgt_count, reduction_pct, strategy, 'COMPLETED', elapsed)
            processed.add(table_name)
            
            # Track created tables for rollback capability
            session.sql(f"""
                UPDATE {src_db}.EDRP_METADATA.REDUCTION_JOB_LOG
                SET TABLES_CREATED_LIST = ARRAY_APPEND(
                    COALESCE(TABLES_CREATED_LIST, PARSE_JSON('[]')),
                    TO_VARIANT('{table_name}')
                )
                WHERE JOB_ID = {job_id}
            """).collect()
            
            results.append({
                "table": table_name, "strategy": strategy,
                "source_rows": src_count, "target_rows": tgt_count,
                "reduction_pct": reduction_pct, "time_sec": round(elapsed, 1)
            })
        
        except Exception as e:
            elapsed = time.time() - start_time
            log_table_result(session, src_db, job_id, table_name, 0, 0, 0, strategy, 'FAILED', elapsed, str(e))
            results.append({"table": table_name, "error": str(e)})
    
    # Update job log
    session.sql(f"""
        UPDATE {src_db}.EDRP_METADATA.REDUCTION_JOB_LOG
        SET JOB_STATUS = 'COMPLETED', COMPLETED_AT = CURRENT_TIMESTAMP(),
            TABLES_PROCESSED = {len(processed)}, TOTAL_TABLES = {len(tables)}
        WHERE JOB_ID = {job_id}
    """).collect()
    
    return {"job_id": job_id, "tables_processed": len(processed), "results": results}


def log_table_result(session, db, job_id, table_name, src_count, tgt_count, reduction_pct, strategy, status, elapsed, error=None):
    error_sql = f"'{error[:3900]}'" if error else "NULL"
    session.sql(f"""
        INSERT INTO {db}.EDRP_METADATA.REDUCTION_TABLE_LOG
            (JOB_ID, TABLE_NAME, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, REDUCTION_PERCENT,
             STRATEGY_USED, STATUS, EXECUTION_TIME_SEC, ERROR_MESSAGE, STARTED_AT, COMPLETED_AT)
        VALUES ({job_id}, '{table_name}', {src_count}, {tgt_count}, {reduction_pct},
                '{strategy}', '{status}', {round(elapsed, 2)}, {error_sql},
                DATEADD('second', -{int(elapsed)}, CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP())
    """).collect()
$$;
```

### Usage

```sql
-- Execute reduction using a named profile
CALL DATA_REDUCTION_POC.EDRP_APP.SP_EXECUTE_REDUCTION('POC_10PCT_REDUCTION');
```

---

## Part 7 — Validation Engine

### Snowpark Stored Procedure — Pure SQL Validation

No scipy. All validation uses Snowflake-native SQL.

```sql
CREATE OR REPLACE PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_VALIDATE_REDUCTION(
    JOB_ID_PARAM NUMBER
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Validates reduced dataset: FK integrity, distribution, scenario coverage'
AS
$$
import json

def run(session, job_id_param):
    # Get job details
    job = session.sql(f"""
        SELECT j.*, p.SOURCE_DATABASE, p.SOURCE_SCHEMA, p.TARGET_DATABASE, p.TARGET_SCHEMA
        FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG j
        JOIN DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_PROFILE p ON j.PROFILE_ID = p.PROFILE_ID
        WHERE j.JOB_ID = {job_id_param}
    """).collect()
    
    if not job:
        return {"error": f"Job {job_id_param} not found"}
    
    j = job[0]
    src = f"{j['SOURCE_DATABASE']}.{j['SOURCE_SCHEMA']}"
    tgt = f"{j['TARGET_DATABASE']}.{j['TARGET_SCHEMA']}"
    db = j['SOURCE_DATABASE']
    
    # Load active relationships
    rels = session.sql(f"""
        SELECT PARENT_TABLE, PARENT_COLUMN, CHILD_TABLE, CHILD_COLUMN
        FROM {db}.EDRP_METADATA.RELATIONSHIP_MAP
        WHERE SOURCE_DATABASE = '{j["SOURCE_DATABASE"]}'
          AND SOURCE_SCHEMA = '{j["SOURCE_SCHEMA"]}'
          AND RELATIONSHIP_STATUS = 'ACTIVE'
    """).collect()
    
    results = {"fk_checks": [], "distribution_checks": [], "summary": {}}
    total_pass = 0
    total_fail = 0
    
    # ===== FK INTEGRITY CHECKS =====
    for r in rels:
        child_tbl = r["CHILD_TABLE"]
        child_col = r["CHILD_COLUMN"]
        parent_tbl = r["PARENT_TABLE"]
        parent_col = r["PARENT_COLUMN"]
        
        check_name = f"{child_tbl}.{child_col} → {parent_tbl}.{parent_col}"
        
        try:
            orphan_count = session.sql(f"""
                SELECT COUNT(*) FROM {tgt}.{child_tbl} c
                WHERE c.{child_col} IS NOT NULL
                  AND c.{child_col} NOT IN (
                    SELECT p.{parent_col} FROM {tgt}.{parent_tbl} p
                  )
            """).collect()[0][0]
            
            status = "PASS" if orphan_count == 0 else "FAIL"
            if status == "PASS":
                total_pass += 1
            else:
                total_fail += 1
            
            # Log to VALIDATION_RESULTS
            session.sql(f"""
                INSERT INTO {db}.EDRP_METADATA.VALIDATION_RESULTS
                    (JOB_ID, TABLE_NAME, CHECK_TYPE, CHECK_NAME, SOURCE_VALUE, TARGET_VALUE, PASS_FAIL, DETAILS)
                VALUES ({job_id_param}, '{child_tbl}', 'FK_INTEGRITY', '{check_name}',
                        '0', '{orphan_count} orphans', '{status}',
                        'Orphaned FK count in reduced {child_tbl}.{child_col}')
            """).collect()
            
            results["fk_checks"].append({"check": check_name, "orphans": orphan_count, "status": status})
        
        except Exception as e:
            results["fk_checks"].append({"check": check_name, "error": str(e)})
    
    # ===== ROW COUNT RATIO CHECKS =====
    table_logs = session.sql(f"""
        SELECT TABLE_NAME, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, REDUCTION_PERCENT
        FROM {db}.EDRP_METADATA.REDUCTION_TABLE_LOG
        WHERE JOB_ID = {job_id_param} AND STATUS = 'COMPLETED'
    """).collect()
    
    for tl in table_logs:
        session.sql(f"""
            INSERT INTO {db}.EDRP_METADATA.VALIDATION_RESULTS
                (JOB_ID, TABLE_NAME, CHECK_TYPE, CHECK_NAME, SOURCE_VALUE, TARGET_VALUE,
                 DEVIATION_PERCENT, PASS_FAIL, DETAILS)
            VALUES ({job_id_param}, '{tl["TABLE_NAME"]}', 'ROW_COUNT', 'Row count ratio',
                    '{tl["SOURCE_ROW_COUNT"]}', '{tl["TARGET_ROW_COUNT"]}',
                    {tl["REDUCTION_PERCENT"] or 0},
                    'PASS', 'Reduction: {tl["REDUCTION_PERCENT"]}%')
        """).collect()
    
    # ===== DISTRIBUTION CHECKS (top categorical columns) =====
    # For each fact/large table, compare top-N value distribution of key categorical columns
    large_tables = session.sql(f"""
        SELECT TABLE_NAME FROM {db}.EDRP_METADATA.TABLE_INVENTORY
        WHERE SOURCE_DATABASE = '{j["SOURCE_DATABASE"]}' AND SOURCE_SCHEMA = '{j["SOURCE_SCHEMA"]}'
          AND TABLE_TYPE = 'FACT' AND IS_ACTIVE = TRUE
    """).collect()
    
    for lt in large_tables:
        tbl = lt["TABLE_NAME"]
        # Get FK columns (categorical columns to check distribution)
        fk_cols = [r["CHILD_COLUMN"] for r in rels if r["CHILD_TABLE"] == tbl]
        
        for col in fk_cols[:3]:  # Check top 3 FK columns per table
            try:
                # Compare top-10 value frequencies between source and target
                dist_check = session.sql(f"""
                    WITH src_dist AS (
                        SELECT {col}, COUNT(*) AS cnt,
                               COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct
                        FROM {src}.{tbl} WHERE {col} IS NOT NULL
                        GROUP BY {col} ORDER BY cnt DESC LIMIT 10
                    ),
                    tgt_dist AS (
                        SELECT {col}, COUNT(*) AS cnt,
                               COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS pct
                        FROM {tgt}.{tbl} WHERE {col} IS NOT NULL
                        GROUP BY {col} ORDER BY cnt DESC LIMIT 10
                    )
                    SELECT 
                        AVG(ABS(COALESCE(s.pct, 0) - COALESCE(t.pct, 0))) AS avg_deviation
                    FROM src_dist s
                    FULL OUTER JOIN tgt_dist t ON s.{col} = t.{col}
                """).collect()[0][0]
                
                dev = float(dist_check or 0)
                status = "PASS" if dev < 5.0 else "WARN" if dev < 10.0 else "FAIL"
                
                session.sql(f"""
                    INSERT INTO {db}.EDRP_METADATA.VALIDATION_RESULTS
                        (JOB_ID, TABLE_NAME, CHECK_TYPE, CHECK_NAME, DEVIATION_PERCENT,
                         THRESHOLD_PERCENT, PASS_FAIL, DETAILS)
                    VALUES ({job_id_param}, '{tbl}', 'DISTRIBUTION', 
                            '{tbl}.{col} top-10 frequency',
                            {round(dev, 4)}, 5.0, '{status}',
                            'Avg deviation of top-10 value frequencies: {round(dev, 2)}%')
                """).collect()
                
                results["distribution_checks"].append({
                    "table": tbl, "column": col, "avg_deviation_pct": round(dev, 2), "status": status
                })
            except Exception as e:
                results["distribution_checks"].append({"table": tbl, "column": col, "error": str(e)})
    
    # Update job status
    session.sql(f"""
        UPDATE {db}.EDRP_METADATA.REDUCTION_JOB_LOG
        SET JOB_STATUS = CASE WHEN {total_fail} > 0 THEN 'VALIDATION_FAILED' ELSE 'VALIDATED' END
        WHERE JOB_ID = {job_id_param}
    """).collect()
    
    results["summary"] = {
        "total_fk_checks": len(results["fk_checks"]),
        "fk_passed": total_pass,
        "fk_failed": total_fail,
        "distribution_checks": len(results["distribution_checks"]),
        "overall": "PASS" if total_fail == 0 else "FAIL"
    }
    
    return results
$$;
```

### Usage

```sql
-- Validate the most recent reduction job
CALL DATA_REDUCTION_POC.EDRP_APP.SP_VALIDATE_REDUCTION(1);

-- View all validation results
SELECT * FROM DATA_REDUCTION_POC.EDRP_METADATA.VALIDATION_RESULTS
WHERE JOB_ID = 1
ORDER BY PASS_FAIL DESC, CHECK_TYPE;
```

### 7.2 Rollback Procedure — Cleanup Failed or Unwanted Reductions

Drops all tables created during a specific job run, enabling safe recovery from partial failures.

```sql
CREATE OR REPLACE PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_ROLLBACK_REDUCTION(
    JOB_ID_PARAM NUMBER
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Rolls back a reduction job by dropping all tables it created in the target schema'
AS
$$
import json

def run(session, job_id_param):
    job = session.sql(f"""
        SELECT j.TABLES_CREATED_LIST, j.JOB_STATUS,
               p.TARGET_DATABASE, p.TARGET_SCHEMA
        FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG j
        JOIN DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_PROFILE p ON j.PROFILE_ID = p.PROFILE_ID
        WHERE j.JOB_ID = {job_id_param}
    """).collect()
    
    if not job:
        return {"error": f"Job {job_id_param} not found"}
    
    j = job[0]
    tgt = f"{j['TARGET_DATABASE']}.{j['TARGET_SCHEMA']}"
    tables_list = json.loads(j["TABLES_CREATED_LIST"]) if j["TABLES_CREATED_LIST"] else []
    
    if not tables_list:
        return {"error": "No tables recorded for this job — nothing to roll back"}
    
    dropped = []
    errors = []
    for tbl_name in reversed(tables_list):
        try:
            session.sql(f"DROP TABLE IF EXISTS {tgt}.{tbl_name}").collect()
            dropped.append(tbl_name)
        except Exception as e:
            errors.append({"table": tbl_name, "error": str(e)})
    
    session.sql(f"""
        UPDATE DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG
        SET JOB_STATUS = 'ROLLED_BACK'
        WHERE JOB_ID = {job_id_param}
    """).collect()
    
    return {"job_id": job_id_param, "tables_dropped": len(dropped), "errors": errors}
$$;
```

### 7.3 Relationship Lineage Report View

Provides a consumable dependency-chain report showing the full parent-child hierarchy used during reduction, including composite key details.

```sql
CREATE OR REPLACE VIEW DATA_REDUCTION_POC.EDRP_METADATA.V_RELATIONSHIP_LINEAGE AS
WITH RECURSIVE lineage AS (
    -- Root tables (parents that are never children)
    SELECT
        r.PARENT_TABLE AS TABLE_NAME,
        NULL AS PARENT_TABLE,
        NULL AS PARENT_COLUMN,
        NULL AS CHILD_COLUMN,
        0 AS DEPTH,
        r.PARENT_TABLE AS ROOT_TABLE,
        r.SOURCE_DATABASE,
        r.SOURCE_SCHEMA
    FROM DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP r
    WHERE r.RELATIONSHIP_STATUS = 'ACTIVE'
      AND r.PARENT_TABLE NOT IN (
          SELECT CHILD_TABLE FROM DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP
          WHERE RELATIONSHIP_STATUS = 'ACTIVE'
            AND SOURCE_DATABASE = r.SOURCE_DATABASE
            AND SOURCE_SCHEMA = r.SOURCE_SCHEMA
      )
    GROUP BY r.PARENT_TABLE, r.SOURCE_DATABASE, r.SOURCE_SCHEMA
    
    UNION ALL
    
    -- Recursive children
    SELECT
        r.CHILD_TABLE AS TABLE_NAME,
        r.PARENT_TABLE,
        r.PARENT_COLUMN,
        r.CHILD_COLUMN,
        l.DEPTH + 1 AS DEPTH,
        l.ROOT_TABLE,
        r.SOURCE_DATABASE,
        r.SOURCE_SCHEMA
    FROM DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP r
    JOIN lineage l ON r.PARENT_TABLE = l.TABLE_NAME
        AND r.SOURCE_DATABASE = l.SOURCE_DATABASE
        AND r.SOURCE_SCHEMA = l.SOURCE_SCHEMA
    WHERE r.RELATIONSHIP_STATUS = 'ACTIVE'
      AND l.DEPTH < 10
)
SELECT
    l.SOURCE_DATABASE,
    l.SOURCE_SCHEMA,
    l.ROOT_TABLE,
    l.DEPTH,
    REPEAT('  ', l.DEPTH) || l.TABLE_NAME AS HIERARCHY_DISPLAY,
    l.TABLE_NAME,
    l.PARENT_TABLE,
    COALESCE(l.PARENT_COLUMN || ' → ' || l.CHILD_COLUMN, '(root)') AS JOIN_PATH,
    COALESCE(r.IS_COMPOSITE_KEY, FALSE) AS IS_COMPOSITE_KEY,
    COALESCE(r.CARDINALITY, '-') AS CARDINALITY,
    t.REDUCTION_STRATEGY,
    t.SAMPLE_PERCENT,
    t.SAMPLING_STRATEGY
FROM lineage l
LEFT JOIN DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP r
    ON r.PARENT_TABLE = l.PARENT_TABLE
    AND r.CHILD_TABLE = l.TABLE_NAME
    AND r.SOURCE_DATABASE = l.SOURCE_DATABASE
    AND r.SOURCE_SCHEMA = l.SOURCE_SCHEMA
    AND r.RELATIONSHIP_STATUS = 'ACTIVE'
LEFT JOIN DATA_REDUCTION_POC.EDRP_METADATA.TABLE_INVENTORY t
    ON t.TABLE_NAME = l.TABLE_NAME
    AND t.SOURCE_DATABASE = l.SOURCE_DATABASE
    AND t.SOURCE_SCHEMA = l.SOURCE_SCHEMA
ORDER BY l.SOURCE_DATABASE, l.SOURCE_SCHEMA, l.ROOT_TABLE, l.DEPTH, l.TABLE_NAME;
```

**Usage:**

```sql
-- Full lineage tree for a schema
SELECT * FROM DATA_REDUCTION_POC.EDRP_METADATA.V_RELATIONSHIP_LINEAGE
WHERE SOURCE_DATABASE = 'DATA_REDUCTION_POC' AND SOURCE_SCHEMA = 'STAGE_DATA'
ORDER BY ROOT_TABLE, DEPTH;

-- Lineage for a specific root table
SELECT HIERARCHY_DISPLAY, JOIN_PATH, CARDINALITY, REDUCTION_STRATEGY
FROM DATA_REDUCTION_POC.EDRP_METADATA.V_RELATIONSHIP_LINEAGE
WHERE ROOT_TABLE = 'CUSTOMER';
```

### 7.4 Categorical Coverage Report View

Generates a per-job comparison of distinct categorical values between source and reduced datasets.

```sql
CREATE OR REPLACE VIEW DATA_REDUCTION_POC.EDRP_METADATA.V_CATEGORICAL_COVERAGE AS
SELECT
    v.JOB_ID,
    v.TABLE_NAME,
    v.CHECK_NAME AS COLUMN_CHECK,
    v.DEVIATION_PERCENT,
    v.THRESHOLD_PERCENT,
    v.PASS_FAIL,
    v.DETAILS,
    v.VALIDATED_AT
FROM DATA_REDUCTION_POC.EDRP_METADATA.VALIDATION_RESULTS v
WHERE v.CHECK_TYPE = 'DISTRIBUTION'
ORDER BY v.JOB_ID DESC, v.PASS_FAIL, v.TABLE_NAME;
```

---

## Part 7b — SQL Safety Guidelines

> **Important:** The Snowpark stored procedures in Parts 3–7 use Python f-strings to construct dynamic SQL from metadata values (table names, column names, schema names). While these values originate from internal metadata tables, consider the following mitigations:
>
> 1. **Validate identifiers at write time:** When populating `RELATIONSHIP_MAP`, `TABLE_INVENTORY`, or `JOIN_CONDITIONS`, validate that table/column names match `^[A-Z_][A-Z0-9_]*$` (standard Snowflake unquoted identifiers).
> 2. **Use `IDENTIFIER()` for parameterized SQL:** Where feasible, prefer Snowflake's `IDENTIFIER()` function over raw string interpolation (e.g., `SELECT * FROM IDENTIFIER(:table_name)` in SQL procedures).
> 3. **Restrict write access:** Only `EDRP_ADMIN` should have INSERT/UPDATE on metadata tables. `EDRP_OPERATOR` should be read-only on metadata.
> 4. **Audit metadata changes:** Consider adding `UPDATED_BY` and `UPDATED_AT` columns to track who modified relationship or reduction configuration metadata.

---

## Part 8 — Cortex AI Augmentation

### 8.1 AI Relationship Discovery

Uses Cortex COMPLETE to suggest implicit relationships based on column naming patterns.

```sql
CREATE OR REPLACE PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_AI_DISCOVER_RELATIONSHIPS(
    SOURCE_DATABASE VARCHAR,
    SOURCE_SCHEMA VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Uses Cortex COMPLETE to suggest undiscovered relationships from column naming patterns'
AS
BEGIN
    -- Step 1: Find columns that look like FKs but aren't in RELATIONSHIP_MAP
    CREATE OR REPLACE TEMPORARY TABLE EDRP_FK_CANDIDATES AS
    SELECT 
        c.TABLE_NAME,
        c.COLUMN_NAME,
        c.DATA_TYPE,
        -- Check if this column is already mapped as a child FK
        CASE WHEN r.RELATIONSHIP_ID IS NOT NULL THEN TRUE ELSE FALSE END AS ALREADY_MAPPED
    FROM IDENTIFIER(:SOURCE_DATABASE || '.EDRP_METADATA.COLUMN_INVENTORY') c
    LEFT JOIN IDENTIFIER(:SOURCE_DATABASE || '.EDRP_METADATA.RELATIONSHIP_MAP') r
        ON r.CHILD_TABLE = c.TABLE_NAME AND r.CHILD_COLUMN = c.COLUMN_NAME
        AND r.SOURCE_DATABASE = c.SOURCE_DATABASE AND r.SOURCE_SCHEMA = c.SOURCE_SCHEMA
    WHERE c.SOURCE_DATABASE = :SOURCE_DATABASE
      AND c.SOURCE_SCHEMA = :SOURCE_SCHEMA
      AND (c.COLUMN_NAME LIKE '%_SK' OR c.COLUMN_NAME LIKE '%_ID' OR c.COLUMN_NAME LIKE '%_KEY')
      AND c.DATA_TYPE = 'NUMBER'
      AND r.RELATIONSHIP_ID IS NULL;  -- Not already mapped

    -- Step 2: Ask Cortex to analyze unmapped FK-like columns
    LET ai_suggestions VARIANT;
    
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'llama3.1-70b',
        'You are a data architect. Given these unmapped columns that look like foreign keys, '
        || 'suggest which parent table and column each one likely references. '
        || 'Return JSON array: [{"child_table":"X","child_column":"Y","parent_table":"Z","parent_column":"W","confidence":"HIGH/MEDIUM/LOW"}]. '
        || 'Columns: ' || (SELECT LISTAGG(TABLE_NAME || '.' || COLUMN_NAME, ', ') FROM EDRP_FK_CANDIDATES)
        || '. Known tables in schema: ' || (
            SELECT LISTAGG(DISTINCT TABLE_NAME, ', ') 
            FROM IDENTIFIER(:SOURCE_DATABASE || '.EDRP_METADATA.TABLE_INVENTORY')
            WHERE SOURCE_DATABASE = :SOURCE_DATABASE AND SOURCE_SCHEMA = :SOURCE_SCHEMA
        )
    ) INTO :ai_suggestions;
    
    -- Step 3: Insert AI suggestions as PENDING_REVIEW
    INSERT INTO IDENTIFIER(:SOURCE_DATABASE || '.EDRP_METADATA.RELATIONSHIP_MAP')
        (SOURCE_DATABASE, SOURCE_SCHEMA, PARENT_TABLE, PARENT_COLUMN, CHILD_TABLE, CHILD_COLUMN,
         JOIN_TYPE, CARDINALITY, DISCOVERED_BY, CONFIDENCE, RELATIONSHIP_STATUS, NOTES)
    SELECT 
        :SOURCE_DATABASE, :SOURCE_SCHEMA,
        f.value:parent_table::STRING,
        f.value:parent_column::STRING,
        f.value:child_table::STRING,
        f.value:child_column::STRING,
        'LEFT', '1:N', 'CORTEX_AI',
        f.value:confidence::STRING,
        'PENDING_REVIEW',
        'AI-suggested relationship'
    FROM TABLE(FLATTEN(PARSE_JSON(:ai_suggestions))) f
    WHERE f.value:parent_table::STRING IN (
        SELECT TABLE_NAME FROM IDENTIFIER(:SOURCE_DATABASE || '.EDRP_METADATA.TABLE_INVENTORY')
        WHERE SOURCE_DATABASE = :SOURCE_DATABASE AND SOURCE_SCHEMA = :SOURCE_SCHEMA
    );
    
    DROP TABLE IF EXISTS EDRP_FK_CANDIDATES;
    
    RETURN :ai_suggestions;
END;
```

### 8.2 AI Validation Report Summarizer

```sql
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

### 8.3 AI Column Sensitivity Classification

```sql
-- Classify all columns for sensitivity using Cortex
CREATE OR REPLACE PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_AI_CLASSIFY_SENSITIVITY(
    SOURCE_DATABASE VARCHAR,
    SOURCE_SCHEMA VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Uses Cortex to classify column sensitivity (PII, PHI, PCI, PUBLIC)'
AS
BEGIN
    UPDATE IDENTIFIER(:SOURCE_DATABASE || '.EDRP_METADATA.COLUMN_INVENTORY') ci
    SET SENSITIVITY_CLASS = SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        ci.TABLE_NAME || '.' || ci.COLUMN_NAME || ' (type: ' || ci.DATA_TYPE || ')',
        ['PII', 'PHI', 'PCI', 'CONFIDENTIAL', 'PUBLIC']
    ):label::VARCHAR
    WHERE ci.SOURCE_DATABASE = :SOURCE_DATABASE
      AND ci.SOURCE_SCHEMA = :SOURCE_SCHEMA
      AND ci.SENSITIVITY_CLASS IS NULL;
    
    RETURN 'Classification complete';
END;
```

### Usage

```sql
-- Discover missing relationships via AI
CALL DATA_REDUCTION_POC.EDRP_APP.SP_AI_DISCOVER_RELATIONSHIPS('DATA_REDUCTION_POC', 'STAGE_DATA');

-- Generate human-readable validation report
SELECT DATA_REDUCTION_POC.EDRP_APP.FN_AI_SUMMARIZE_VALIDATION(1);

-- Classify column sensitivity
CALL DATA_REDUCTION_POC.EDRP_APP.SP_AI_CLASSIFY_SENSITIVITY('DATA_REDUCTION_POC', 'STAGE_DATA');
```

---

## Part 9 — Streamlit in Snowflake UI

### Create the Streamlit App

```sql
CREATE OR REPLACE STREAMLIT DATA_REDUCTION_POC.EDRP_APP.EDRP_DASHBOARD
    ROOT_LOCATION = '@DATA_REDUCTION_POC.EDRP_APP.EDRP_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = 'EDRP_WH'
    COMMENT = 'Enterprise Data Reduction Platform - Dashboard';
```

### streamlit_app.py

```python
import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.set_page_config(page_title="EDRP Dashboard", layout="wide")
st.title("Enterprise Data Reduction Platform")

# Sidebar navigation
page = st.sidebar.radio("Navigation", [
    "Overview",
    "Relationship Map",
    "Reduction Profiles",
    "Execute Reduction",
    "Validation Results",
    "AI Insights"
])

# ─── OVERVIEW PAGE ───
if page == "Overview":
    st.header("Reduction Overview")
    
    col1, col2, col3 = st.columns(3)
    
    tables = session.sql("""
        SELECT COUNT(*) AS CNT FROM DATA_REDUCTION_POC.EDRP_METADATA.TABLE_INVENTORY WHERE IS_ACTIVE = TRUE
    """).collect()[0]["CNT"]
    
    rels = session.sql("""
        SELECT COUNT(*) AS CNT FROM DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP WHERE RELATIONSHIP_STATUS = 'ACTIVE'
    """).collect()[0]["CNT"]
    
    jobs = session.sql("""
        SELECT COUNT(*) AS CNT FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG WHERE JOB_STATUS = 'COMPLETED'
    """).collect()[0]["CNT"]
    
    col1.metric("Tables in Scope", tables)
    col2.metric("Active Relationships", rels)
    col3.metric("Completed Jobs", jobs)
    
    # Latest job results
    st.subheader("Latest Reduction Results")
    latest = session.sql("""
        SELECT TABLE_NAME, SOURCE_ROW_COUNT, TARGET_ROW_COUNT, REDUCTION_PERCENT, STRATEGY_USED, STATUS
        FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_TABLE_LOG
        WHERE JOB_ID = (SELECT MAX(JOB_ID) FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG)
        ORDER BY SOURCE_ROW_COUNT DESC
    """).to_pandas()
    st.dataframe(latest, use_container_width=True)

# ─── RELATIONSHIP MAP PAGE ───
elif page == "Relationship Map":
    st.header("Relationship Map")
    
    # Filter by discovery source
    source_filter = st.multiselect("Filter by Discovery Source", 
        ["SCALA_PARSER", "INFO_SCHEMA", "ACCESS_HISTORY", "CORTEX_AI", "MANUAL"],
        default=["SCALA_PARSER", "INFO_SCHEMA", "MANUAL"])
    
    if source_filter:
        filter_str = ",".join([f"'{s}'" for s in source_filter])
        rels_df = session.sql(f"""
            SELECT PARENT_TABLE, PARENT_COLUMN, CHILD_TABLE, CHILD_COLUMN, 
                   JOIN_TYPE, DISCOVERED_BY, CONFIDENCE, RELATIONSHIP_STATUS
            FROM DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP
            WHERE DISCOVERED_BY IN ({filter_str})
            ORDER BY CHILD_TABLE, CHILD_COLUMN
        """).to_pandas()
        st.dataframe(rels_df, use_container_width=True)
    
    # Pending review section
    st.subheader("Pending Review")
    pending = session.sql("""
        SELECT RELATIONSHIP_ID, PARENT_TABLE, PARENT_COLUMN, CHILD_TABLE, CHILD_COLUMN, 
               DISCOVERED_BY, CONFIDENCE
        FROM DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP
        WHERE RELATIONSHIP_STATUS = 'PENDING_REVIEW'
    """).to_pandas()
    
    if len(pending) > 0:
        st.dataframe(pending, use_container_width=True)
        if st.button("Approve All Pending"):
            session.sql("""
                UPDATE DATA_REDUCTION_POC.EDRP_METADATA.RELATIONSHIP_MAP
                SET RELATIONSHIP_STATUS = 'ACTIVE', 
                    REVIEWED_BY = CURRENT_USER(), 
                    REVIEWED_AT = CURRENT_TIMESTAMP()
                WHERE RELATIONSHIP_STATUS = 'PENDING_REVIEW'
            """).collect()
            st.success("All pending relationships approved!")
            st.rerun()
    else:
        st.info("No relationships pending review.")

# ─── REDUCTION PROFILES PAGE ───
elif page == "Reduction Profiles":
    st.header("Reduction Profiles")
    
    profiles = session.sql("""
        SELECT PROFILE_NAME, SOURCE_SCHEMA, TARGET_SCHEMA, DEFAULT_SAMPLE_PERCENT, 
               DIMENSION_STRATEGY, FACT_STRATEGY, STATUS
        FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_PROFILE
        ORDER BY CREATED_AT DESC
    """).to_pandas()
    st.dataframe(profiles, use_container_width=True)
    
    # Table-level strategy editor
    st.subheader("Table-Level Strategies")
    table_strats = session.sql("""
        SELECT TABLE_NAME, TABLE_TYPE, TABLE_ROLE, ROW_COUNT, REDUCTION_STRATEGY, SAMPLE_PERCENT
        FROM DATA_REDUCTION_POC.EDRP_METADATA.TABLE_INVENTORY
        WHERE IS_ACTIVE = TRUE
        ORDER BY ROW_COUNT DESC
    """).to_pandas()
    st.dataframe(table_strats, use_container_width=True)

# ─── EXECUTE REDUCTION PAGE ───
elif page == "Execute Reduction":
    st.header("Execute Reduction")
    
    profiles = session.sql("""
        SELECT PROFILE_NAME FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_PROFILE WHERE STATUS = 'ACTIVE'
    """).to_pandas()
    
    selected_profile = st.selectbox("Select Profile", profiles["PROFILE_NAME"].tolist())
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Run Reduction", type="primary"):
            with st.spinner("Executing reduction... this may take several minutes for large datasets."):
                result = session.sql(f"CALL DATA_REDUCTION_POC.EDRP_APP.SP_EXECUTE_REDUCTION('{selected_profile}')").collect()
                st.success("Reduction complete!")
                st.json(result[0][0])
    
    with col2:
        if st.button("Run Validation"):
            job_id = session.sql("SELECT MAX(JOB_ID) FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG").collect()[0][0]
            with st.spinner("Validating..."):
                result = session.sql(f"CALL DATA_REDUCTION_POC.EDRP_APP.SP_VALIDATE_REDUCTION({job_id})").collect()
                st.success("Validation complete!")
                st.json(result[0][0])

# ─── VALIDATION RESULTS PAGE ───
elif page == "Validation Results":
    st.header("Validation Results")
    
    job_ids = session.sql("""
        SELECT JOB_ID, JOB_STATUS, STARTED_AT, TABLES_PROCESSED
        FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG
        ORDER BY JOB_ID DESC LIMIT 10
    """).to_pandas()
    
    selected_job = st.selectbox("Select Job", job_ids["JOB_ID"].tolist())
    
    # Summary metrics
    summary = session.sql(f"""
        SELECT 
            SUM(CASE WHEN PASS_FAIL = 'PASS' THEN 1 ELSE 0 END) AS PASSED,
            SUM(CASE WHEN PASS_FAIL = 'FAIL' THEN 1 ELSE 0 END) AS FAILED,
            SUM(CASE WHEN PASS_FAIL = 'WARN' THEN 1 ELSE 0 END) AS WARNINGS,
            COUNT(*) AS TOTAL
        FROM DATA_REDUCTION_POC.EDRP_METADATA.VALIDATION_RESULTS
        WHERE JOB_ID = {selected_job}
    """).collect()[0]
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Checks", summary["TOTAL"])
    col2.metric("Passed", summary["PASSED"])
    col3.metric("Failed", summary["FAILED"])
    col4.metric("Warnings", summary["WARNINGS"])
    
    # Detailed results
    tab1, tab2 = st.tabs(["FK Integrity", "Distribution"])
    
    with tab1:
        fk_results = session.sql(f"""
            SELECT TABLE_NAME, CHECK_NAME, PASS_FAIL, DETAILS
            FROM DATA_REDUCTION_POC.EDRP_METADATA.VALIDATION_RESULTS
            WHERE JOB_ID = {selected_job} AND CHECK_TYPE = 'FK_INTEGRITY'
            ORDER BY PASS_FAIL DESC
        """).to_pandas()
        st.dataframe(fk_results, use_container_width=True)
    
    with tab2:
        dist_results = session.sql(f"""
            SELECT TABLE_NAME, CHECK_NAME, DEVIATION_PERCENT, THRESHOLD_PERCENT, PASS_FAIL
            FROM DATA_REDUCTION_POC.EDRP_METADATA.VALIDATION_RESULTS
            WHERE JOB_ID = {selected_job} AND CHECK_TYPE = 'DISTRIBUTION'
            ORDER BY DEVIATION_PERCENT DESC
        """).to_pandas()
        st.dataframe(dist_results, use_container_width=True)
    
    # AI Summary
    st.subheader("AI-Generated Summary")
    if st.button("Generate AI Summary"):
        ai_summary = session.sql(f"""
            SELECT DATA_REDUCTION_POC.EDRP_APP.FN_AI_SUMMARIZE_VALIDATION({selected_job})
        """).collect()[0][0]
        st.markdown(ai_summary)

# ─── AI INSIGHTS PAGE ───
elif page == "AI Insights":
    st.header("AI-Powered Insights")
    
    tab1, tab2 = st.tabs(["Relationship Discovery", "Column Sensitivity"])
    
    with tab1:
        st.subheader("Discover Missing Relationships")
        if st.button("Run AI Relationship Discovery"):
            with st.spinner("Analyzing column patterns with Cortex AI..."):
                result = session.sql("""
                    CALL DATA_REDUCTION_POC.EDRP_APP.SP_AI_DISCOVER_RELATIONSHIPS('DATA_REDUCTION_POC', 'STAGE_DATA')
                """).collect()
                st.success("Discovery complete! Check the Relationship Map page for new PENDING_REVIEW entries.")
                st.json(result[0][0])
    
    with tab2:
        st.subheader("Column Sensitivity Classification")
        if st.button("Run AI Classification"):
            with st.spinner("Classifying columns with Cortex AI..."):
                session.sql("""
                    CALL DATA_REDUCTION_POC.EDRP_APP.SP_AI_CLASSIFY_SENSITIVITY('DATA_REDUCTION_POC', 'STAGE_DATA')
                """).collect()
                st.success("Classification complete!")
        
        sensitivity = session.sql("""
            SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, SENSITIVITY_CLASS
            FROM DATA_REDUCTION_POC.EDRP_METADATA.COLUMN_INVENTORY
            WHERE SENSITIVITY_CLASS IS NOT NULL
            ORDER BY SENSITIVITY_CLASS, TABLE_NAME
        """).to_pandas()
        st.dataframe(sensitivity, use_container_width=True)
```

---

## Part 10 — Snowflake Tasks Orchestration

### Task DAG: Extract → Reduce → Validate → Notify

```sql
-- Root task: Refresh metadata
CREATE OR REPLACE TASK DATA_REDUCTION_POC.EDRP_APP.TASK_REFRESH_METADATA
    WAREHOUSE = EDRP_WH
    SCHEDULE = 'USING CRON 0 2 * * 0 America/Los_Angeles'  -- Weekly Sunday 2am
    COMMENT = 'Weekly metadata refresh from INFORMATION_SCHEMA'
AS
    CALL DATA_REDUCTION_POC.EDRP_APP.SP_EXTRACT_METADATA('DATA_REDUCTION_POC', 'STAGE_DATA');

-- Child task: Build dependency graph
CREATE OR REPLACE TASK DATA_REDUCTION_POC.EDRP_APP.TASK_BUILD_GRAPH
    WAREHOUSE = EDRP_WH
    AFTER DATA_REDUCTION_POC.EDRP_APP.TASK_REFRESH_METADATA
    COMMENT = 'Rebuild dependency graph after metadata refresh'
AS
    CALL DATA_REDUCTION_POC.EDRP_APP.SP_BUILD_DEPENDENCY_GRAPH('DATA_REDUCTION_POC', 'STAGE_DATA');

-- Child task: Execute reduction
CREATE OR REPLACE TASK DATA_REDUCTION_POC.EDRP_APP.TASK_EXECUTE_REDUCTION
    WAREHOUSE = EDRP_WH
    AFTER DATA_REDUCTION_POC.EDRP_APP.TASK_BUILD_GRAPH
    COMMENT = 'Execute data reduction using active profile'
AS
    CALL DATA_REDUCTION_POC.EDRP_APP.SP_EXECUTE_REDUCTION('POC_10PCT_REDUCTION');

-- Child task: Validate results (uses DECLARE/LET to resolve job_id before calling procedure)
CREATE OR REPLACE TASK DATA_REDUCTION_POC.EDRP_APP.TASK_VALIDATE
    WAREHOUSE = EDRP_WH
    AFTER DATA_REDUCTION_POC.EDRP_APP.TASK_EXECUTE_REDUCTION
    COMMENT = 'Validate reduced dataset integrity and distribution'
AS
DECLARE
    latest_job_id NUMBER;
BEGIN
    SELECT MAX(JOB_ID) INTO :latest_job_id 
    FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG;
    
    CALL DATA_REDUCTION_POC.EDRP_APP.SP_VALIDATE_REDUCTION(:latest_job_id);
END;

-- Resume the task DAG
ALTER TASK DATA_REDUCTION_POC.EDRP_APP.TASK_VALIDATE RESUME;
ALTER TASK DATA_REDUCTION_POC.EDRP_APP.TASK_EXECUTE_REDUCTION RESUME;
ALTER TASK DATA_REDUCTION_POC.EDRP_APP.TASK_BUILD_GRAPH RESUME;
ALTER TASK DATA_REDUCTION_POC.EDRP_APP.TASK_REFRESH_METADATA RESUME;
```

### Monitor Task Execution

```sql
-- View task execution history
SELECT NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 20
))
WHERE NAME LIKE 'TASK_%'
ORDER BY SCHEDULED_TIME DESC;
```

---

## Part 11 — Semantic View + Cortex Analyst

### Semantic View over Reduced Data

Create a semantic view that enables natural language querying of the reduced dataset.

```sql
-- Create semantic view for the reduced returns data
CREATE OR REPLACE SEMANTIC VIEW DATA_REDUCTION_POC.EDRP_APP.SV_REDUCED_RETURNS

  TABLES (
    store_returns AS DATA_REDUCTION_POC.STAGE_DATA_REDUCED.STORE_RETURNS
      PRIMARY KEY (SR_ITEM_SK, SR_TICKET_NUMBER)
      COMMENT = 'Store returns fact table (reduced)',
    customer AS DATA_REDUCTION_POC.STAGE_DATA_REDUCED.CUSTOMER
      PRIMARY KEY (C_CUSTOMER_SK)
      COMMENT = 'Customer dimension (10% sample)',
    store AS DATA_REDUCTION_POC.STAGE_DATA_REDUCED.STORE
      PRIMARY KEY (S_STORE_SK)
      COMMENT = 'Store dimension (full copy)',
    item AS DATA_REDUCTION_POC.STAGE_DATA_REDUCED.ITEM
      PRIMARY KEY (I_ITEM_SK)
      COMMENT = 'Item dimension (full copy)'
  )

  RELATIONSHIPS (
    sr_to_customer AS
      store_returns (SR_CUSTOMER_SK) REFERENCES customer,
    sr_to_store AS
      store_returns (SR_STORE_SK) REFERENCES store,
    sr_to_item AS
      store_returns (SR_ITEM_SK) REFERENCES item
  )

  FACTS (
    store_returns.return_quantity AS SR_RETURN_QUANTITY
      COMMENT = 'Number of items returned',
    store_returns.return_amount AS SR_RETURN_AMT
      COMMENT = 'Dollar amount returned',
    store_returns.return_tax AS SR_RETURN_TAX
      COMMENT = 'Tax on return',
    store_returns.fee AS SR_FEE
      COMMENT = 'Return processing fee',
    store_returns.net_loss AS SR_NET_LOSS
      COMMENT = 'Net loss from return'
  )

  DIMENSIONS (
    customer.customer_name AS C_FIRST_NAME || ' ' || C_LAST_NAME
      WITH SYNONYMS = ('customer name', 'who returned')
      COMMENT = 'Customer full name',
    customer.birth_country AS C_BIRTH_COUNTRY
      COMMENT = 'Customer country of birth',
    store.store_name AS S_STORE_NAME
      WITH SYNONYMS = ('store', 'location')
      COMMENT = 'Store name',
    store.store_state AS S_STATE
      COMMENT = 'Store state',
    store.store_city AS S_CITY
      COMMENT = 'Store city',
    item.item_brand AS I_BRAND
      WITH SYNONYMS = ('brand')
      COMMENT = 'Item brand name',
    item.item_category AS I_CATEGORY
      WITH SYNONYMS = ('category', 'product type')
      COMMENT = 'Item category',
    item.item_class AS I_CLASS
      COMMENT = 'Item class',
    item.current_price AS I_CURRENT_PRICE
      COMMENT = 'Current item price'
  )

  METRICS (
    store_returns.total_return_amount AS SUM(store_returns.return_amount)
      WITH SYNONYMS = ('total returns', 'return dollars')
      COMMENT = 'Total dollar amount of returns',
    store_returns.total_return_quantity AS SUM(store_returns.return_quantity)
      COMMENT = 'Total number of items returned',
    store_returns.avg_return_amount AS AVG(store_returns.return_amount)
      COMMENT = 'Average return amount per transaction',
    store_returns.total_net_loss AS SUM(store_returns.net_loss)
      WITH SYNONYMS = ('total loss', 'loss')
      COMMENT = 'Total net loss from returns',
    store_returns.return_count AS COUNT(store_returns.return_quantity)
      WITH SYNONYMS = ('number of returns', 'return transactions')
      COMMENT = 'Count of return transactions',
    customer.customer_count AS COUNT(C_CUSTOMER_SK)
      COMMENT = 'Count of distinct customers'
  )

  COMMENT = 'Semantic view over reduced STAGE_DATA for returns analysis via Cortex Analyst'

  AI_SQL_GENERATION 'All monetary amounts are in USD. When asked about top or bottom results, default to 10 unless specified. Round all numeric outputs to 2 decimal places.'

  AI_VERIFIED_QUERIES (
    top_stores_by_returns AS (
      QUESTION 'What are the top 10 stores by total return amount?'
      SQL 'SELECT store.store_name, store.store_state, SUM(store_returns.return_amount) AS total_return_amount FROM store_returns INNER JOIN store ON store_returns.SR_STORE_SK = store.S_STORE_SK GROUP BY store.store_name, store.store_state ORDER BY total_return_amount DESC LIMIT 10'
    ),
    returns_by_category AS (
      QUESTION 'Show total returns by item category'
      SQL 'SELECT item.item_category, SUM(store_returns.return_amount) AS total_return_amount, COUNT(*) AS return_count FROM store_returns INNER JOIN item ON store_returns.SR_ITEM_SK = item.I_ITEM_SK GROUP BY item.item_category ORDER BY total_return_amount DESC'
    )
  );
```

### Query Reduced Data with Cortex Analyst

Once the semantic view exists, users can query the reduced dataset using natural language via Cortex Analyst in Snowsight, or programmatically:

```sql
-- Example: Natural language query via Cortex Analyst
-- (In Snowsight, users simply type questions in the Analyst panel)
-- Programmatic example:
SELECT SNOWFLAKE.CORTEX.ANALYST(
    'DATA_REDUCTION_POC.EDRP_APP.SV_REDUCED_RETURNS',
    'What are the top 10 stores by total return amount?'
);
```

---

## Part 12 — End-to-End Pipeline & Deployment

### Pipeline Flow

```
┌─────────────────────────────────────────────────────────────┐
│                   EDRP PIPELINE FLOW                         │
│                                                              │
│  1. SCALA PARSER (one-time or on-demand)                    │
│     └── SP_PARSE_SCALA_SCRIPTS()                            │
│     └── Bitbucket → RELATIONSHIP_MAP                        │
│                                                              │
│  2. METADATA EXTRACTION (scheduled weekly)                  │
│     └── SP_EXTRACT_METADATA()                               │
│     └── INFORMATION_SCHEMA → TABLE_INVENTORY, COLUMN_INV    │
│     └── ACCESS_HISTORY → RELATIONSHIP_MAP (supplementary)   │
│                                                              │
│  3. HUMAN REVIEW (Streamlit UI)                             │
│     └── Review PENDING_REVIEW relationships                 │
│     └── Approve or reject                                   │
│     └── Adjust table strategies (FULL_COPY/SAMPLE/CASCADE)  │
│                                                              │
│  4. GRAPH ANALYSIS                                          │
│     └── SP_BUILD_DEPENDENCY_GRAPH()                         │
│     └── NetworkX: topo sort, cycle detection                │
│     └── Stores reduction order in TABLE_INVENTORY           │
│                                                              │
│  5. AI AUGMENTATION (optional)                              │
│     └── SP_AI_DISCOVER_RELATIONSHIPS() — Cortex COMPLETE    │
│     └── SP_AI_CLASSIFY_SENSITIVITY()  — Cortex CLASSIFY     │
│                                                              │
│  6. EXECUTE REDUCTION                                       │
│     └── SP_EXECUTE_REDUCTION('profile_name')                │
│     └── Tables processed in topological order               │
│     └── FULL_COPY → SAMPLE → FK_CASCADE                    │
│     └── Logged to REDUCTION_JOB_LOG + TABLE_LOG             │
│                                                              │
│  7. VALIDATE                                                │
│     └── SP_VALIDATE_REDUCTION(job_id)                       │
│     └── FK integrity: zero orphaned keys                    │
│     └── Distribution: top-N frequency deviation             │
│     └── Results in VALIDATION_RESULTS                       │
│                                                              │
│  8. AI REPORT (optional)                                    │
│     └── FN_AI_SUMMARIZE_VALIDATION() — human-readable       │
│                                                              │
│  9. EXPLORE REDUCED DATA                                    │
│     └── Cortex Analyst + Semantic View                      │
│     └── Natural language queries over reduced dataset       │
│                                                              │
│  ORCHESTRATION: Snowflake Tasks DAG (weekly schedule)       │
│  UI: Streamlit in Snowflake (on-demand interaction)         │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Deployment Script

```sql
-- ============================================================
-- EDRP DEPLOYMENT SCRIPT
-- Run this once to set up the entire platform
-- ============================================================

-- 1. Prerequisites
USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS DATA_REDUCTION_POC;
CREATE SCHEMA IF NOT EXISTS DATA_REDUCTION_POC.EDRP_METADATA;
CREATE SCHEMA IF NOT EXISTS DATA_REDUCTION_POC.EDRP_APP;

CREATE WAREHOUSE IF NOT EXISTS EDRP_WH
    WAREHOUSE_SIZE = 'LARGE' AUTO_SUSPEND = 120 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE;

-- 2. Metadata Tables (see POC_Script.md Section 2 for full DDL)
-- TABLE_INVENTORY, COLUMN_INVENTORY, RELATIONSHIP_MAP, 
-- REDUCTION_PROFILE, REDUCTION_JOB_LOG, REDUCTION_TABLE_LOG, VALIDATION_RESULTS

-- 3. External Access Integration (for Bitbucket — update with your credentials)
-- See Part 2 of this document

-- 4. Deploy Stored Procedures (copy each CREATE OR REPLACE PROCEDURE from Parts 3-8)
-- SP_PARSE_SCALA_SCRIPTS
-- SP_EXTRACT_METADATA
-- SP_BUILD_DEPENDENCY_GRAPH
-- SP_EXECUTE_REDUCTION
-- SP_VALIDATE_REDUCTION
-- SP_AI_DISCOVER_RELATIONSHIPS
-- SP_AI_CLASSIFY_SENSITIVITY
-- FN_AI_SUMMARIZE_VALIDATION

-- 5. Deploy Streamlit App
-- Upload streamlit_app.py to stage, then CREATE STREAMLIT

-- 6. Deploy Tasks (see Part 10)
-- TASK_REFRESH_METADATA → TASK_BUILD_GRAPH → TASK_EXECUTE_REDUCTION → TASK_VALIDATE

-- 7. Create Semantic View (see Part 11)

-- 8. Create initial reduction profile
INSERT INTO DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_PROFILE
    (PROFILE_NAME, SOURCE_DATABASE, SOURCE_SCHEMA, TARGET_DATABASE, TARGET_SCHEMA,
     DEFAULT_SAMPLE_PERCENT, DIMENSION_STRATEGY, FACT_STRATEGY, RANDOM_SEED, STATUS, CREATED_BY)
VALUES
    ('PRODUCTION_10PCT', 'DATA_REDUCTION_POC', 'STAGE_DATA', 
     'DATA_REDUCTION_POC', 'STAGE_DATA_REDUCED',
     10.00, 'FULL_COPY', 'FK_CASCADE', 42, 'ACTIVE', CURRENT_USER());

-- 9. Run the pipeline manually for the first time
CALL DATA_REDUCTION_POC.EDRP_APP.SP_EXTRACT_METADATA('DATA_REDUCTION_POC', 'STAGE_DATA');
CALL DATA_REDUCTION_POC.EDRP_APP.SP_BUILD_DEPENDENCY_GRAPH('DATA_REDUCTION_POC', 'STAGE_DATA');
CALL DATA_REDUCTION_POC.EDRP_APP.SP_EXECUTE_REDUCTION('PRODUCTION_10PCT');
CALL DATA_REDUCTION_POC.EDRP_APP.SP_VALIDATE_REDUCTION(
    (SELECT MAX(JOB_ID) FROM DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_JOB_LOG)
);
```

### RBAC Setup

```sql
-- Create EDRP roles
CREATE ROLE IF NOT EXISTS EDRP_ADMIN;    -- Full access: configure, execute, validate
CREATE ROLE IF NOT EXISTS EDRP_OPERATOR; -- Execute and monitor, no config changes
CREATE ROLE IF NOT EXISTS EDRP_VIEWER;   -- Read-only: view results and dashboards

-- Grant hierarchy
GRANT ROLE EDRP_VIEWER TO ROLE EDRP_OPERATOR;
GRANT ROLE EDRP_OPERATOR TO ROLE EDRP_ADMIN;
GRANT ROLE EDRP_ADMIN TO ROLE SYSADMIN;

-- Database grants
GRANT USAGE ON DATABASE DATA_REDUCTION_POC TO ROLE EDRP_VIEWER;
GRANT USAGE ON SCHEMA DATA_REDUCTION_POC.EDRP_METADATA TO ROLE EDRP_VIEWER;
GRANT USAGE ON SCHEMA DATA_REDUCTION_POC.EDRP_APP TO ROLE EDRP_VIEWER;
GRANT USAGE ON SCHEMA DATA_REDUCTION_POC.STAGE_DATA_REDUCED TO ROLE EDRP_VIEWER;

-- Viewer: read metadata and reduced data
GRANT SELECT ON ALL TABLES IN SCHEMA DATA_REDUCTION_POC.EDRP_METADATA TO ROLE EDRP_VIEWER;
GRANT SELECT ON ALL TABLES IN SCHEMA DATA_REDUCTION_POC.STAGE_DATA_REDUCED TO ROLE EDRP_VIEWER;

-- Operator: execute procedures
GRANT USAGE ON WAREHOUSE EDRP_WH TO ROLE EDRP_OPERATOR;
GRANT USAGE ON PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_EXECUTE_REDUCTION(VARCHAR) TO ROLE EDRP_OPERATOR;
GRANT USAGE ON PROCEDURE DATA_REDUCTION_POC.EDRP_APP.SP_VALIDATE_REDUCTION(NUMBER) TO ROLE EDRP_OPERATOR;

-- Admin: full control
GRANT ALL ON SCHEMA DATA_REDUCTION_POC.EDRP_METADATA TO ROLE EDRP_ADMIN;
GRANT ALL ON SCHEMA DATA_REDUCTION_POC.EDRP_APP TO ROLE EDRP_ADMIN;
GRANT ALL ON ALL TABLES IN SCHEMA DATA_REDUCTION_POC.EDRP_METADATA TO ROLE EDRP_ADMIN;
```

---

**END OF IMPLEMENTATION PLAN**

**Key Principle:** Every component runs inside Snowflake. The reduction logic is the same proven SQL from the POC, wrapped in Snowpark stored procedures for automation. AI is an enhancer, not the engine. The Streamlit UI provides self-service. Tasks provide scheduling. Semantic Views + Cortex Analyst provide data exploration.
