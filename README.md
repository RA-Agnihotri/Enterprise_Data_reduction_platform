# EDRP — Enterprise Data Reduction Platform

## Deployment Package

Metadata-driven Snowflake framework that generates reduced-volume datasets while preserving referential integrity, business relationships, and categorical data coverage.

**Proven on:** TPC-DS 100TB (52B rows → 3.7B rows, 59/60 FK checks PASS, 12/12 distribution checks PASS)

---

## Quick Start

Run the SQL scripts **in order** against your Snowflake account:

| Step | File | What It Does | Run As |
|------|------|-------------|--------|
| 1 | `01_prerequisites.sql` | Creates database, schemas, warehouse | ACCOUNTADMIN |
| 2 | `02_metadata_tables.sql` | Creates 8 metadata tables | ACCOUNTADMIN |
| 3 | `03_stored_procedures.sql` | Deploys 6 stored procedures + 1 function | ACCOUNTADMIN |
| 4 | `04_views.sql` | Creates lineage and coverage views | ACCOUNTADMIN |
| 5 | `05_rbac.sql` | Creates roles (ADMIN/OPERATOR/VIEWER) | SECURITYADMIN |
| 6 | `06_initial_config.sql` | Creates first reduction profile | EDRP_ADMIN |
| 7 | `07_run_pipeline.sql` | Executes the full pipeline | EDRP_OPERATOR |

After deployment, use `08_validation_queries.sql` to validate results.

---

## Architecture

```
EDRP_METADATA (config)          SOURCE_SCHEMA          TARGET_SCHEMA
├── TABLE_INVENTORY              ├── CUSTOMER    ──→    ├── CUSTOMER (10%)
├── COLUMN_INVENTORY             ├── ITEM        ──→    ├── ITEM (100%)
├── RELATIONSHIP_MAP             ├── STORE_RETURNS ──→  ├── STORE_RETURNS (FK cascade)
├── JOIN_CONDITIONS              └── ...                └── ...
├── REDUCTION_PROFILE
├── REDUCTION_JOB_LOG            EDRP_APP (procedures)
├── REDUCTION_TABLE_LOG          ├── SP_EXTRACT_METADATA
├── VALIDATION_RESULTS           ├── SP_BUILD_DEPENDENCY_GRAPH
├── V_RELATIONSHIP_LINEAGE       ├── SP_EXECUTE_REDUCTION
└── V_CATEGORICAL_COVERAGE       ├── SP_VALIDATE_REDUCTION
                                 ├── SP_ROLLBACK_REDUCTION
                                 ├── SP_AI_DISCOVER_RELATIONSHIPS
                                 └── FN_AI_SUMMARIZE_VALIDATION
```

---

## Pipeline Steps

```
SP_EXTRACT_METADATA          Populates table/column inventory from INFORMATION_SCHEMA
        ↓
SP_BUILD_DEPENDENCY_GRAPH    Topological sort, sets reduction order
        ↓
SP_EXECUTE_REDUCTION         CTAS in dependency order: FULL_COPY → SAMPLE → FK_CASCADE
        ↓
SP_VALIDATE_REDUCTION        FK integrity + distribution checks
```

---

## Reduction Strategies

| Strategy | Used For | Behavior |
|----------|----------|----------|
| `FULL_COPY` | Dimension/reference tables | Copies all rows |
| `SAMPLE` | Driver tables (e.g., CUSTOMER) | Bernoulli or stratified sampling |
| `FK_CASCADE` | Fact tables | INNER JOIN to already-reduced parents |
| `EXCLUDE` | Tables to skip | No output created |

---

## Stratified Sampling (Categorical Coverage)

To guarantee all distinct values of a column survive reduction:

```sql
UPDATE DATA_REDUCTION_POC.EDRP_METADATA.TABLE_INVENTORY
SET SAMPLING_STRATEGY = 'STRATIFIED',
    STRATIFY_COLUMNS = 'C_BIRTH_COUNTRY'      -- comma-separated for multiple
WHERE TABLE_NAME = 'CUSTOMER'
  AND SOURCE_DATABASE = 'DATA_REDUCTION_POC'
  AND SOURCE_SCHEMA = 'STAGE_DATA';
```

This ensures at least 1 row per distinct value, with proportional representation.

---

## Deploying to a Different Target Schema

Change one place — the reduction profile:

```sql
INSERT INTO DATA_REDUCTION_POC.EDRP_METADATA.REDUCTION_PROFILE
    (PROFILE_NAME, SOURCE_DATABASE, SOURCE_SCHEMA, TARGET_DATABASE, TARGET_SCHEMA,
     DEFAULT_SAMPLE_PERCENT, DIMENSION_STRATEGY, FACT_STRATEGY, RANDOM_SEED, STATUS, CREATED_BY)
VALUES
    ('MY_ENV_10PCT', 'MY_DATABASE', 'MY_SOURCE_SCHEMA',
     'MY_DATABASE', 'MY_TARGET_SCHEMA',
     10.00, 'FULL_COPY', 'FK_CASCADE', 42, 'ACTIVE', CURRENT_USER());
```

Then run: `CALL SP_EXECUTE_REDUCTION('MY_ENV_10PCT');`

---

## Rollback

If a job fails or produces unwanted results:

```sql
CALL DATA_REDUCTION_POC.EDRP_APP.SP_ROLLBACK_REDUCTION(<JOB_ID>);
```

This drops all tables created by that job and marks it as `ROLLED_BACK`.

---

## Files in This Package

| File | Lines | Description |
|------|-------|-------------|
| `01_prerequisites.sql` | 29 | Database, schemas, warehouse |
| `02_metadata_tables.sql` | 174 | 8 metadata table DDLs |
| `03_stored_procedures.sql` | 751 | 6 SPs + 1 function |
| `04_views.sql` | 71 | 2 reporting views |
| `05_rbac.sql` | 44 | 3 roles + grants |
| `06_initial_config.sql` | 48 | Seed reduction profile |
| `07_run_pipeline.sql` | 87 | End-to-end execution guide |
| `08_validation_queries.sql` | 464 | 10 validation query sections |
| `README.md` | This file | Deployment guide |
