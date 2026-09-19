-- ============================================================================
-- EDRP DEPLOYMENT — Step 7: Run Pipeline
-- ============================================================================
-- Execute the full reduction pipeline end-to-end.
-- Run as: EDRP_OPERATOR or EDRP_ADMIN
-- Prerequisite: Steps 01-06 completed. Source data exists in source schema.
-- ============================================================================

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
