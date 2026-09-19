-- ============================================================================
-- EDRP DEPLOYMENT — Step 6: Initial Configuration
-- ============================================================================
-- Creates the first reduction profile. Adjust source/target as needed.
-- Run as: EDRP_ADMIN or ACCOUNTADMIN
-- ============================================================================

USE SCHEMA DATA_REDUCTION_POC.EDRP_METADATA;

-- ============================================================================
-- OPTION A: Default 10% reduction profile (adjust SOURCE/TARGET as needed)
-- ============================================================================
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

-- ============================================================================
-- EXAMPLE: Additional profile targeting a different schema
-- ============================================================================
-- INSERT INTO REDUCTION_PROFILE
--     (PROFILE_NAME, SOURCE_DATABASE, SOURCE_SCHEMA, TARGET_DATABASE, TARGET_SCHEMA,
--      DEFAULT_SAMPLE_PERCENT, DIMENSION_STRATEGY, FACT_STRATEGY, RANDOM_SEED, STATUS, CREATED_BY)
-- VALUES
--     ('RAKESH_10PCT', 'DATA_REDUCTION_POC', 'STAGE_DATA',
--      'DATA_REDUCTION_POC', 'RAKESH_RD',
--      10.00, 'FULL_COPY', 'FK_CASCADE', 42, 'ACTIVE', CURRENT_USER());

-- ============================================================================
-- EXAMPLE: Configure stratified sampling for categorical coverage
-- ============================================================================
-- After running SP_EXTRACT_METADATA + SP_BUILD_DEPENDENCY_GRAPH, you can
-- configure stratified sampling on any SAMPLE-strategy table:
--
-- UPDATE TABLE_INVENTORY
-- SET SAMPLING_STRATEGY = 'STRATIFIED',
--     STRATIFY_COLUMNS = 'C_BIRTH_COUNTRY'
-- WHERE TABLE_NAME = 'CUSTOMER'
--   AND SOURCE_DATABASE = 'DATA_REDUCTION_POC'
--   AND SOURCE_SCHEMA = 'STAGE_DATA';
