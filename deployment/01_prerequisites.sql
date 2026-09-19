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
