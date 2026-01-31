-- DuckSync Test Environment - Snowflake Permissions Setup
-- Run this as ACCOUNTADMIN or SECURITYADMIN
--
-- BEFORE RUNNING: Replace these placeholders with your values from .env:
--   ${SNOWFLAKE_DATABASE}  -> Your test database name
--   ${SNOWFLAKE_SCHEMA}    -> Your test schema name  
--   ${SNOWFLAKE_WAREHOUSE} -> Your warehouse name
--   ${SNOWFLAKE_ROLE}      -> Your existing role (or create new one)
--   ${SNOWFLAKE_USER}      -> Your existing service account user
--
-- Or use the shell command at the bottom to auto-substitute.

-- ============================================================================
-- 1. Create Test Database/Schema (if not exists)
-- ============================================================================
CREATE DATABASE IF NOT EXISTS ${SNOWFLAKE_DATABASE};
CREATE SCHEMA IF NOT EXISTS ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA};

-- ============================================================================
-- 2. Grant Database/Schema Access to Your Role
-- ============================================================================
-- Skip this section if your role already has access

GRANT USAGE ON DATABASE ${SNOWFLAKE_DATABASE} TO ROLE ${SNOWFLAKE_ROLE};
GRANT USAGE ON SCHEMA ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA} TO ROLE ${SNOWFLAKE_ROLE};

-- Grant SELECT on all current and future tables
GRANT SELECT ON ALL TABLES IN SCHEMA ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA} TO ROLE ${SNOWFLAKE_ROLE};
GRANT SELECT ON FUTURE TABLES IN SCHEMA ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA} TO ROLE ${SNOWFLAKE_ROLE};

-- ============================================================================
-- 3. Grant Warehouse Access (if not already granted)
-- ============================================================================
GRANT USAGE ON WAREHOUSE ${SNOWFLAKE_WAREHOUSE} TO ROLE ${SNOWFLAKE_ROLE};

-- ============================================================================
-- 4. Verify Role is Assigned to User (if using existing user)
-- ============================================================================
-- Uncomment if you need to grant the role to your user:
-- GRANT ROLE ${SNOWFLAKE_ROLE} TO USER ${SNOWFLAKE_USER};

-- ============================================================================
-- 5. Verification Queries
-- ============================================================================
-- Run these as your service account user to verify access:
--
-- SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE();
-- SELECT * FROM ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.CUSTOMERS LIMIT 1;
-- SELECT table_name, last_altered FROM ${SNOWFLAKE_DATABASE}.information_schema.tables 
--     WHERE table_schema = '${SNOWFLAKE_SCHEMA}';

-- ============================================================================
-- HELPER: Auto-substitute from .env
-- ============================================================================
-- Run this from your terminal to generate the SQL with your values:
--
-- source .env && envsubst < scripts/setup_snowflake_permissions.sql
--
-- Or manually find/replace the ${...} placeholders above.
