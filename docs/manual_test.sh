#!/bin/bash
# DuckSync Manual Test Script
# Run this for quick end-to-end verification with real Snowflake

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║           DuckSync Manual Testing Script                    ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Check prerequisites
echo -e "${YELLOW}[1/8] Checking prerequisites...${NC}"

if [ ! -f ".env" ]; then
    echo -e "${RED}ERROR: .env file not found. Copy .env.example and fill in credentials.${NC}"
    exit 1
fi

if [ ! -f "build/release/duckdb" ]; then
    echo -e "${RED}ERROR: DuckDB not built. Run 'make release' first.${NC}"
    exit 1
fi

# Load environment variables
set -a
source .env
set +a

echo -e "${GREEN}✓ Prerequisites OK${NC}"
echo ""

# Reset PostgreSQL (fresh catalog)
echo -e "${YELLOW}[2/8] Resetting PostgreSQL (clean catalog)...${NC}"
cd test && docker compose down -v 2>/dev/null || true
docker compose up -d postgres && cd ..
sleep 3
echo -e "${GREEN}✓ PostgreSQL running (fresh)${NC}"
echo ""

# Clean previous test data
echo -e "${YELLOW}[3/8] Cleaning previous test data...${NC}"
rm -rf test/data/manual_test
rm -rf test/data/prod
echo -e "${GREEN}✓ Test data cleaned${NC}"
echo ""

# Create test SQL file
TEST_SQL=$(mktemp)
cat > "$TEST_SQL" << EOF
-- DuckSync Manual Test
.timer on
.mode box

-- Load extensions
INSTALL snowflake FROM community;
LOAD snowflake;
LOAD 'build/release/extension/ducksync/ducksync.duckdb_extension';

-- Show loaded functions
SELECT '=== DuckSync Functions ===' as section;
SELECT function_name FROM duckdb_functions() WHERE function_name LIKE 'ducksync%' ORDER BY function_name;

-- Create Snowflake secret
SELECT '=== Creating Snowflake Secret ===' as section;
CREATE SECRET sf_test (
    TYPE snowflake,
    ACCOUNT '${SNOWFLAKE_ACCOUNT}',
    USER '${SNOWFLAKE_USER}',
    PASSWORD '${SNOWFLAKE_PASSWORD}',
    DATABASE '${SNOWFLAKE_DATABASE}',
    WAREHOUSE '${SNOWFLAKE_WAREHOUSE}'
);

-- Test Snowflake connection
SELECT '=== Testing Snowflake Connection ===' as section;
SELECT * FROM snowflake_query('SELECT CURRENT_USER() as user, CURRENT_ROLE() as role', 'sf_test');

-- Initialize DuckSync
SELECT '=== Initializing DuckSync ===' as section;
SELECT * FROM ducksync_setup_storage(
    'host=localhost port=5432 dbname=ducklake user=ducksync password=ducksync',
    './test/data/manual_test'
);

-- Add source
SELECT '=== Adding Snowflake Source ===' as section;
SELECT * FROM ducksync_add_source(
    'prod',
    'snowflake',
    'sf_test'
);

-- Create cache
SELECT '=== Creating Cache ===' as section;
SELECT * FROM ducksync_create_cache(
    'customers_cache',
    'prod',
    'SELECT * FROM ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.CUSTOMERS',
    ['${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.CUSTOMERS'],
    3600
);

-- Refresh cache to load data from Snowflake
SELECT '=== Refreshing Cache ===' as section;
SELECT * FROM ducksync_refresh('customers_cache');

-- Test direct DuckLake access (standard DuckDB - no magic)
SELECT '=== Testing Direct DuckLake Access ===' as section;
SELECT * FROM ducksync.prod.customers_cache LIMIT 5;

-- Test ducksync_query with cached table
SELECT '=== Testing ducksync_query (Cache Hit) ===' as section;
SELECT * FROM ducksync_query('SELECT * FROM ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.CUSTOMERS LIMIT 5', 'prod');

-- Test ducksync_query with named query (cache name directly)
SELECT '=== Testing ducksync_query (Named Query - Cache Name) ===' as section;
SELECT * FROM ducksync_query('SELECT * FROM customers_cache LIMIT 5', 'prod');

-- Test ducksync_query with uncached table (passthrough)
SELECT '=== Testing ducksync_query (Passthrough) ===' as section;
SELECT * FROM ducksync_query('SELECT * FROM ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.PRODUCTS LIMIT 5', 'prod');

-- Test TTL expiry and auto-refresh
SELECT '=== Testing TTL Expiry (Forcing Expired State) ===' as section;

-- Manually set the cache state to expired (hack expires_at to the past)
-- State is stored in DuckLake metadata schema: ducksync.ducksync.state
UPDATE ducksync.ducksync.state 
SET expires_at = '2020-01-01 00:00:00'::TIMESTAMP 
WHERE cache_name = 'customers_cache';

-- Verify it's expired
SELECT cache_name, expires_at, 
       CASE WHEN expires_at < CURRENT_TIMESTAMP THEN 'EXPIRED' ELSE 'VALID' END as status
FROM ducksync.ducksync.state WHERE cache_name = 'customers_cache';

-- Query should trigger auto-refresh since TTL is expired
SELECT '=== Querying Expired Cache (Should Auto-Refresh) ===' as section;
SELECT * FROM ducksync_query('SELECT * FROM ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.CUSTOMERS LIMIT 3', 'prod');

-- Check that last_refresh was updated (confirms auto-refresh ran)
SELECT cache_name, last_refresh, expires_at,
       CASE WHEN last_refresh > '2026-01-01' THEN 'AUTO-REFRESHED' ELSE 'NOT REFRESHED' END as status
FROM ducksync.ducksync.state WHERE cache_name = 'customers_cache';

SELECT '=== All Tests Complete ===' as section;
EOF

echo -e "${YELLOW}[4/8] Running DuckSync tests...${NC}"
echo ""

# Run the test
./build/release/duckdb < "$TEST_SQL"

# Cleanup temp file
rm -f "$TEST_SQL"

echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║                    Testing Complete!                         ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "To clean up:"
echo -e "  ${BLUE}make clean-test-data${NC}  - Remove test parquet files"
echo -e "  ${BLUE}make test-docker-down${NC} - Stop PostgreSQL"
echo ""
