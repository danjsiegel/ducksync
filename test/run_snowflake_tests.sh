#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DUCKDB="$PROJECT_DIR/build/release/duckdb"
EXTENSION="$PROJECT_DIR/build/release/extension/ducksync/ducksync.duckdb_extension"

echo "=========================================="
echo "DuckSync Snowflake Integration Tests"
echo "=========================================="

# Load .env file if it exists
if [ -f "$PROJECT_DIR/.env" ]; then
    echo ">> Loading credentials from .env"
    set -a
    source "$PROJECT_DIR/.env"
    set +a
else
    echo "WARNING: No .env file found. Using environment variables."
fi

# Check required environment variables
REQUIRED_VARS=(SNOWFLAKE_ACCOUNT SNOWFLAKE_USER SNOWFLAKE_PASSWORD SNOWFLAKE_WAREHOUSE SNOWFLAKE_DATABASE)
for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var}" ]; then
        echo "ERROR: $var is not set"
        echo "Copy .env.example to .env and fill in your Snowflake credentials"
        exit 1
    fi
done

# Check if extension exists
if [ ! -f "$EXTENSION" ]; then
    echo "ERROR: Extension not built. Run 'make release' first."
    exit 1
fi

# Start PostgreSQL if not running
echo ""
echo ">> Starting PostgreSQL..."
cd "$SCRIPT_DIR"
docker compose up -d postgres

# Wait for PostgreSQL to be ready
echo ">> Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
    if docker compose exec -T postgres pg_isready -U ducksync -d ducklake > /dev/null 2>&1; then
        echo ">> PostgreSQL is ready!"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "ERROR: PostgreSQL failed to start"
        docker compose logs postgres
        exit 1
    fi
    sleep 1
done

# Create temp directory for test data
TEST_DATA_DIR="/tmp/ducksync_test_data"
mkdir -p "$TEST_DATA_DIR"

# PostgreSQL connection string (libpq format for DuckLake)
PG_CONN="host=localhost port=5432 dbname=ducklake user=ducksync password=ducksync"

# Default schema if not set
SNOWFLAKE_SCHEMA="${SNOWFLAKE_SCHEMA:-TEST_DATA}"

echo ""
echo "=========================================="
echo "Test 1: Create Snowflake Secret"
echo "=========================================="
$DUCKDB -c "
LOAD '$EXTENSION';

-- Create Snowflake secret
CREATE SECRET sf_test (
    TYPE snowflake,
    account '$SNOWFLAKE_ACCOUNT',
    user '$SNOWFLAKE_USER',
    password '$SNOWFLAKE_PASSWORD'
);

SELECT 'Snowflake secret created!' as result;
"

echo ""
echo "=========================================="
echo "Test 2: Verify Snowflake Connection"
echo "=========================================="
$DUCKDB -c "
LOAD '$EXTENSION';
INSTALL snowflake;
LOAD snowflake;

CREATE SECRET sf_test (
    TYPE snowflake,
    account '$SNOWFLAKE_ACCOUNT',
    user '$SNOWFLAKE_USER',
    password '$SNOWFLAKE_PASSWORD'
);

-- Test basic query
SELECT * FROM snowflake_query('SELECT CURRENT_USER(), CURRENT_ROLE()', 'sf_test');
"

echo ""
echo "=========================================="
echo "Test 3: Query Test Tables"
echo "=========================================="
$DUCKDB -c "
LOAD '$EXTENSION';
INSTALL snowflake;
LOAD snowflake;

CREATE SECRET sf_test (
    TYPE snowflake,
    account '$SNOWFLAKE_ACCOUNT',
    user '$SNOWFLAKE_USER',
    password '$SNOWFLAKE_PASSWORD'
);

-- Query test tables
SELECT 'CUSTOMERS' as table_name, COUNT(*) as rows FROM snowflake_query('SELECT * FROM $SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA.CUSTOMERS', 'sf_test')
UNION ALL
SELECT 'PRODUCTS', COUNT(*) FROM snowflake_query('SELECT * FROM $SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA.PRODUCTS', 'sf_test')
UNION ALL
SELECT 'ORDERS', COUNT(*) FROM snowflake_query('SELECT * FROM $SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA.ORDERS', 'sf_test');
"

echo ""
echo "=========================================="
echo "Test 4: Full DuckSync Flow"
echo "=========================================="
$DUCKDB -c "
LOAD '$EXTENSION';
INSTALL snowflake;
LOAD snowflake;

-- Create Snowflake secret
CREATE SECRET sf_test (
    TYPE snowflake,
    account '$SNOWFLAKE_ACCOUNT',
    user '$SNOWFLAKE_USER',
    password '$SNOWFLAKE_PASSWORD'
);

-- Setup DuckSync storage
SELECT * FROM ducksync_setup_storage('$PG_CONN', '$TEST_DATA_DIR');

-- Add Snowflake source
SELECT * FROM ducksync_add_source('sf', 'snowflake', 'sf_test');

-- Create cache for CUSTOMERS table
SELECT * FROM ducksync_create_cache(
    'customers_cache',
    'sf',
    'SELECT * FROM $SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA.CUSTOMERS',
    ['$SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA.CUSTOMERS'],
    3600
);

-- Refresh the cache (actually fetches from Snowflake)
SELECT * FROM ducksync_refresh('customers_cache');

-- Query the cached data
SELECT 'Cached rows:' as label, COUNT(*) as count FROM customers_cache;
"

echo ""
echo "=========================================="
echo "Test 5: Smart Refresh (Should Skip)"
echo "=========================================="
$DUCKDB -c "
LOAD '$EXTENSION';
INSTALL snowflake;
LOAD snowflake;

CREATE SECRET sf_test (
    TYPE snowflake,
    account '$SNOWFLAKE_ACCOUNT',
    user '$SNOWFLAKE_USER',
    password '$SNOWFLAKE_PASSWORD'
);

SELECT * FROM ducksync_setup_storage('$PG_CONN', '$TEST_DATA_DIR');
SELECT * FROM ducksync_add_source('sf', 'snowflake', 'sf_test');

SELECT * FROM ducksync_create_cache(
    'customers_cache',
    'sf',
    'SELECT * FROM $SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA.CUSTOMERS',
    ['$SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA.CUSTOMERS'],
    3600
);

-- First refresh
SELECT 'First refresh:' as label, result, message FROM ducksync_refresh('customers_cache');

-- Second refresh should skip (data hasn't changed)
SELECT 'Second refresh:' as label, result, message FROM ducksync_refresh('customers_cache');
"

echo ""
echo "=========================================="
echo "Test 6: Passthrough Query"
echo "=========================================="
$DUCKDB -c "
LOAD '$EXTENSION';
INSTALL snowflake;
LOAD snowflake;

CREATE SECRET sf_test (
    TYPE snowflake,
    account '$SNOWFLAKE_ACCOUNT',
    user '$SNOWFLAKE_USER',
    password '$SNOWFLAKE_PASSWORD'
);

SELECT * FROM ducksync_setup_storage('$PG_CONN', '$TEST_DATA_DIR');
SELECT * FROM ducksync_add_source('sf', 'snowflake', 'sf_test');

-- Passthrough query (not cached)
SELECT * FROM ducksync_passthrough_query(
    'SELECT region, COUNT(*) as customer_count FROM $SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA.CUSTOMERS GROUP BY region',
    'sf'
);
"

echo ""
echo "=========================================="
echo "All Snowflake Tests Completed!"
echo "=========================================="
echo ""
echo "To stop PostgreSQL: make test-docker-down"
