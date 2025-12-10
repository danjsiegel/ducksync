#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DUCKDB="$PROJECT_DIR/build/release/duckdb"
EXTENSION="$PROJECT_DIR/build/release/extension/ducksync/ducksync.duckdb_extension"

echo "=========================================="
echo "DuckSync Integration Tests"
echo "=========================================="

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

echo ""
echo "=========================================="
echo "Test 1: Extension Loading"
echo "=========================================="
$DUCKDB -c "
LOAD '$EXTENSION';
SELECT 'Extension loaded successfully!' as result;
"

echo ""
echo "=========================================="
echo "Test 2: Check Functions Registered"
echo "=========================================="
$DUCKDB -c "
LOAD '$EXTENSION';
SELECT function_name FROM duckdb_functions() WHERE function_name LIKE 'ducksync%' ORDER BY function_name;
"

echo ""
echo "=========================================="
echo "Test 3: Install DuckLake + Dependencies"
echo "=========================================="
$DUCKDB -c "
-- DuckLake with PostgreSQL backend requires postgres_scanner
INSTALL postgres_scanner;
INSTALL ducklake;
SELECT 'Extensions installed!' as result;
"

echo ""
echo "=========================================="
echo "Test 4: Setup Storage (DuckLake + PostgreSQL)"
echo "=========================================="
$DUCKDB -c "
LOAD '$EXTENSION';
SELECT * FROM ducksync_setup_storage(
    '$PG_CONN',
    '$TEST_DATA_DIR'
);
"

echo ""
echo "=========================================="
echo "Test 5: Add Source"
echo "=========================================="
$DUCKDB -c "
LOAD '$EXTENSION';
-- First setup storage
SELECT * FROM ducksync_setup_storage('$PG_CONN', '$TEST_DATA_DIR');
-- Then add source
SELECT * FROM ducksync_add_source(
    'test_snowflake',
    'snowflake', 
    'my_sf_secret'
);
"

echo ""
echo "=========================================="
echo "Test 6: Create Cache"
echo "=========================================="
$DUCKDB -c "
LOAD '$EXTENSION';
-- Setup
SELECT * FROM ducksync_setup_storage('$PG_CONN', '$TEST_DATA_DIR');
SELECT * FROM ducksync_add_source('test_snowflake', 'snowflake', 'my_sf_secret');
-- Create cache
SELECT * FROM ducksync_create_cache(
    'customer_cache',
    'test_snowflake',
    'SELECT * FROM customers',
    ['mydb.myschema.customers'],
    3600
);
"

echo ""
echo "=========================================="
echo "All Tests Completed!"
echo "=========================================="
echo ""
echo "Note: Refresh tests require real Snowflake credentials."
echo "To test refresh, create a Snowflake secret and run:"
echo "  SELECT * FROM ducksync_refresh('customer_cache');"
echo ""
echo "To stop PostgreSQL: make test-docker-down"
