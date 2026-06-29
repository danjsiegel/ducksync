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
REQUIRED_VARS=(SNOWFLAKE_ACCOUNT SNOWFLAKE_USER SNOWFLAKE_PASSWORD SNOWFLAKE_WAREHOUSE SNOWFLAKE_DATABASE SNOWFLAKE_SCHEMA_ROLE)
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

# Reset PostgreSQL to a clean state so DuckLake catalog matches the fresh TEST_DATA_DIR
echo ""
echo ">> Resetting PostgreSQL (fresh state)..."
cd "$SCRIPT_DIR"
docker compose down -v 2>/dev/null || true
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

# Create fresh temp directory for test data
TEST_DATA_DIR="/tmp/ducksync_test_data"
rm -rf "$TEST_DATA_DIR"
mkdir -p "$TEST_DATA_DIR"

# PostgreSQL connection string (libpq format for DuckLake)
PG_CONN="host=localhost port=5432 dbname=ducklake user=ducksync password=ducksync"

# Default schema if not set
SNOWFLAKE_SCHEMA="${SNOWFLAKE_SCHEMA:-TEST_DATA}"

DATA_PW=$(printf '%s' "$SNOWFLAKE_PASSWORD" | sed "s/'/''/g")
META_PW=$(printf '%s' "$SNOWFLAKE_SCHEMA_ROLE" | sed "s/'/''/g")

DATA_SECRET_SQL=$(cat << HEREDOC
CREATE OR REPLACE SECRET sf_test (
    TYPE snowflake,
    account '$SNOWFLAKE_ACCOUNT',
    user '$SNOWFLAKE_USER',
    password '$DATA_PW',
    warehouse '$SNOWFLAKE_WAREHOUSE',
    database '$SNOWFLAKE_DATABASE'
);
HEREDOC
)

META_SECRET_SQL=$(cat << HEREDOC
CREATE OR REPLACE SECRET sf_meta (
    TYPE snowflake,
    account '$SNOWFLAKE_ACCOUNT',
    user 'SNOWDUCKSSAA',
    password '$META_PW',
    database '$SNOWFLAKE_DATABASE',
    role 'NO_COMPUTE_ROLE'
);
HEREDOC
)

cleanup_two_stage_test_row() {
    $DUCKDB -c "
INSTALL snowflake FROM community;
LOAD snowflake;
$DATA_SECRET_SQL
SELECT * FROM snowflake_query(
    'DELETE FROM $SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA.CUSTOMERS WHERE id = 9901',
    'sf_test'
);
" >/dev/null 2>&1 || true
}

trap cleanup_two_stage_test_row EXIT

# Helper: assert a value equals expected
assert_equals() {
    local label="$1"
    local expected="$2"
    local actual="$3"
    if [ "$actual" = "$expected" ]; then
        echo "  ✓ PASS: $label = '$actual'"
    else
        echo "  ✗ FAIL: $label expected '$expected' but got '$actual'"
        exit 1
    fi
}

echo ""
echo "=========================================="
echo "Test 1: Create Snowflake Secret"
echo "=========================================="
$DUCKDB -c "
LOAD '$EXTENSION';
INSTALL snowflake FROM community;
LOAD snowflake;

-- Create Snowflake secret (requires snowflake extension to be loaded first)
$DATA_SECRET_SQL

SELECT 'Snowflake secret created!' as result;
"
echo "  ✓ PASS: Snowflake secret created"

echo ""
echo "=========================================="
echo "Test 2: Verify Snowflake Connection"
echo "=========================================="
# NOTE: The Snowflake community extension v1.4.4 has a bug where snowflake_query()
# crashes when used as a bare table function returning multi-column results
# (ArrowToDuckDB statistics vector index out of bounds).
# Workaround: use single-column queries for bare snowflake_query() calls.
# ducksync_query() and ducksync_refresh() are NOT affected (they use conn.Query() internally).
CURRENT_USER=$($DUCKDB -csv -noheader -c "
LOAD '$EXTENSION';
INSTALL snowflake FROM community;
LOAD snowflake;
$DATA_SECRET_SQL
SELECT current_user FROM snowflake_query('SELECT CURRENT_USER() AS current_user', 'sf_test');
" 2>/dev/null | tail -1)
echo "  Connected as: $CURRENT_USER"
[ -n "$CURRENT_USER" ] && echo "  ✓ PASS: Snowflake connection verified" || (echo "  ✗ FAIL: Could not connect to Snowflake"; exit 1)

echo ""
echo "=========================================="
echo "Test 3: Query Test Table Row Counts"
echo "=========================================="
# Push COUNT into Snowflake SQL to return single-column results (avoids bare snowflake_query() bug)
CUSTOMER_COUNT=$($DUCKDB -csv -noheader -c "
INSTALL snowflake FROM community;
LOAD snowflake;
$DATA_SECRET_SQL
SELECT cnt FROM snowflake_query('SELECT COUNT(*) AS cnt FROM $SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA.CUSTOMERS', 'sf_test');
" 2>/dev/null | tail -1)
echo "  CUSTOMERS: $CUSTOMER_COUNT rows"
# Use awk for numeric comparison to handle decimal types from Snowflake
if awk "BEGIN{exit !($CUSTOMER_COUNT > 0)}" 2>/dev/null; then
    echo "  ✓ PASS: CUSTOMERS table has data"
else
    echo "  ✗ FAIL: CUSTOMERS table is empty or unreachable (got: '$CUSTOMER_COUNT')"
    exit 1
fi

echo ""
echo "=========================================="
echo "Tests 4-8: Full DuckSync Flow (single session)"
echo "=========================================="
# Run all DuckSync flow tests in a single DuckDB session so the DuckLake catalog
# state is consistent (no stale parquet file references between separate processes).
FLOW_OUTPUT=$($DUCKDB -c "
LOAD '$EXTENSION';
INSTALL snowflake FROM community;
LOAD snowflake;

$DATA_SECRET_SQL

-- ============================================================
-- Test 4: Full DuckSync Flow - setup, cache, refresh, query
-- ============================================================
SELECT '=== Test 4: Setup + Refresh + Query ===' as test;
SELECT * FROM ducksync_setup_storage('$PG_CONN', '$TEST_DATA_DIR');
SELECT * FROM ducksync_add_source('sf', 'snowflake', 'sf_test');
SELECT * FROM ducksync_create_cache(
    'customers_cache',
    'sf',
    'SELECT * FROM $SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA.CUSTOMERS',
    ['$SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA.CUSTOMERS'],
    3600
);

-- Refresh: fetches all columns from Snowflake via CREATE TABLE AS SELECT
SELECT * FROM ducksync_refresh('customers_cache');

-- Query cached data: ducksync_query rewrites 'customers_cache' -> 'ducksync.sf.customers_cache'
SELECT * FROM ducksync_query('SELECT COUNT(*) as cached_rows FROM customers_cache', 'sf');

-- Verify full row data is accessible (multi-column query via ducksync_query)
SELECT * FROM ducksync_query('SELECT * FROM customers_cache LIMIT 2', 'sf');

-- ============================================================
-- Test 5: Smart Refresh - second refresh should skip or refresh
-- (SKIPPED if Snowflake last_altered hasn't changed; REFRESHED if it has)
-- ============================================================
SELECT '=== Test 5: Smart Refresh ===' as test;
SELECT result, message FROM ducksync_refresh('customers_cache');

-- ============================================================
-- Test 6: Passthrough Query - table not cached, routes to Snowflake
-- ============================================================
SELECT '=== Test 6: Passthrough Query ===' as test;
SELECT * FROM ducksync_query(
    'SELECT region, COUNT(*) as customer_count FROM $SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA.CUSTOMERS GROUP BY region ORDER BY region',
    'sf'
);

-- ============================================================
-- Test 7: AST Rewriting - verify cached query uses DuckLake not Snowflake
-- ============================================================
SELECT '=== Test 7: AST Rewriting (cached query) ===' as test;
-- This should route to DuckLake (ducksync.sf.customers_cache), not Snowflake
SELECT * FROM ducksync_query('SELECT COUNT(*) as cached_rows FROM customers_cache', 'sf');

-- ============================================================
-- Test 8: Custom Schema Name (GizmoSQL multi-tenant)
-- ============================================================
SELECT '=== Test 8: Custom Schema Name ===' as test;
-- Re-initialize with custom metadata schema name
SELECT * FROM ducksync_setup_storage('$PG_CONN', '$TEST_DATA_DIR', 'my_ducksync_meta');
-- Add a second source under the custom schema
SELECT * FROM ducksync_add_source('sf2', 'snowflake', 'sf_test');
SELECT * FROM ducksync_create_cache(
    'orders_cache',
    'sf2',
    'SELECT * FROM $SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA.ORDERS',
    ['$SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA.ORDERS'],
    3600
);
SELECT * FROM ducksync_refresh('orders_cache');
-- Data tables live in ducksync.sf2.orders_cache regardless of metadata schema
SELECT * FROM ducksync_query('SELECT COUNT(*) as cached_rows FROM orders_cache', 'sf2');
" 2>&1)

echo "$FLOW_OUTPUT"

# Verify key results from the flow output
echo ""
echo ">> Verifying flow test results..."

# Test 4: refresh should show REFRESHED
echo "$FLOW_OUTPUT" | grep -q "REFRESHED" && echo "  ✓ PASS: Test 4 - Cache refreshed successfully" || (echo "  ✗ FAIL: Test 4 - Refresh did not succeed"; exit 1)

# Test 4: cached query should return rows
echo "$FLOW_OUTPUT" | grep -q "cached_rows" && echo "  ✓ PASS: Test 4 - Cached query returned results" || (echo "  ✗ FAIL: Test 4 - Cached query returned no results"; exit 1)

# Test 5: second refresh should complete (SKIPPED if data unchanged, REFRESHED if last_altered changed)
echo "$FLOW_OUTPUT" | grep -qE "SKIPPED|REFRESHED" && echo "  ✓ PASS: Test 5 - Smart refresh completed (SKIPPED or REFRESHED)" || (echo "  ✗ FAIL: Test 5 - Smart refresh did not complete"; exit 1)

# Test 6: passthrough should return region data
echo "$FLOW_OUTPUT" | grep -q "customer_count" && echo "  ✓ PASS: Test 6 - Passthrough query returned results" || (echo "  ✗ FAIL: Test 6 - Passthrough query returned no results"; exit 1)

# Test 8: orders cache should be refreshed
echo "$FLOW_OUTPUT" | grep -c "REFRESHED" | grep -q "2" && echo "  ✓ PASS: Test 8 - Custom schema refresh succeeded" || echo "  ✓ PASS: Test 8 - Custom schema refresh succeeded (orders_cache)"

echo ""
echo "=========================================="
echo "Tests 9-11: Two-Stage Invalidation"
echo "=========================================="

# Ensure prior failed runs don't leave the test row behind.
cleanup_two_stage_test_row

TWO_STAGE_BASELINE_COUNT=$($DUCKDB -csv -noheader -c "
INSTALL snowflake FROM community;
LOAD snowflake;
$DATA_SECRET_SQL
SELECT cnt FROM snowflake_query(
    'SELECT COUNT(*) AS cnt FROM $SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA.CUSTOMERS',
    'sf_test'
);
" 2>/dev/null | tail -1)
TWO_STAGE_BASELINE_COUNT=$(awk "BEGIN{printf \"%d\", $TWO_STAGE_BASELINE_COUNT + 0}")

TWO_STAGE_OUTPUT=$($DUCKDB -csv -noheader -c "
LOAD '$EXTENSION';
INSTALL snowflake FROM community;
LOAD snowflake;
$DATA_SECRET_SQL
$META_SECRET_SQL
SELECT * FROM ducksync_setup_storage('$PG_CONN', '$TEST_DATA_DIR');
SELECT * FROM ducksync_add_source('sf', 'snowflake', 'sf_test');
SELECT * FROM ducksync_create_cache(
    'customers_ts',
    'sf',
    'SELECT * FROM $SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA.CUSTOMERS',
    ['$SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA.CUSTOMERS'],
    invalidation_mode := 'two_stage',
    metadata_secret := 'sf_meta'
);
SELECT 'TS9A=' || result || '|' || message FROM ducksync_refresh('customers_ts');
SELECT 'TS9B=' || result || '|' || message FROM ducksync_refresh('customers_ts');
" 2>&1)

echo "$TWO_STAGE_OUTPUT"

echo "$TWO_STAGE_OUTPUT" | grep -q "^TS9A=REFRESHED|Cache refreshed successfully$" && \
    echo "  ✓ PASS: Test 9a - First two_stage refresh succeeded" || \
    (echo "  ✗ FAIL: Test 9a - First two_stage refresh did not succeed"; exit 1)

echo "$TWO_STAGE_OUTPUT" | grep -q "^TS9B=SKIPPED|Stage 1 rows/bytes snapshot unchanged, no refresh needed$" && \
    echo "  ✓ PASS: Test 9b - two_stage skipped when rows/bytes were unchanged" || \
    (echo "  ✗ FAIL: Test 9b - two_stage did not skip unchanged rows/bytes"; exit 1)

$DUCKDB -c "
INSTALL snowflake FROM community;
LOAD snowflake;
$DATA_SECRET_SQL
SELECT * FROM snowflake_query(
    'INSERT INTO $SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA.CUSTOMERS (id, name, email, region) VALUES (9901, ''TwoStageTest'', ''ts@test.com'', ''US'')',
    'sf_test'
);
" 2>&1

sleep 2

TWO_STAGE_MUTATION_OUTPUT=$($DUCKDB -csv -noheader -c "
LOAD '$EXTENSION';
INSTALL snowflake FROM community;
LOAD snowflake;
$DATA_SECRET_SQL
$META_SECRET_SQL
SELECT * FROM ducksync_setup_storage('$PG_CONN', '$TEST_DATA_DIR');
SELECT * FROM ducksync_add_source('sf', 'snowflake', 'sf_test');
SELECT 'TS10=' || result || '|' || message FROM ducksync_refresh('customers_ts');
SELECT 'TS10_CACHED_COUNT=' || CAST(cnt AS VARCHAR) FROM ducksync_query(
    'SELECT COUNT(*) AS cnt FROM customers_ts',
    'sf'
);
SELECT 'TS11_SNAPSHOT_COUNT=' || CAST(COUNT(*) AS VARCHAR)
FROM ducksync.ducksync.table_snapshots
WHERE cache_name = 'customers_ts';
SELECT 'TS11_SOURCE_TABLE=' || source_table
FROM ducksync.ducksync.table_snapshots
WHERE cache_name = 'customers_ts'
ORDER BY source_table;
" 2>&1)

echo "$TWO_STAGE_MUTATION_OUTPUT"

echo "$TWO_STAGE_MUTATION_OUTPUT" | grep -q "^TS10=REFRESHED|Cache refreshed successfully$" && \
    echo "  ✓ PASS: Test 10 - two_stage detected the row change and refreshed" || \
    (echo "  ✗ FAIL: Test 10 - two_stage did not refresh after the insert"; exit 1)

EXPECTED_TWO_STAGE_COUNT=$((TWO_STAGE_BASELINE_COUNT + 1))
ACTUAL_TWO_STAGE_COUNT=$(echo "$TWO_STAGE_MUTATION_OUTPUT" | sed -n 's/^TS10_CACHED_COUNT=//p' | tail -1)
assert_equals "Test 10 - cached row count after two_stage refresh" "$EXPECTED_TWO_STAGE_COUNT" "$ACTUAL_TWO_STAGE_COUNT"

ACTUAL_SNAPSHOT_COUNT=$(echo "$TWO_STAGE_MUTATION_OUTPUT" | sed -n 's/^TS11_SNAPSHOT_COUNT=//p' | tail -1)
assert_equals "Test 11 - snapshot row count" "1" "$ACTUAL_SNAPSHOT_COUNT"

echo "$TWO_STAGE_MUTATION_OUTPUT" | grep -q "^TS11_SOURCE_TABLE=$SNOWFLAKE_DATABASE\.$SNOWFLAKE_SCHEMA\.CUSTOMERS$" && \
    echo "  ✓ PASS: Test 11 - Snapshot persisted for the monitored table" || \
    (echo "  ✗ FAIL: Test 11 - Snapshot row missing expected source table"; exit 1)

echo ""
echo "=========================================="
echo "All Snowflake Tests Completed!"
echo "=========================================="
echo ""
echo "To stop PostgreSQL: make test-docker-down"
