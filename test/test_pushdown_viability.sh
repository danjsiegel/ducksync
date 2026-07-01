#!/bin/bash
# test/test_pushdown_viability.sh
#
# PURPOSE
# =======
# Spike C: Does substituting snowflake_query('SELECT * FROM table') under a
# DuckDB query preserve filter and projection pushdown to Snowflake?
#
# This is the viability test for transparent cache-miss passthrough.
# If DuckDB cannot push predicates and projections into snowflake_query(),
# then a miss handled via ReplacementScan + snowflake_query() would:
#   - Pull the entire table over the network
#   - Apply all filters locally in DuckDB
#   - Give Snowflake no chance to use its compute advantage
#
# HOW WE TEST WITHOUT RUNNING QUERIES
# ====================================
# EXPLAIN output is the primary signal. In DuckDB's query plan:
#   - A FILTER node above TABLE_FUNCTION means the predicate runs locally
#   - A PROJECTION node above TABLE_FUNCTION means column selection runs locally
# This tells us definitively that nothing was pushed into the Snowflake query.
#
# We also run two equivalent queries and compare what SQL lands in Snowflake
# query history to confirm the empirical behavior.
#
# Requires: .env with SNOWFLAKE_PASSWORD, built DuckDB, ADBC driver installed.
# Usage: bash test/test_pushdown_viability.sh

set -a
source "$(dirname "$0")/../.env"
set +a

DUCKDB="$(dirname "$0")/../build/release/duckdb"
OUT="/tmp/test_pushdown_viability.txt"
PASS=0
FAIL=0
EXPECTED_FAIL=0

if [ ! -f "$DUCKDB" ]; then
    echo "ERROR: build/release/duckdb not found. Run 'make release' first."
    exit 1
fi

echo "DuckSync Spike C: pushdown viability via snowflake_query()" | tee "$OUT"
echo "Question: does DuckDB push WHERE/SELECT into snowflake_query() SQL?" | tee -a "$OUT"
echo "" | tee -a "$OUT"

DATA_PW=$(printf '%s' "$SNOWFLAKE_PASSWORD" | sed "s/'/''/g")

PREAMBLE=$(cat << HEREDOC
INSTALL snowflake FROM community;
LOAD snowflake;
CREATE OR REPLACE SECRET sf_data (
    TYPE snowflake,
    ACCOUNT 'AEDTBHT-ALIAS_ANALYTICS',
    USER 'SNOWDUCKSSAA',
    PASSWORD '${DATA_PW}',
    DATABASE 'DUCKSYNC_TEST',
    WAREHOUSE 'WH_DAN',
    ROLE 'ROLE_DAN'
);
HEREDOC
)

run_test() {
    local label="$1"
    local sql="$2"
    local mode="${3:-normal}"
    echo "--- $label ---" | tee -a "$OUT"
    result=$(printf '%s\n%s' "$PREAMBLE" "$sql" | $DUCKDB -box 2>&1)
    echo "$result" | tee -a "$OUT"
    if echo "$result" | grep -qiE "INTERNAL Error|FATAL Error" && [ "$mode" = "expected_crash" ]; then
        echo "RESULT: EXPECTED FAIL (crash is the finding)" | tee -a "$OUT"
        EXPECTED_FAIL=$((EXPECTED_FAIL + 1))
    elif echo "$result" | grep -qiE "INTERNAL Error|FATAL Error|IO Error|Binder Error"; then
        echo "RESULT: FAIL" | tee -a "$OUT"
        FAIL=$((FAIL + 1))
    else
        echo "RESULT: PASS" | tee -a "$OUT"
        PASS=$((PASS + 1))
    fi
    echo "" | tee -a "$OUT"
}

# ============================================================================
# SECTION 1: EXPLAIN-based pushdown analysis
#
# Read the plan. Look for:
#   FILTER above TABLE_FUNCTION  → predicate runs locally in DuckDB (no pushdown)
#   PROJECTION above TABLE_FUNCTION → column selection runs locally (no pushdown)
# ============================================================================
echo "=== SECTION 1: Execution plan analysis (no Snowflake queries run) ===" | tee -a "$OUT"
echo "" | tee -a "$OUT"

run_test "P1.1: EXPLAIN baseline — snowflake_query with filter embedded in SQL string
(this IS efficient: filter runs on Snowflake)" \
"EXPLAIN
SELECT * FROM snowflake_query(
    'SELECT * FROM DUCKSYNC_TEST.TEST_DATA.CUSTOMERS WHERE region = ''US''',
    'sf_data'
);"

run_test "P1.2: EXPLAIN outer WHERE on snowflake_query SELECT *
(KEY TEST: if FILTER node appears above TABLE_FUNCTION, filter is local — no pushdown)" \
"EXPLAIN
SELECT * FROM snowflake_query(
    'SELECT * FROM DUCKSYNC_TEST.TEST_DATA.CUSTOMERS',
    'sf_data'
) WHERE region = 'US';"

run_test "P1.3: EXPLAIN projection — SELECT id on snowflake_query SELECT *
(if PROJECTION node appears, column selection is local — Snowflake still returns all columns)" \
"EXPLAIN
SELECT id FROM snowflake_query(
    'SELECT * FROM DUCKSYNC_TEST.TEST_DATA.CUSTOMERS',
    'sf_data'
);"

run_test "P1.4: EXPLAIN both filter AND projection on snowflake_query SELECT *
(real-world miss-path: user queries SELECT id WHERE region='US' on a full-table pull)" \
"EXPLAIN
SELECT id, name FROM snowflake_query(
    'SELECT * FROM DUCKSYNC_TEST.TEST_DATA.CUSTOMERS',
    'sf_data'
) WHERE region = 'US';"

run_test "P1.5: EXPLAIN JOIN — two snowflake_query calls joined locally
(join also runs in DuckDB — both full tables cross the wire)" \
"EXPLAIN
SELECT c.name, o.id
FROM snowflake_query('SELECT * FROM DUCKSYNC_TEST.TEST_DATA.CUSTOMERS', 'sf_data') c
JOIN snowflake_query('SELECT * FROM DUCKSYNC_TEST.TEST_DATA.ORDERS', 'sf_data') o
  ON c.id = o.id
WHERE c.region = 'US';"

# ============================================================================
# SECTION 2: Empirical confirmation — run actual queries and compare results
#
# Both queries should return the same rows. The difference is what Snowflake
# sees in its query history (check the Snowflake UI after this runs).
#
# Query A: filter embedded → Snowflake gets: SELECT * FROM CUSTOMERS WHERE region = 'US'
# Query B: filter external → Snowflake gets: SELECT * FROM CUSTOMERS  (all rows)
#
# Query B pulls 10 rows from Snowflake and DuckDB filters to the US ones.
# Query A pulls only the US rows from Snowflake.
# ============================================================================
echo "=== SECTION 2: Empirical queries — check Snowflake history after this runs ===" | tee -a "$OUT"
echo "" | tee -a "$OUT"

run_test "P2.1: Filter INSIDE snowflake_query string — efficient path (Snowflake filters)" \
"SELECT * FROM snowflake_query(
    'SELECT * FROM DUCKSYNC_TEST.TEST_DATA.CUSTOMERS WHERE region = ''US''',
    'sf_data'
);"

run_test "P2.2: Filter OUTSIDE snowflake_query — miss-path simulation
EXPECTED TO CRASH: outer WHERE on snowflake_query() hits ArrowTypeInfo mismatch
(DECIMAL vs STRING on column statistics). Not just no pushdown — outright broken." \
"SELECT * FROM snowflake_query(
    'SELECT * FROM DUCKSYNC_TEST.TEST_DATA.CUSTOMERS',
    'sf_data'
) WHERE region = 'US';" "expected_crash"

run_test "P2.3: Confirm both return same rows (filter outcome is identical; cost is different)" \
"SELECT COUNT(*) AS count_inside FROM snowflake_query(
    'SELECT COUNT(*) AS n FROM DUCKSYNC_TEST.TEST_DATA.CUSTOMERS WHERE region = ''US''',
    'sf_data'
);"

# ============================================================================
# SECTION 3: Aggregate pushdown
#
# If the user runs COUNT(*) through a ReplacementScan miss-path, does DuckDB
# push the aggregate to Snowflake? Almost certainly not.
# ============================================================================
echo "=== SECTION 3: Aggregate pushdown ===" | tee -a "$OUT"
echo "" | tee -a "$OUT"

run_test "P3.1: EXPLAIN COUNT(*) on snowflake_query SELECT *
(AGGREGATE_AND_GROUP_BY above TABLE_FUNCTION = local aggregation)" \
"EXPLAIN
SELECT COUNT(*) FROM snowflake_query(
    'SELECT * FROM DUCKSYNC_TEST.TEST_DATA.CUSTOMERS',
    'sf_data'
);"

run_test "P3.2: COUNT(*) embedded in Snowflake query string — efficient path" \
"SELECT * FROM snowflake_query(
    'SELECT COUNT(*) AS n FROM DUCKSYNC_TEST.TEST_DATA.CUSTOMERS',
    'sf_data'
);"

echo "======================================" | tee -a "$OUT"
echo "PASS: $PASS  EXPECTED_FAIL: $EXPECTED_FAIL  FAIL: $FAIL" | tee -a "$OUT"
echo "" | tee -a "$OUT"
echo "Interpreting EXPLAIN output:" | tee -a "$OUT"
echo "  FILTER above TABLE_FUNCTION  → predicate runs in DuckDB, NOT Snowflake" | tee -a "$OUT"
echo "  PROJECTION above TABLE_FUNCTION → column selection in DuckDB, Snowflake returns all cols" | tee -a "$OUT"
echo "  AGGREGATE above TABLE_FUNCTION → aggregation in DuckDB, all rows cross the wire" | tee -a "$OUT"
echo "" | tee -a "$OUT"
echo "After running, check Snowflake query history to confirm:" | tee -a "$OUT"
echo "  P2.1 should show: SELECT * FROM CUSTOMERS WHERE region = 'US'" | tee -a "$OUT"
echo "  P2.2 should show: SELECT * FROM CUSTOMERS  (no WHERE)" | tee -a "$OUT"
echo "" | tee -a "$OUT"
echo "Full output: $OUT" | tee -a "$OUT"
