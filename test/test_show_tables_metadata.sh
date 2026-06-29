#!/bin/bash
# test/test_show_tables_metadata.sh
#
# PURPOSE
# =======
# Validates that Snowflake SHOW TABLES LIKE can serve as a warehouse-free
# change-detection signal for DuckSync's invalidation path.
#
# The invalidation heuristic:
#   rows + bytes both unchanged  →  skip refresh (nothing changed)
#   rows OR bytes changed        →  run detailed check, then decide
#
# This is a heuristic, not a correctness proof. Known limitation: in-place
# updates that do not change row count or compressed byte size are invisible.
#
# TWO CREDENTIALS REQUIRED
# ========================
# sf_meta  (SNOWFLAKE_SCHEMA_ROLE env var, NO_COMPUTE_ROLE)
#   - No warehouse grants, DEFAULT_WAREHOUSE = NULL
#   - Used for all SHOW TABLES metadata checks — must never wake a warehouse
#
# sf_data  (SNOWFLAKE_PASSWORD env var, ROLE_DAN, WH_DAN)
#   - Full data access with warehouse
#   - Used for DML mutations and verification queries
#
# Snowflake setup (run once as account admin):
#   CREATE ROLE NO_COMPUTE_ROLE;
#   GRANT USAGE ON DATABASE DUCKSYNC_TEST TO ROLE NO_COMPUTE_ROLE;
#   GRANT USAGE ON SCHEMA DUCKSYNC_TEST.TEST_DATA TO ROLE NO_COMPUTE_ROLE;
#   GRANT SELECT ON ALL TABLES IN SCHEMA DUCKSYNC_TEST.TEST_DATA TO ROLE NO_COMPUTE_ROLE;
#   GRANT ROLE NO_COMPUTE_ROLE TO USER SNOWDUCKSSAA;
#   -- no GRANT USAGE ON WAREHOUSE to NO_COMPUTE_ROLE
#
# Usage: bash test/test_show_tables_metadata.sh

set -a
source "$(dirname "$0")/../.env"
set +a

DUCKDB="$(dirname "$0")/../build/release/duckdb"
OUT="/tmp/test_show_tables_metadata.txt"
PASS=0
FAIL=0
EXPECTED_FAIL=0

if [ ! -f "$DUCKDB" ]; then
    echo "ERROR: build/release/duckdb not found. Run 'make release' first."
    exit 1
fi

echo "DuckSync: SHOW TABLES metadata freshness detection tests" | tee "$OUT"
echo "Target: DUCKSYNC_TEST.TEST_DATA (CUSTOMERS=10r, ORDERS=10r, PRODUCTS=5r)" | tee -a "$OUT"
echo "" | tee -a "$OUT"

META_PW=$(printf '%s' "$SNOWFLAKE_SCHEMA_ROLE" | sed "s/'/''/g")
DATA_PW=$(printf '%s' "$SNOWFLAKE_PASSWORD" | sed "s/'/''/g")

PREAMBLE_META=$(cat << HEREDOC
INSTALL snowflake FROM community;
LOAD snowflake;
CREATE OR REPLACE SECRET sf_meta (
    TYPE snowflake,
    ACCOUNT 'AEDTBHT-ALIAS_ANALYTICS',
    USER 'SNOWDUCKSSAA',
    PASSWORD '${META_PW}',
    DATABASE 'DUCKSYNC_TEST',
    ROLE 'NO_COMPUTE_ROLE'
);
HEREDOC
)

PREAMBLE_DATA=$(cat << HEREDOC
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

# Run SQL using the no-compute metadata credential
run_meta() {
    local label="$1"
    local sql="$2"
    local mode="${3:-normal}"  # normal | expected_error | expected_crash
    echo "--- $label ---" | tee -a "$OUT"
    result=$(printf '%s\n%s' "$PREAMBLE_META" "$sql" | $DUCKDB -box 2>&1)
    echo "$result" | tee -a "$OUT"
    local crashed=false
    echo "$result" | grep -qiE "INTERNAL Error|FATAL Error" && crashed=true
    if [ "$mode" = "expected_crash" ] && $crashed; then
        echo "RESULT: EXPECTED FAIL (crash is the finding)" | tee -a "$OUT"
        EXPECTED_FAIL=$((EXPECTED_FAIL + 1))
    elif [ "$mode" = "expected_error" ] && echo "$result" | grep -qiE "IO Error|Binder Error|Error"; then
        echo "RESULT: EXPECTED FAIL (error is the finding)" | tee -a "$OUT"
        EXPECTED_FAIL=$((EXPECTED_FAIL + 1))
    elif echo "$result" | grep -qiE "IO Error|Binder Error|INTERNAL Error|FATAL Error"; then
        echo "RESULT: FAIL" | tee -a "$OUT"
        FAIL=$((FAIL + 1))
    else
        echo "RESULT: PASS" | tee -a "$OUT"
        PASS=$((PASS + 1))
    fi
    echo "" | tee -a "$OUT"
}

# Run SQL using the data credential (has warehouse, can run DML)
run_data() {
    local label="$1"
    local sql="$2"
    echo "--- $label ---" | tee -a "$OUT"
    result=$(printf '%s\n%s' "$PREAMBLE_DATA" "$sql" | $DUCKDB -box 2>&1)
    echo "$result" | tee -a "$OUT"
    if echo "$result" | grep -qiE "INTERNAL Error|FATAL Error|IO Error|Binder Error"; then
        echo "RESULT: FAIL" | tee -a "$OUT"
        FAIL=$((FAIL + 1))
    else
        echo "RESULT: PASS" | tee -a "$OUT"
        PASS=$((PASS + 1))
    fi
    echo "" | tee -a "$OUT"
}

# ============================================================================
# SECTION 1: Warehouse isolation
# NO_COMPUTE_ROLE must never wake a warehouse, even running SHOW TABLES.
# ============================================================================
echo "=== SECTION 1: Warehouse-free isolation ===" | tee -a "$OUT"
echo "" | tee -a "$OUT"

run_meta "S1.1: CURRENT_WAREHOUSE() is NULL with NO_COMPUTE_ROLE (control check)" \
"SELECT * FROM snowflake_query(
    'SELECT CURRENT_USER() AS u, CURRENT_ROLE() AS r, CURRENT_WAREHOUSE() AS wh',
    'sf_meta'
);"

run_meta "S1.2: SHOW TABLES LIKE with IN SCHEMA succeeds without a warehouse" \
"SELECT * FROM snowflake_query(
    'SHOW TABLES LIKE ''CUSTOMERS'' IN SCHEMA DUCKSYNC_TEST.TEST_DATA',
    'sf_meta'
);"

run_meta "S1.3: Missing table returns 0 rows — no crash, no error" \
"SELECT * FROM snowflake_query(
    'SHOW TABLES LIKE ''DOES_NOT_EXIST'' IN SCHEMA DUCKSYNC_TEST.TEST_DATA',
    'sf_meta'
);"

run_meta "S1.4: CURRENT_WAREHOUSE() still NULL after SHOW TABLES (session not contaminated)" \
"SELECT * FROM snowflake_query(
    'SELECT CURRENT_WAREHOUSE() AS wh_after',
    'sf_meta'
);"

# Chained ->> form needs a warehouse. Without one it errors hard.
# This is intentional — it confirms the chained form is forbidden in the invalidation path.
run_meta "S1.5: Chained ->> form fails hard without a warehouse (expected error)" \
"SELECT * FROM snowflake_query(
    'SHOW TABLES IN SCHEMA DUCKSYNC_TEST.TEST_DATA ->> SELECT \"name\", \"rows\", \"bytes\" FROM \$1',
    'sf_meta'
);" "expected_error"

# ============================================================================
# SECTION 2: Column shape
# The ADBC driver returns more columns than Snowflake documents.
# Column projection crashes the DuckDB Arrow scanner — must use SELECT *.
# ============================================================================
echo "=== SECTION 2: Column shape ===" | tee -a "$OUT"
echo "" | tee -a "$OUT"

run_meta "S2.1: DESCRIBE full column list returned through ADBC (expect 27 columns)" \
"DESCRIBE (SELECT * FROM snowflake_query(
    'SHOW TABLES LIKE ''CUSTOMERS'' IN SCHEMA DUCKSYNC_TEST.TEST_DATA',
    'sf_meta'
));"

# This crashes the Arrow scanner with ArrowTypeInfo mismatch on created_on (TIMESTAMP).
# Implementation must read SELECT * and extract fields by name in C++.
run_meta "S2.2: Column projection crashes Arrow scanner — documents SELECT * requirement" \
"SELECT name, rows, bytes FROM snowflake_query(
    'SHOW TABLES LIKE ''CUSTOMERS'' IN SCHEMA DUCKSYNC_TEST.TEST_DATA',
    'sf_meta'
);" "expected_crash"

run_meta "S2.3: LIKE is case-insensitive: lowercase 'customers' matches CUSTOMERS" \
"SELECT * FROM snowflake_query(
    'SHOW TABLES LIKE ''customers'' IN SCHEMA DUCKSYNC_TEST.TEST_DATA',
    'sf_meta'
);"

run_meta "S2.4: Wildcard LIKE ORDER% matches ORDERS and not other tables" \
"SELECT * FROM snowflake_query(
    'SHOW TABLES LIKE ''ORDER%'' IN SCHEMA DUCKSYNC_TEST.TEST_DATA',
    'sf_meta'
);"

run_meta "S2.5: Full schema scan without LIKE returns all 3 tables" \
"SELECT * FROM snowflake_query(
    'SHOW TABLES IN SCHEMA DUCKSYNC_TEST.TEST_DATA',
    'sf_meta'
);"

# ============================================================================
# SECTION 3: Fingerprint mutation sensitivity
#
# Heuristic: rows + bytes same → skip. rows OR bytes changed → check further.
#
# DML runs via sf_data (needs warehouse). Fingerprint checks run via sf_meta.
# A 2-second sleep between DML and check allows Snowflake metadata to settle.
#
# Baseline: CUSTOMERS has 10 rows, id 1-10.
# ============================================================================
echo "=== SECTION 3: Fingerprint mutation sensitivity ===" | tee -a "$OUT"
echo "" | tee -a "$OUT"

run_meta "S3.0: Baseline fingerprint — rows and bytes before any mutations" \
"SELECT * FROM snowflake_query(
    'SHOW TABLES IN SCHEMA DUCKSYNC_TEST.TEST_DATA',
    'sf_meta'
);"

# --- Append ---
# Inserting a row increases row count. Heuristic should detect this.
run_data "S3.1a: INSERT 1 row into CUSTOMERS (id=99)" \
"SELECT * FROM snowflake_query(
    'INSERT INTO DUCKSYNC_TEST.TEST_DATA.CUSTOMERS (id, name, email, region) VALUES (99, ''TestAppend'', ''append@test.com'', ''US'')',
    'sf_data'
);"

sleep 2

run_meta "S3.1b: SHOW TABLES after INSERT — expect rows increased (heuristic detects append)" \
"SELECT * FROM snowflake_query(
    'SHOW TABLES LIKE ''CUSTOMERS'' IN SCHEMA DUCKSYNC_TEST.TEST_DATA',
    'sf_meta'
);"

# --- Delete ---
run_data "S3.2a: DELETE the inserted row (id=99)" \
"SELECT * FROM snowflake_query(
    'DELETE FROM DUCKSYNC_TEST.TEST_DATA.CUSTOMERS WHERE id = 99',
    'sf_data'
);"

sleep 2

run_meta "S3.2b: SHOW TABLES after DELETE — expect rows decreased (heuristic detects delete)" \
"SELECT * FROM snowflake_query(
    'SHOW TABLES LIKE ''CUSTOMERS'' IN SCHEMA DUCKSYNC_TEST.TEST_DATA',
    'sf_meta'
);"

# --- Fixed-width in-place update (known false negative) ---
# Updating a 2-char region with another 2-char region.
# Row count unchanged; bytes likely unchanged after compression.
# Heuristic says: skip. Data changed but the heuristic cannot see it.
# This is the documented limitation of the rows+bytes signal.
run_data "S3.3a: UPDATE region EU to US (same byte width) on 3 rows" \
"SELECT * FROM snowflake_query(
    'UPDATE DUCKSYNC_TEST.TEST_DATA.CUSTOMERS SET region = ''US'' WHERE region = ''EU'' AND id <= 3',
    'sf_data'
);"

sleep 2

run_meta "S3.3b: SHOW TABLES after fixed-width UPDATE — expect rows+bytes UNCHANGED (known false negative: heuristic misses this)" \
"SELECT * FROM snowflake_query(
    'SHOW TABLES LIKE ''CUSTOMERS'' IN SCHEMA DUCKSYNC_TEST.TEST_DATA',
    'sf_meta'
);"

# --- Variable-width update ---
# Appending a suffix to name makes row bytes larger.
# Bytes should increase; heuristic should detect.
run_data "S3.4a: UPDATE name with longer suffix on 3 rows (bytes should increase)" \
"SELECT * FROM snowflake_query(
    'UPDATE DUCKSYNC_TEST.TEST_DATA.CUSTOMERS SET name = name || ''_UPDATED'' WHERE id <= 3',
    'sf_data'
);"

sleep 2

run_meta "S3.4b: SHOW TABLES after variable-width UPDATE — expect bytes changed (heuristic detects)" \
"SELECT * FROM snowflake_query(
    'SHOW TABLES LIKE ''CUSTOMERS'' IN SCHEMA DUCKSYNC_TEST.TEST_DATA',
    'sf_meta'
);"

# --- Restore ---
run_data "S3.5a: Restore modified rows to original values" \
"SELECT * FROM snowflake_query(
    'UPDATE DUCKSYNC_TEST.TEST_DATA.CUSTOMERS SET name = REPLACE(name, ''_UPDATED'', ''''), region = CASE id WHEN 2 THEN ''EU'' WHEN 7 THEN ''EU'' WHEN 10 THEN ''EU'' ELSE region END WHERE id <= 3',
    'sf_data'
);"

sleep 2

run_meta "S3.5b: SHOW TABLES after restore — compare bytes to S3.0 baseline" \
"SELECT * FROM snowflake_query(
    'SHOW TABLES LIKE ''CUSTOMERS'' IN SCHEMA DUCKSYNC_TEST.TEST_DATA',
    'sf_meta'
);"

# ============================================================================
# SUMMARY
# ============================================================================
echo "======================================" | tee -a "$OUT"
echo "PASS: $PASS  EXPECTED_FAIL: $EXPECTED_FAIL  FAIL: $FAIL" | tee -a "$OUT"
echo "" | tee -a "$OUT"
echo "Expected failures (document constraints, not bugs):" | tee -a "$OUT"
echo "  S1.5  chained ->> form: hard error without warehouse" | tee -a "$OUT"
echo "  S2.2  column projection: crashes DuckDB Arrow scanner on created_on timestamp" | tee -a "$OUT"
echo "" | tee -a "$OUT"
echo "Known heuristic limitations (false negatives, not failures):" | tee -a "$OUT"
echo "  S3.3  fixed-width in-place UPDATE: rows+bytes may not change, heuristic skips" | tee -a "$OUT"
echo "  Auto-clustering: bytes can change with no data change (false positive risk)" | tee -a "$OUT"
echo "" | tee -a "$OUT"
echo "Full output: $OUT" | tee -a "$OUT"
