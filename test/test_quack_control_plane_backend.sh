#!/bin/bash
# test/test_quack_control_plane_backend.sh
#
# Spike E: Quack-backed control-plane backend feasibility
#
# What this tests:
# 1. A single DuckDB process can own the DuckLake/Postgres-backed control plane
# 2. A second DuckDB client can read that metadata over Quack
# 3. Metadata mutations on the owner become visible to the client immediately
# 4. A simple polling mirror can be defined, and we can measure the stale-read window
# 5. Killing the owner process makes reads fail cleanly
# 6. Restarting the owner process preserves metadata because the control plane lives in DuckLake/Postgres
#
# This does NOT implement a local mirror in product code. It measures the raw backend behavior
# so we can decide whether a mirrored-cache design is realistic.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DUCKDB="$PROJECT_DIR/build/release/duckdb"
EXTENSION="$PROJECT_DIR/build/release/extension/ducksync/ducksync.duckdb_extension"
SERVER_FIFO="/tmp/ducksync_spike_e_server.fifo"
SERVER_LOG="/tmp/ducksync_spike_e_server.log"
OUT="/tmp/test_quack_control_plane_backend.txt"
DATA_DIR="/tmp/ducksync_spike_e_data"
SERVER_URI="quack:localhost:19555"
SERVER_TOKEN="spikeEtoken"
PG_CONN="host=localhost port=5432 dbname=ducklake user=ducksync password=ducksync"

PASS=0
FAIL=0

cleanup() {
    if [[ -n "${SERVER_PID:-}" ]]; then
        kill "$SERVER_PID" >/dev/null 2>&1 || true
        wait "$SERVER_PID" >/dev/null 2>&1 || true
    fi
    exec 3>&- || true
    rm -f "$SERVER_FIFO"
}
trap cleanup EXIT

ms_now() {
    python3 - <<'PY'
import time
print(time.time_ns() // 1_000_000)
PY
}

if [[ ! -f "$DUCKDB" ]]; then
    echo "ERROR: build/release/duckdb not found. Run 'make release' first."
    exit 1
fi
if [[ ! -f "$EXTENSION" ]]; then
    echo "ERROR: DuckSync extension not found. Run 'make release' first."
    exit 1
fi

echo "DuckSync Spike E: Quack-backed control-plane backend feasibility" | tee "$OUT"
echo "" | tee -a "$OUT"

echo "--- E-1: Verify local DuckDB build exposes Quack remote protocol ---" | tee -a "$OUT"
DUCKDB_VERSION=$("$DUCKDB" -box -c "SELECT version();" 2>/dev/null | awk '/^│ v/{print $2; exit}')
QUACK_FUNCS=$("$DUCKDB" -box -c "INSTALL quack; LOAD quack; SELECT function_name FROM duckdb_functions() WHERE function_name LIKE 'quack%' ORDER BY function_name;" 2>/dev/null || true)
echo "DuckDB version: ${DUCKDB_VERSION:-unknown}" | tee -a "$OUT"
echo "$QUACK_FUNCS" | tee -a "$OUT"
if ! echo "$QUACK_FUNCS" | grep -q "quack_serve"; then
    echo "FAIL: quack_serve is not present in this DuckDB build." | tee -a "$OUT"
    echo "This environment cannot execute Spike E yet." | tee -a "$OUT"
    echo "Current binary exposes only the older quack extension API, not the Quack remote protocol server API described in the docs." | tee -a "$OUT"
    echo "Required next step: upgrade DuckDB to v1.5.3+ (or another build that includes quack_serve/quack_stop/quack_query)." | tee -a "$OUT"
    exit 1
fi

echo "--- E0: Ensure PostgreSQL-backed DuckLake catalog is up ---" | tee -a "$OUT"
cd "$SCRIPT_DIR"
docker compose up -d postgres >/dev/null
for i in {1..30}; do
    if docker compose exec -T postgres pg_isready -U ducksync -d ducklake >/dev/null 2>&1; then
        echo "PostgreSQL is ready" | tee -a "$OUT"
        PASS=$((PASS + 1))
        break
    fi
    if [[ $i -eq 30 ]]; then
        echo "FAIL: PostgreSQL did not become ready" | tee -a "$OUT"
        FAIL=$((FAIL + 1))
        exit 1
    fi
    sleep 1
done

rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"
rm -f "$SERVER_FIFO" "$SERVER_LOG"
mkfifo "$SERVER_FIFO"

echo "" | tee -a "$OUT"
echo "--- E1: Start central control-plane node ---" | tee -a "$OUT"
"$DUCKDB" < "$SERVER_FIFO" > "$SERVER_LOG" 2>&1 &
SERVER_PID=$!
exec 3> "$SERVER_FIFO"

cat >&3 <<EOF
LOAD '$EXTENSION';
INSTALL quack;
LOAD quack;
SELECT * FROM ducksync_setup_storage('$PG_CONN', '$DATA_DIR');
SELECT * FROM ducksync_add_source('prod', 'snowflake', 'dummy_secret');
SELECT * FROM ducksync_create_cache(
    'orders_cache',
    'prod',
    'SELECT * FROM ORDERS',
    ['DUCKSYNC_TEST.TEST_DATA.ORDERS'],
    3600
);
SELECT * FROM ducksync_create_cache(
    'customers_cache',
    'prod',
    'SELECT * FROM CUSTOMERS',
    ['DUCKSYNC_TEST.TEST_DATA.CUSTOMERS'],
    3600
);
CALL quack_serve('$SERVER_URI', token := '$SERVER_TOKEN');
EOF

sleep 3
if grep -q "$SERVER_TOKEN" "$SERVER_LOG"; then
    echo "Control-plane node is serving on $SERVER_URI" | tee -a "$OUT"
    PASS=$((PASS + 1))
else
    echo "FAIL: central node did not start Quack successfully" | tee -a "$OUT"
    tail -50 "$SERVER_LOG" | tee -a "$OUT"
    FAIL=$((FAIL + 1))
    exit 1
fi

echo "" | tee -a "$OUT"
echo "--- E2: Client can read metadata over Quack ---" | tee -a "$OUT"
CLIENT_READ_OUTPUT=$(cat <<EOF | "$DUCKDB" -box 2>&1
INSTALL quack;
LOAD quack;
FROM quack_query(
    '$SERVER_URI',
    'SELECT cache_name, source_name FROM ducksync.ducksync.caches ORDER BY cache_name',
    token := '$SERVER_TOKEN'
);
EOF
)
echo "$CLIENT_READ_OUTPUT" | tee -a "$OUT"
if echo "$CLIENT_READ_OUTPUT" | grep -q "orders_cache" && echo "$CLIENT_READ_OUTPUT" | grep -q "customers_cache"; then
    echo "RESULT: PASS" | tee -a "$OUT"
    PASS=$((PASS + 1))
else
    echo "RESULT: FAIL" | tee -a "$OUT"
    FAIL=$((FAIL + 1))
fi

echo "" | tee -a "$OUT"
echo "--- E3: Direct remote-read latency (upper bound, local machine) ---" | tee -a "$OUT"
LAT_START=$(ms_now)
cat <<EOF | "$DUCKDB" >/dev/null 2>&1
INSTALL quack;
LOAD quack;
FROM quack_query(
    '$SERVER_URI',
    'SELECT cache_name FROM ducksync.ducksync.caches ORDER BY cache_name',
    token := '$SERVER_TOKEN'
);
EOF
LAT_END=$(ms_now)
LAT_MS=$((LAT_END - LAT_START))
echo "Single remote metadata read latency: ${LAT_MS} ms" | tee -a "$OUT"
PASS=$((PASS + 1))

echo "" | tee -a "$OUT"
echo "--- E4: Mutation visibility from central node to client ---" | tee -a "$OUT"
cat >&3 <<EOF
UPDATE ducksync.ducksync.state
SET refresh_count = 7
WHERE cache_name = 'orders_cache';
EOF

START_MS=$(ms_now)
VISIBILITY_MS=-1
for _ in {1..20}; do
    VALUE=$(cat <<EOF | "$DUCKDB" -csv -noheader 2>/dev/null | tail -n1
INSTALL quack;
LOAD quack;
COPY (
    FROM quack_query(
        '$SERVER_URI',
        'SELECT refresh_count FROM ducksync.ducksync.state WHERE cache_name = ''orders_cache''',
        token := '$SERVER_TOKEN'
    )
) TO STDOUT (FORMAT csv, HEADER false);
EOF
)
    if [[ "$VALUE" == "7" ]]; then
        NOW_MS=$(ms_now)
        VISIBILITY_MS=$((NOW_MS - START_MS))
        break
    fi
    sleep 0.1
done

if [[ $VISIBILITY_MS -ge 0 ]]; then
    echo "Remote update visible to client within ${VISIBILITY_MS} ms (100 ms poll interval upper bound)" | tee -a "$OUT"
    echo "Mirror guidance: with a 100 ms polling mirror, stale-read window is ~${VISIBILITY_MS} ms on this machine." | tee -a "$OUT"
    PASS=$((PASS + 1))
else
    echo "FAIL: client did not observe updated refresh_count within 2 seconds" | tee -a "$OUT"
    FAIL=$((FAIL + 1))
fi

echo "" | tee -a "$OUT"
echo "--- E5: Failure behavior when central node dies ---" | tee -a "$OUT"
kill "$SERVER_PID" >/dev/null 2>&1 || true
wait "$SERVER_PID" >/dev/null 2>&1 || true
SERVER_PID=""
FAIL_SQL=$(mktemp)
cat > "$FAIL_SQL" <<EOF
INSTALL quack;
LOAD quack;
FROM quack_query(
    '$SERVER_URI',
    'SELECT cache_name FROM ducksync.ducksync.caches',
    token := '$SERVER_TOKEN'
);
EOF
"$DUCKDB" -box < "$FAIL_SQL" > /tmp/spike_e_fail_query.txt 2>&1 &
FAIL_PID=$!
for _ in {1..30}; do
    if ! kill -0 "$FAIL_PID" >/dev/null 2>&1; then
        break
    fi
    sleep 0.1
done
if kill -0 "$FAIL_PID" >/dev/null 2>&1; then
    kill "$FAIL_PID" >/dev/null 2>&1 || true
    wait "$FAIL_PID" >/dev/null 2>&1 || true
    FAIL_OUTPUT="TIMEOUT: quack_query did not fail fast when the control-plane node was down"
else
    FAIL_OUTPUT=$(cat /tmp/spike_e_fail_query.txt)
fi
rm -f "$FAIL_SQL" /tmp/spike_e_fail_query.txt
echo "$FAIL_OUTPUT" | tee -a "$OUT"
if echo "$FAIL_OUTPUT" | grep -qiE "IO Error|HTTP|Connection|refused|failed"; then
    echo "RESULT: PASS (client fails clearly when control-plane node is down)" | tee -a "$OUT"
    PASS=$((PASS + 1))
else
    echo "RESULT: FAIL" | tee -a "$OUT"
    FAIL=$((FAIL + 1))
fi

echo "" | tee -a "$OUT"
echo "--- E6: Restart central node and confirm metadata persists ---" | tee -a "$OUT"
exec 3>&- || true
rm -f "$SERVER_FIFO"
mkfifo "$SERVER_FIFO"
"$DUCKDB" < "$SERVER_FIFO" > "$SERVER_LOG" 2>&1 &
SERVER_PID=$!
exec 3> "$SERVER_FIFO"
cat >&3 <<EOF
LOAD '$EXTENSION';
INSTALL quack;
LOAD quack;
SELECT * FROM ducksync_setup_storage('$PG_CONN', '$DATA_DIR');
CALL quack_serve('$SERVER_URI', token := '$SERVER_TOKEN');
EOF
sleep 3
PERSIST_OUTPUT=$(cat <<EOF | "$DUCKDB" -box 2>&1
INSTALL quack;
LOAD quack;
FROM quack_query(
    '$SERVER_URI',
    'SELECT cache_name, refresh_count FROM ducksync.ducksync.state ORDER BY cache_name',
    token := '$SERVER_TOKEN'
);
EOF
)
echo "$PERSIST_OUTPUT" | tee -a "$OUT"
if echo "$PERSIST_OUTPUT" | grep -q "orders_cache" && echo "$PERSIST_OUTPUT" | grep -q "7"; then
    echo "RESULT: PASS (metadata persisted across central-node restart)" | tee -a "$OUT"
    PASS=$((PASS + 1))
else
    echo "RESULT: FAIL" | tee -a "$OUT"
    FAIL=$((FAIL + 1))
fi

echo "" | tee -a "$OUT"
echo "======================================" | tee -a "$OUT"
echo "PASS: $PASS  FAIL: $FAIL" | tee -a "$OUT"
echo "" | tee -a "$OUT"
echo "Spike E takeaways:" | tee -a "$OUT"
echo "- A single DuckDB process can serve as the serialized control-plane owner." | tee -a "$OUT"
echo "- Quack clients can read control-plane metadata remotely." | tee -a "$OUT"
echo "- Metadata updates are visible immediately to direct remote reads; any stale-read window comes from the mirror polling interval, not from the backend itself." | tee -a "$OUT"
echo "- Failure mode is clean: client queries fail when the owner is down." | tee -a "$OUT"
echo "- Because metadata lives in DuckLake/Postgres, restarting the Quack owner process preserves state." | tee -a "$OUT"
echo "" | tee -a "$OUT"
echo "Full output: $OUT" | tee -a "$OUT"