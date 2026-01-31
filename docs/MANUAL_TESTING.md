# Manual Testing Guide

End-to-end testing guide for DuckSync with a real Snowflake instance.

## Prerequisites

- DuckSync built: `make release`
- Docker running: `docker compose -f test/docker-compose.yml up -d postgres`
- Snowflake credentials in `.env` (see [test/README.md](../test/README.md))
- ADBC driver installed (see [duckdb-snowflake setup](https://github.com/iqea-ai/duckdb-snowflake#adbc-driver-setup))

## Quick Test

```bash
./docs/manual_test.sh
```

## Step-by-Step Testing

### 1. Start DuckDB

```bash
cd ducksync
./build/release/duckdb
```

### 2. Load Extensions

```sql
INSTALL snowflake FROM community;
LOAD snowflake;
LOAD 'build/release/extension/ducksync/ducksync.duckdb_extension';

-- Verify: should show 7 functions
SELECT function_name FROM duckdb_functions() WHERE function_name LIKE 'ducksync%';
```

### 3. Create Snowflake Secret

```sql
CREATE SECRET sf_test (
    TYPE snowflake,
    ACCOUNT 'your-account-id',
    USER 'your-username',
    PASSWORD 'your-password-or-pat',
    DATABASE 'your-database',
    WAREHOUSE 'your-warehouse'
);

-- Verify connection
SELECT * FROM snowflake_query('SELECT CURRENT_USER(), CURRENT_ROLE()', 'sf_test');
```

### 4. Initialize DuckSync

```sql
SELECT * FROM ducksync_setup_storage(
    'host=localhost port=5432 dbname=ducklake user=ducksync password=ducksync',
    './test/data/manual_test'
);
```

### 5. Add Source & Create Cache

```sql
-- Register Snowflake source (source_name, driver_type, secret_name)
SELECT * FROM ducksync_add_source('prod', 'snowflake', 'sf_test');

-- Create cache with 1-hour TTL
SELECT * FROM ducksync_create_cache(
    'customers_cache',
    'prod',
    'SELECT * FROM YOUR_DB.YOUR_SCHEMA.CUSTOMERS',
    ['YOUR_DB.YOUR_SCHEMA.CUSTOMERS'],
    3600
);
```

### 6. Test Query Routing

```sql
-- Cache hit via monitored table name
SELECT * FROM ducksync_query(
    'SELECT * FROM YOUR_DB.YOUR_SCHEMA.CUSTOMERS WHERE id = 1',
    'prod'
);

-- Cache hit via named query (use cache name directly)
SELECT * FROM ducksync_query(
    'SELECT * FROM customers_cache LIMIT 5',
    'prod'
);

-- Direct DuckLake access (standard DuckDB - no TTL checks)
SELECT * FROM ducksync.prod.customers_cache;

-- Passthrough: uncached table goes to Snowflake
SELECT * FROM ducksync_query(
    'SELECT * FROM YOUR_DB.YOUR_SCHEMA.OTHER_TABLE',
    'prod'
);
```

### 7. Test Refresh

```sql
-- Manual refresh (skips if data unchanged)
SELECT * FROM ducksync_refresh('customers_cache');

-- Force refresh
SELECT * FROM ducksync_refresh('customers_cache', true);
```

### 8. Cleanup

```bash
make clean-test-data
make test-docker-down
```

---

## Test Checklist

| Test | Command | Expected |
|------|---------|----------|
| Extension loads | `LOAD ducksync` | No errors |
| Functions registered | `SELECT ... WHERE function_name LIKE 'ducksync%'` | 7 functions |
| Snowflake connects | `snowflake_query(...)` | User/role displayed |
| Storage init | `ducksync_setup_storage(...)` | Success message |
| Add source | `ducksync_add_source(...)` | Source created |
| Create cache | `ducksync_create_cache(...)` | Cache created |
| Refresh cache | `ducksync_refresh(...)` | Data loaded |
| Direct DuckLake | `SELECT * FROM ducksync.prod.X` | Returns cached data |
| Query cache hit (table) | `ducksync_query(monitored_table)` | Fast, local data |
| Query cache hit (named) | `ducksync_query(cache_name)` | Fast, local data |
| Query passthrough | `ducksync_query(uncached_table)` | Snowflake data |
| TTL auto-refresh | Query expired cache | Refreshes then returns |
| Manual refresh | `ducksync_refresh(...)` | Completes or skips |

## Error Scenarios

| Scenario | Trigger | Expected |
|----------|---------|----------|
| Not initialized | Call functions before `ducksync_init` | "DuckSync not initialized" |
| Invalid credentials | Wrong password in secret | Auth error from Snowflake |
| Table not found | Query non-existent table | Snowflake error message |
| Missing secret | Wrong secret name | "Secret not found" |
| Invalid SQL | Malformed query | Parser error |

## Performance Testing

```sql
.timer on
-- Compare cache hit vs passthrough timing
SELECT * FROM ducksync_query('SELECT * FROM CACHED_TABLE', 'prod');
SELECT * FROM ducksync_query('SELECT * FROM UNCACHED_TABLE', 'prod');
```
