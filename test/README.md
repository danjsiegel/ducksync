# DuckSync Testing Guide

This guide explains how to run DuckSync tests locally and in CI/CD.

## Test Types

| Test | Dependencies | Description |
|------|--------------|-------------|
| `test_basic.test` | None | Unit tests - no external services |
| `run_tests.sh` | PostgreSQL (Docker) | Integration tests with DuckLake |
| `run_snowflake_tests.sh` | PostgreSQL + Snowflake | Full end-to-end tests |

## Quick Start

### 1. Build the Extension

```bash
make release
```

### 2. Run Basic Tests (No Dependencies)

```bash
# Uses DuckDB's sqllogictest
./build/release/duckdb -c "LOAD 'build/release/extension/ducksync/ducksync.duckdb_extension';"
```

### 3. Run Integration Tests (PostgreSQL Only)

```bash
# Starts PostgreSQL via Docker, runs tests
./test/run_tests.sh

# Stop PostgreSQL when done
make test-docker-down
```

### 4. Run Snowflake Integration Tests

Requires a Snowflake account with test data set up.

```bash
# 1. Copy and fill in credentials
cp .env.example .env
# Edit .env with your Snowflake credentials

# 2. Run Snowflake setup scripts in your Snowflake account:
#    - scripts/setup_snowflake_permissions.sql (as ACCOUNTADMIN)
#    - scripts/setup_test_snowflake.sql (creates test tables)

# 3. Run tests
./test/run_snowflake_tests.sh
```

## Snowflake Setup

### Prerequisites

- Snowflake account with ACCOUNTADMIN access (for initial setup)
- A warehouse (default: `COMPUTE_WH`)
- DuckDB v1.4.4+ (use `brew upgrade duckdb` to update)
- ADBC Snowflake driver (see below)

### Step 0: Install ADBC Snowflake Driver

The DuckDB Snowflake extension requires the Apache Arrow ADBC driver. 

**Follow the setup instructions here:** [iqea-ai/duckdb-snowflake - ADBC Driver Setup](https://github.com/iqea-ai/duckdb-snowflake#adbc-driver-setup)

### Step 1: Create Service Account

Run `scripts/setup_snowflake_permissions.sql` as ACCOUNTADMIN:

```sql
-- Creates:
-- - DUCKSYNC_TEST_ROLE (read-only role)
-- - DUCKSYNC_TEST database and TEST_DATA schema
-- - DUCKSYNC_TEST_USER service account
```

### Step 2: Create Test Data

Run `scripts/setup_test_snowflake.sql`:

```sql
-- Creates tables:
-- - CUSTOMERS (10 rows)
-- - PRODUCTS (5 rows)
-- - ORDERS (10 rows)
```

### Step 3: Configure Credentials

Copy `.env.example` to `.env` and fill in:

```bash
SNOWFLAKE_ACCOUNT=your_account.us-east-1
SNOWFLAKE_USER=DUCKSYNC_TEST_USER
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=DUCKSYNC_TEST
SNOWFLAKE_SCHEMA=TEST_DATA
```

### Minimum Permissions

The test service account needs:

- `USAGE` on database `DUCKSYNC_TEST`
- `USAGE` on schema `DUCKSYNC_TEST.TEST_DATA`
- `SELECT` on all tables in `TEST_DATA`
- `USAGE` on warehouse
- Access to `information_schema.tables` (default for all users)

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SNOWFLAKE_ACCOUNT` | Yes | Account identifier (e.g., `abc123.us-east-1`) |
| `SNOWFLAKE_USER` | Yes | Username |
| `SNOWFLAKE_PASSWORD` | Yes | Password or PAT token |
| `SNOWFLAKE_WAREHOUSE` | Yes | Warehouse name |
| `SNOWFLAKE_DATABASE` | Yes | Database name |
| `SNOWFLAKE_SCHEMA` | No | Default schema (default: `TEST_DATA`) |
| `SNOWFLAKE_ROLE` | No | Role to use (default: user's default role) |

## CI/CD

For GitHub Actions, set secrets:

```yaml
env:
  SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
  SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
  SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
  SNOWFLAKE_WAREHOUSE: ${{ secrets.SNOWFLAKE_WAREHOUSE }}
  SNOWFLAKE_DATABASE: ${{ secrets.SNOWFLAKE_DATABASE }}
```

Tests that require Snowflake use `require-env` and will be skipped if credentials are not set.

## Troubleshooting

### "ADBC Snowflake driver not found" Error

The native ADBC driver is missing. See [ADBC Driver Setup](https://github.com/iqea-ai/duckdb-snowflake#adbc-driver-setup).

### "Secret not found" Error

Make sure you created the DuckDB secret before running tests:

```sql
CREATE SECRET my_snowflake (
    TYPE snowflake,
    account 'your_account',
    user 'your_user',
    password 'your_password'
);
```

### "Table does not exist" Error

Verify test tables exist in Snowflake:

```sql
SELECT table_name FROM DUCKSYNC_TEST.information_schema.tables 
WHERE table_schema = 'TEST_DATA';
```

### "Insufficient privileges" Error

Check role grants:

```sql
SHOW GRANTS TO ROLE DUCKSYNC_TEST_ROLE;
```

### Connection Timeout

Verify your account identifier format. It should be `orgname-accountname` (hyphen-separated), e.g., `AEDTBHT-ALIAS_ANALYTICS`.

Find this in Snowflake under Admin → Accounts. Look for "Account identifier" (not "Account name" or "Server URL").
