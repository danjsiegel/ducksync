# DuckSync

DuckSync is a DuckDB extension that provides intelligent query result caching between DuckDB and Snowflake. It uses DuckLake for storage (PostgreSQL catalog + Parquet files) and features transparent query routing, TTL-based expiration, and smart refresh based on source table metadata.

## Features

- **Transparent Query Routing**: Queries automatically route to cached data when available
- **Smart Refresh**: Only refreshes when source tables have changed (checks `last_altered`)
- **TTL Support**: Configurable cache expiration with time-to-live
- **DuckLake Storage**: Uses DuckLake for efficient Parquet-based storage
- **PostgreSQL Catalog**: Metadata stored in DuckLake's PostgreSQL catalog

## Prerequisites

DuckSync automatically installs the required extensions (DuckLake, Snowflake) on first use.

You'll need:
1. **PostgreSQL database** - for DuckLake catalog storage
2. **Snowflake account** - with a configured DuckDB secret
3. **ADBC Snowflake driver** - native library for Snowflake connectivity

### Snowflake Setup

1. Create a DuckDB secret with your Snowflake credentials
2. Install the ADBC Snowflake driver - see [ADBC Driver Setup](https://github.com/iqea-ai/duckdb-snowflake#adbc-driver-setup)

For more details: [Snowflake Extension Docs](https://duckdb.org/community_extensions/extensions/snowflake)

## Installation

```sql
INSTALL ducksync FROM community;
LOAD ducksync;
```

## Quick Start

### Option A: Using Existing DuckLake (Recommended)

If you already have DuckLake attached:

```sql
-- Your existing DuckLake setup
ATTACH 'ducklake:postgres:host=localhost dbname=mydb...' AS my_lake (DATA_PATH '/data');

-- Initialize DuckSync with your existing catalog
SELECT * FROM ducksync_init('my_lake');

-- Register Snowflake source (using your existing secret)
SELECT * FROM ducksync_add_source('prod', 'snowflake', 'my_snowflake_secret');

-- Create a cache
SELECT * FROM ducksync_create_cache(
    'sales_summary',
    'prod',
    'SELECT region, SUM(amount) as total FROM sales GROUP BY region',
    ['PROD.PUBLIC.SALES'],
    3600
);

-- Refresh the cache
SELECT * FROM ducksync_refresh('sales_summary');

-- Query via ducksync_query (smart routing)
SELECT * FROM ducksync_query('SELECT * FROM PROD.PUBLIC.SALES WHERE region = ''US''', 'prod');
```

### Option B: Full Setup (New Users)

If you don't have DuckLake configured yet:

```sql
-- Setup DuckLake + DuckSync in one step
SELECT * FROM ducksync_setup_storage(
    'host=localhost port=5432 dbname=ducklake user=postgres password=secret',
    '/data/ducksync'
);

-- Then continue with add_source, create_cache, etc.
```

## SQL Functions

### `ducksync_init(catalog_name)`

Initialize DuckSync with an existing DuckLake catalog. **Recommended approach.**

**Parameters:**
- `catalog_name`: Name of your attached DuckLake catalog

**Example:**
```sql
SELECT * FROM ducksync_init('my_ducklake');
```

### `ducksync_setup_storage(pg_connection_string, data_path)`

Full setup - attaches DuckLake and initializes DuckSync. Use if you don't have DuckLake configured yet.

**Parameters:**
- `pg_connection_string`: PostgreSQL connection string (libpq format)
- `data_path`: Local path or S3 path for Parquet data files

### `ducksync_add_source(source_name, driver_type, secret_name, [passthrough_enabled])`

Register a Snowflake data source.

**Parameters:**
- `source_name`: Unique identifier for the source
- `driver_type`: Currently only `'snowflake'`
- `secret_name`: Name of existing DuckDB secret with Snowflake credentials
- `passthrough_enabled` (optional): Allow passthrough for uncached tables (default: false)

### `ducksync_create_cache(cache_name, source_name, source_query, monitor_tables, [ttl_seconds])`

Define a cached query result.

**Parameters:**
- `cache_name`: Unique identifier (used in SQL queries)
- `source_name`: Source to execute query against
- `source_query`: SQL query to cache results from
- `monitor_tables`: List of tables to monitor for changes (e.g., `['DB.SCHEMA.TABLE']`)
- `ttl_seconds` (optional): Cache TTL in seconds (NULL = no expiration)

### `ducksync_refresh(cache_name, [force])`

Refresh a cache with smart check logic.

**Parameters:**
- `cache_name`: Cache to refresh
- `force` (optional): Skip smart check and force refresh (default: false)

**Returns:**
- `result`: SKIPPED, REFRESHED, or ERROR
- `message`: Status message
- `rows_refreshed`: Number of rows (if refreshed)
- `duration_ms`: Refresh duration in milliseconds

### `ducksync_query(sql_query, source_name)`

**The main query interface.** Executes queries with smart routing - returns actual data (not status messages).

**Parameters:**
- `sql_query`: SQL query to execute (use Snowflake table names)
- `source_name`: Source to use for execution

**Routing Logic:**
1. Parses SQL using DuckDB's parser to extract all table references
2. Checks if each table is cached (by cache name or monitored table)
3. **All tables cached** → Rewrites query to use local DuckLake tables
4. **Any table not cached** → Passes entire query to Snowflake

**Examples:**
```sql
-- Cache hit via monitored table name → executes locally
SELECT * FROM ducksync_query(
    'SELECT * FROM PROD.PUBLIC.CUSTOMERS WHERE region = ''US''',
    'prod'
);

-- Cache hit via named query (use cache_name directly)
-- Useful for complex queries cached under a friendly name
SELECT * FROM ducksync_query(
    'SELECT * FROM customers_cache LIMIT 10',
    'prod'
);

-- Table NOT cached → passthrough to Snowflake
SELECT * FROM ducksync_query(
    'SELECT * FROM PROD.PUBLIC.RAW_ORDERS LIMIT 100',
    'prod'
);

-- JOIN with mixed tables (1 cached, 1 not) → passthrough to Snowflake
SELECT * FROM ducksync_query(
    'SELECT c.*, o.total FROM CUSTOMERS c JOIN ORDERS o ON c.id = o.customer_id',
    'prod'
);

-- Complex aggregation from cache
SELECT * FROM ducksync_query(
    'SELECT region, COUNT(*) as cnt FROM PROD.PUBLIC.CUSTOMERS GROUP BY region',
    'prod'
);
```

**Named Queries:** You can query by cache name directly (e.g., `customers_cache`) instead of the Snowflake table name. This is useful for caching complex joins or aggregations under a friendly name that doesn't exist as a table in Snowflake.

**Best Practice:** Create caches with `monitor_tables` matching your Snowflake table names for automatic routing.

### Direct DuckLake Access

Cached data is stored in standard DuckLake tables. Query them directly with normal DuckDB SQL:

```sql
-- Create and refresh a cache
SELECT * FROM ducksync_create_cache('customers', 'prod', 'SELECT * FROM CUSTOMERS', ['CUSTOMERS'], 3600);
SELECT * FROM ducksync_refresh('customers');

-- Query the DuckLake table directly (standard DuckDB - no magic)
SELECT * FROM ducksync.prod.customers;
SELECT * FROM ducksync.prod.customers WHERE region = 'US';

-- Join with local tables
SELECT c.*, l.segment 
FROM ducksync.prod.customers c 
JOIN local_segments l ON c.id = l.customer_id;
```

**Path format:** `{catalog}.{source_name}.{cache_name}` (e.g., `ducksync.prod.customers`)

**When to use each approach:**

| Method | Use When |
|--------|----------|
| `ducksync_query()` | Smart routing with TTL checks and auto-refresh |
| Direct DuckLake | Fast queries, no TTL checks, joining with local data |

## Smart Refresh Logic

DuckSync uses a "smart check" approach to minimize unnecessary data transfers:

1. **TTL Check**: If `expires_at < now()`, trigger refresh
2. **Metadata Check**: Query `information_schema.tables` for `last_altered` timestamps
3. **Hash Comparison**: Compare hash of current metadata vs stored `source_state_hash`
4. **Skip if Match**: If hashes match and TTL not expired, skip refresh
5. **Refresh if Changed**: Execute query, write to DuckLake, update state

This approach means:
- Zero Snowflake compute cost when data hasn't changed
- Sub-second metadata checks vs full query execution
- Automatic refresh when source tables are modified

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     DuckSync Extension                       │
├─────────────────────────────────────────────────────────────┤
│  ducksync_query()    MetadataManager      StorageManager     │
│  (smart routing)     (DuckLake tables)    (DuckLake)         │
│         │                  │                   │             │
│         ▼                  ▼                   ▼             │
│  ┌──────────┐       ┌───────────┐       ┌───────────┐       │
│  │ TTL +    │       │ ducksync. │       │ DuckLake  │       │
│  │ Routing  │◄─────►│ sources   │       │ Parquet   │       │
│  └──────────┘       │ caches    │       │ Files     │       │
│         │           │ state     │       └───────────┘       │
│         │           └───────────┘              ▲             │
│         │                 │                    │             │
│         │          PostgreSQL Catalog ─────────┘             │
│  ┌──────▼───────────────────────────────────────┐           │
│  │            RefreshOrchestrator               │           │
│  │  • TTL check + auto-refresh                  │           │
│  │  • Source metadata query (snowflake_query)   │           │
│  │  • State hash comparison                     │           │
│  │  • Query execution & storage                 │           │
│  └──────────────────────────────────────────────┘           │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
              ┌───────────────────────┐
              │  Snowflake Extension  │
              │  (snowflake_query)    │
              └───────────────────────┘
```

## Testing

### Prerequisites

- Docker (for PostgreSQL)
- DuckDB v1.4.2+
- ADBC Snowflake driver (for Snowflake integration tests)

### Run Tests

```bash
# Build and run integration tests
make test

# Reset test environment (clean slate)
make reset-test

# Or manually:
cd test && docker compose up -d postgres
./test/run_tests.sh

# Cleanup
make test-docker-down    # Stop PostgreSQL
make clean-test-data     # Remove local test files
make clean-all           # Full cleanup (Docker + data + build)
```

For Snowflake integration tests, see [test/README.md](test/README.md).

## Building from Source

```bash
# Clone with submodules
git clone --recurse-submodules https://github.com/danjsiegel/ducksync.git
cd ducksync

# Build
make release

# Run
./build/release/duckdb
```

## Dependencies

- **DuckDB v1.4.2+**
- **DuckLake extension** - auto-installed ([docs](https://ducklake.select/docs/))
- **Snowflake extension** - auto-installed ([docs](https://duckdb.org/community_extensions/extensions/snowflake))

## Known Limitations

- **Snowflake-only**: Currently only supports Snowflake as a data source
- **Manual monitor_tables**: Tables to monitor for changes must be explicitly specified when creating a cache
- **All-or-nothing routing**: If a query references multiple tables and any one is not cached, the entire query passes through to Snowflake
- **SELECT only**: Query rewriting only handles SELECT statements; DDL and DML pass through unchanged

## License

MIT License - see LICENSE file for details.
