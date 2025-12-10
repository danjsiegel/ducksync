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

For Snowflake setup, see: [Snowflake Extension Docs](https://duckdb.org/community_extensions/extensions/snowflake)

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

-- Refresh and query
SELECT * FROM ducksync_refresh('sales_summary');
SELECT * FROM sales_summary;
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

### `ducksync_passthrough_query(sql_query, source_name)`

Execute a query with smart routing: checks cache first, falls back to Snowflake.

**Parameters:**
- `sql_query`: SQL query to execute (use Snowflake table names)
- `source_name`: Source to use for Snowflake execution

**Behavior:**
- **Simple query (single table)**: Checks if table is cached → uses cache if found
- **Complex query (JOINs, UNIONs, etc.)**: Passes directly to Snowflake

**Examples:**
```sql
-- Simple query - table IS cached → hits local cache
SELECT * FROM ducksync_passthrough_query(
    'SELECT * FROM PROD.PUBLIC.SALES_SUMMARY',
    'prod'
);
-- Returns: "Cache hit: 100 rows from ducksync.prod.sales_summary"

-- Simple query - table NOT cached → Snowflake
SELECT * FROM ducksync_passthrough_query(
    'SELECT * FROM PROD.PUBLIC.RAW_ORDERS WHERE date > ''2024-01-01''',
    'prod'
);
-- Returns: "Passthrough: 5000 rows from Snowflake"

-- Complex query (JOIN) → always Snowflake (doesn't try to parse)
SELECT * FROM ducksync_passthrough_query(
    'SELECT a.*, b.name FROM orders a JOIN customers b ON a.cust_id = b.id',
    'prod'
);
-- Returns: "Passthrough: 200 rows from Snowflake"
```

**Note:** Cache names should match your Snowflake table names for transparent routing.

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
│  QueryRouter         MetadataManager      StorageManager     │
│  (replacement_scan)  (DuckLake tables)    (DuckLake)         │
│         │                  │                   │             │
│         ▼                  ▼                   ▼             │
│  ┌──────────┐       ┌───────────┐       ┌───────────┐       │
│  │ Cache    │       │ ducksync. │       │ DuckLake  │       │
│  │ Routing  │◄─────►│ sources   │       │ Parquet   │       │
│  └──────────┘       │ caches    │       │ Files     │       │
│         │           │ state     │       └───────────┘       │
│         │           └───────────┘              ▲             │
│         │                 │                    │             │
│         │          PostgreSQL Catalog ─────────┘             │
│  ┌──────▼───────────────────────────────────────┐           │
│  │            RefreshOrchestrator               │           │
│  │  • TTL check                                 │           │
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

### Run Tests

```bash
# Build and run integration tests
make test

# Or manually:
cd test && docker compose up -d postgres
./test/run_tests.sh

# Stop PostgreSQL
make test-docker-down
```

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

## Limitations

- **Snowflake-only**: No support for other data sources yet
- **Manual monitor_tables**: Tables to monitor must be explicitly specified
- **No partial routing**: Entire query uses cache or passthrough, not mixed

## License

MIT License - see LICENSE file for details.
