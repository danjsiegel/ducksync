# DuckSync

DuckSync is a DuckDB extension that provides intelligent query result caching between DuckDB and Snowflake. It uses DuckLake for storage (PostgreSQL catalog + Parquet files) and features transparent query routing, TTL-based expiration, and smart refresh based on source table metadata.

## Features

- **Transparent Query Routing**: Queries automatically route to cached data when available
- **Smart Refresh**: Only refreshes when source tables have changed (checks `last_altered`)
- **TTL Support**: Configurable cache expiration with time-to-live
- **DuckLake Storage**: Uses DuckLake for efficient Parquet-based storage
- **PostgreSQL Catalog**: Metadata stored in DuckLake's PostgreSQL catalog

## Installation

```sql
-- From community extensions (once published)
INSTALL ducksync FROM community;
LOAD ducksync;
```

## Quick Start

```sql
-- 1. Setup Storage (DuckLake with PostgreSQL catalog + local data path)
SELECT * FROM ducksync_setup_storage(
    'host=localhost port=5432 dbname=ducklake user=ducksync password=ducksync',
    '/data/ducksync'
);

-- 2. Create Snowflake Secret
CREATE SECRET snowflake_prod (
    TYPE snowflake,
    ACCOUNT 'your-account',
    USER 'your-user',
    PASSWORD 'your-password',
    DATABASE 'your-database',
    WAREHOUSE 'your-warehouse'
);

-- 3. Register Source
SELECT * FROM ducksync_add_source('prod', 'snowflake', 'snowflake_prod');

-- 4. Create Cache with 1-hour TTL
SELECT * FROM ducksync_create_cache(
    'sales_summary',
    'prod',
    'SELECT region, SUM(amount) as total FROM sales GROUP BY region',
    ['PROD.PUBLIC.SALES'],
    3600
);

-- 5. Refresh Cache
SELECT * FROM ducksync_refresh('sales_summary');

-- 6. Query (transparently routes to cache)
SELECT * FROM sales_summary;
```

## SQL Functions

### `ducksync_setup_storage(pg_connection_string, data_path)`

Configure DuckLake storage for cached data and metadata.

**Parameters:**
- `pg_connection_string`: PostgreSQL connection string for DuckLake catalog (libpq format)
- `data_path`: Local path or S3 path for Parquet data files

**Example:**
```sql
-- Local storage
SELECT * FROM ducksync_setup_storage(
    'host=localhost dbname=ducklake user=postgres',
    '/tmp/ducksync_data'
);

-- S3 storage (requires httpfs extension and S3 credentials)
SELECT * FROM ducksync_setup_storage(
    'host=mydb.xyz.rds.amazonaws.com dbname=ducklake user=admin password=secret',
    's3://my-bucket/ducksync/'
);
```

### `ducksync_add_source(source_name, driver_type, secret_name, [passthrough_enabled])`

Register a Snowflake data source.

**Parameters:**
- `source_name`: Unique identifier for the source
- `driver_type`: Currently only `'snowflake'`
- `secret_name`: Name of DuckDB secret with Snowflake credentials
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
git clone --recurse-submodules https://github.com/your-org/ducksync.git
cd ducksync

# Build
make release

# Run
./build/release/duckdb
```

## Dependencies

- **DuckDB v1.4.2+**
- **DuckLake extension** - for storage
- **Snowflake extension** - for querying Snowflake
- **postgres_scanner extension** - used by DuckLake for PostgreSQL catalog

## Limitations

- **Snowflake-only**: No support for other data sources yet
- **Manual monitor_tables**: Tables to monitor must be explicitly specified
- **No partial routing**: Entire query uses cache or passthrough, not mixed

## License

MIT License - see LICENSE file for details.
