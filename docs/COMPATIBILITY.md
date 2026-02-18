# DuckSync Compatibility Reference

> **Known issue (Snowflake community extension v1.4.4):** `snowflake_query()` crashes with
> `Attempted to access index 1 within vector of size 1` in `ArrowToDuckDB` when used as a
> **bare table function** (`SELECT * FROM snowflake_query(...)`) returning multi-column results.
> **This does NOT affect DuckSync's core functionality:**
> - `ducksync_query()` uses `conn.Query()` internally — multi-column passthrough works ✅
> - `ducksync_refresh()` uses `CREATE TABLE AS SELECT * FROM snowflake_query(...)` — works ✅
> - `GetSourceTableMetadata` uses `conn.Query()` — works ✅
> **Only affected:** direct `SELECT * FROM snowflake_query(...)` in test scripts.
> **Workaround for tests:** push aggregations into the Snowflake SQL, e.g.
> `SELECT * FROM snowflake_query('SELECT COUNT(*) AS cnt FROM table', secret)`.

This document tracks the external API surfaces DuckSync depends on, and what to check when upgrading DuckDB or DuckLake versions.

---

## DuckLake API Surface

DuckSync depends on the following DuckLake behaviors. Check these when upgrading DuckLake.

### ATTACH syntax

```sql
ATTACH 'ducklake:postgres:<connection_string>' AS <name> (DATA_PATH '<path>');
```

**Location:** [`src/storage_manager.cpp:95`](../src/storage_manager.cpp)

If DuckLake changes the ATTACH URI scheme or option syntax, update `DuckSyncStorageManager::AttachDuckLake()`.

### Cleanup procedures

| Procedure | Location | Notes |
|-----------|----------|-------|
| `ducklake_expire_snapshots(name, older_than => INTERVAL '...')` | [`src/cleanup_manager.cpp:62`](../src/cleanup_manager.cpp) | Expires old snapshots |
| `ducklake_cleanup_old_files(name, older_than => INTERVAL '...')` | [`src/cleanup_manager.cpp:70`](../src/cleanup_manager.cpp) | Removes old Parquet files |
| `ducklake_delete_orphaned_files(name)` | [`src/cleanup_manager.cpp:78`](../src/cleanup_manager.cpp) | Removes orphaned files |

If DuckLake renames or changes the signature of these procedures, update `DuckSyncCleanupManager::RunGlobalCleanup()`.

### DDL limitations (as of DuckLake 0.3)

These are **known workarounds** in the codebase. If DuckLake 0.4+ fixes them, the workarounds can be simplified.

#### No `ON CONFLICT` support → DELETE + INSERT pattern

DuckLake does not support `INSERT ... ON CONFLICT DO UPDATE`. DuckSync works around this with a DELETE then INSERT:

```cpp
// DuckLake doesn't support ON CONFLICT, so delete then insert
auto delete_stmt = conn.Prepare("DELETE FROM " + TableName("sources") + " WHERE source_name = $1");
```

**Locations:**
- [`src/metadata_manager.cpp:90`](../src/metadata_manager.cpp) — `CreateSource()`
- [`src/metadata_manager.cpp:186`](../src/metadata_manager.cpp) — `CreateCache()`
- [`src/metadata_manager.cpp:384`](../src/metadata_manager.cpp) — `UpdateState()`

**If DuckLake adds `ON CONFLICT`:** Replace DELETE+INSERT with `INSERT INTO ... ON CONFLICT (key_col) DO UPDATE SET ...`

#### No `DEFAULT` expressions in DDL

DuckLake does not support `DEFAULT` expressions in `CREATE TABLE`. DuckSync passes `CURRENT_TIMESTAMP` explicitly in every INSERT.

**Location:** [`src/metadata_manager.cpp:42`](../src/metadata_manager.cpp) — table DDL in `Initialize()`

**If DuckLake adds `DEFAULT`:** Add `created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP` to table DDL and remove explicit timestamp from INSERT statements.

---

## DuckDB API Surface

These are the DuckDB C++ API surfaces DuckSync uses. Check these when upgrading DuckDB (e.g., v1.4.4 → v1.5.0).

### Extension loading

| API | Location | Notes |
|-----|----------|-------|
| `ExtensionLoader::RegisterFunction(TableFunction)` | [`src/ducksync_extension.cpp:764`](../src/ducksync_extension.cpp) | Used to register all 6 table functions |
| `ExtensionLoader::GetDatabaseInstance()` | [`src/ducksync_extension.cpp:800`](../src/ducksync_extension.cpp) | Used to register replacement scan hook |
| `DucksyncExtension::Load(ExtensionLoader &)` | [`src/ducksync_extension.cpp:804`](../src/ducksync_extension.cpp) | Extension entry point |

### Connection and query execution

| API | Location | Notes |
|-----|----------|-------|
| `Connection(*context_.db)` | [`src/metadata_manager.cpp`](../src/metadata_manager.cpp) | 16+ call sites — creates a connection per operation |
| `conn.Prepare(sql)` | [`src/metadata_manager.cpp`](../src/metadata_manager.cpp) | Prepared statements for all CRUD |
| `stmt->Execute(params...)` | [`src/metadata_manager.cpp`](../src/metadata_manager.cpp) | Executes prepared statements |
| `result->HasError()` / `result->GetError()` | Throughout | Error checking pattern |
| `result->Cast<MaterializedQueryResult>()` | [`src/metadata_manager.cpp`](../src/metadata_manager.cpp) | Result materialization |

### Parser / AST

| API | Location | Notes |
|-----|----------|-------|
| `Parser::ParseQuery(sql)` | [`src/ducksync_extension.cpp:519`](../src/ducksync_extension.cpp) | Parses SQL for table extraction |
| `SelectStatement` / `SetOperationNode` | [`src/ducksync_extension.cpp:460`](../src/ducksync_extension.cpp) | AST node types |
| `BaseTableRef` | [`src/ducksync_extension.cpp:484`](../src/ducksync_extension.cpp) | Table reference rewriting |
| `JoinRef` / `SubqueryRef` | [`src/ducksync_extension.cpp:497`](../src/ducksync_extension.cpp) | Recursive AST traversal |
| `stmt->ToString()` | [`src/ducksync_extension.cpp:554`](../src/ducksync_extension.cpp) | Regenerates SQL from modified AST |

### Client context state

| API | Location | Notes |
|-----|----------|-------|
| `ClientContextState` (base class) | [`src/include/query_router.hpp:11`](../src/include/query_router.hpp) | `DuckSyncState` inherits from this |
| `context.registered_state->GetOrCreate<T>(key)` | [`src/query_router.cpp:8`](../src/query_router.cpp) | Per-connection state management |

### Table function registration

| API | Location | Notes |
|-----|----------|-------|
| `TableFunction(name, arg_types, func, bind)` | [`src/ducksync_extension.cpp:766`](../src/ducksync_extension.cpp) | 6 functions registered (2 overloads each for init/setup_storage) |
| `TableFunction::named_parameters` | [`src/ducksync_extension.cpp:778`](../src/ducksync_extension.cpp) | Used for `passthrough_enabled` and `force` named params |
| `TableFunctionInitGlobal` | [`src/ducksync_extension.cpp:795`](../src/ducksync_extension.cpp) | Used for `ducksync_query` global state |

---

## Upgrading to DuckDB v1.5

### Before the release (proactive)

1. Check if `duckdb-next-build` CI job is passing (see [`.github/workflows/MainDistributionPipeline.yml`](../.github/workflows/MainDistributionPipeline.yml))
2. Run `make test-compat BRANCH=origin/v1.5-variegata` to test the build locally
3. If the build fails, create a `v1.5-variegata` branch in this repo and apply fixes there

### On release day

```bash
make update-version VERSION=v1.5.0
make clean-all && make release
make test
bash docs/manual_test.sh
```

### What to check in the DuckDB 1.5 changelog

- `ExtensionLoader` API changes (function registration, database instance access)
- `ClientContextState` / `registered_state` API changes
- `Parser` / AST node type changes (especially `BaseTableRef`, `SelectStatement`)
- `Connection` / `PreparedStatement` API changes
- `TableFunction` constructor or `named_parameters` changes

---

## Upgrading to DuckLake 0.4 / 1.0

### What to check in the DuckLake changelog

- `ATTACH` URI scheme or option changes
- Cleanup procedure renames or signature changes
- New DDL support (`ON CONFLICT`, `DEFAULT` expressions) — see workarounds above
- Schema/table creation behavior changes

### Testing

```bash
# Install new DuckLake version and run full manual test
bash docs/manual_test.sh
```

---

## GizmoSQL Notes

GizmoSQL uses DuckLake with a shared PostgreSQL backend. DuckSync supports GizmoSQL environments via the optional `schema_name` parameter:

```sql
-- Use a custom metadata schema to avoid collisions in shared DuckLake environments
SELECT * FROM ducksync_init('my_lake', 'my_ducksync_schema');

-- Or with full setup:
SELECT * FROM ducksync_setup_storage('host=...', 's3://...', 'my_ducksync_schema');
```

The metadata schema parameter only affects where DuckSync stores its own `sources`, `caches`, and `state` tables. It does **not** affect where cached data tables are stored (those always live in `catalog.source_name.cache_name`).

### Future: Replacement scan

A true "drop-in" experience where `SELECT * FROM snowflake_table` is intercepted without `ducksync_query()` would require implementing [`QueryRouter::Register`](../src/query_router.cpp) (currently a no-op). This is tracked as future work.
