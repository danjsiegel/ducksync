# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(ducksync
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# Any extra extensions that should be built
# These are dependencies that DuckSync needs at runtime
# duckdb_extension_load(postgres)
# duckdb_extension_load(httpfs)
