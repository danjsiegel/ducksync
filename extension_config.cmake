# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(ducksync
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

if(MSVC)
    set(CMAKE_CXX_STANDARD 17 CACHE STRING "C++ standard to enforce" FORCE)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    add_compile_options($<$<COMPILE_LANGUAGE:CXX>:/std:c++17>)
endif()

# Any extra extensions that should be built
# These are dependencies that DuckSync needs at runtime
# duckdb_extension_load(postgres)
# duckdb_extension_load(httpfs)
