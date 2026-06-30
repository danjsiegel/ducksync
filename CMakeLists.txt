cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME ducksync)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/ducksync_extension.cpp
    src/metadata_manager.cpp
    src/storage_manager.cpp
    src/refresh_orchestrator.cpp
    src/query_router.cpp
    src/cleanup_manager.cpp
)

# Detect DuckDB API shape for BaseTableRef.
# v1.5.4: direct string fields catalog_name / schema_name / table_name.
# main+:  table_name is a QualifiedName struct with .catalog / .schema / .name.
# We test whether catalog_name exists as a direct string member (old API).
include(CheckCXXSourceCompiles)
set(CMAKE_REQUIRED_INCLUDES
    "${CMAKE_CURRENT_SOURCE_DIR}/duckdb/src/include"
    "${CMAKE_SOURCE_DIR}/duckdb/src/include")
set(CMAKE_REQUIRED_FLAGS "-std=c++17")
check_cxx_source_compiles("
#include \"duckdb/parser/tableref/basetableref.hpp\"
#include <string>
int main() { duckdb::BaseTableRef r; std::string s = r.catalog_name; (void)s; return 0; }
" DUCKDB_BASETABLEREF_HAS_CATALOG_NAME)
if(DUCKDB_BASETABLEREF_HAS_CATALOG_NAME)
    add_compile_definitions(DUCKDB_BASETABLEREF_HAS_CATALOG_NAME)
endif()

# Detect whether PreparedStatement::GetNames() returns vector<string> (old) or vector<Identifier> (new).
# We test whether GetNames()[0] is directly assignable to std::string.
check_cxx_source_compiles("
#include \"duckdb.hpp\"
#include <string>
void f(duckdb::PreparedStatement *p) { std::string s = p->GetNames()[0]; (void)s; }
int main() { return 0; }
" DUCKDB_GETNAMES_RETURNS_STRING)
if(DUCKDB_GETNAMES_RETURNS_STRING)
    add_compile_definitions(DUCKDB_GETNAMES_RETURNS_STRING)
endif()

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
