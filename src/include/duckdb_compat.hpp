#pragma once

// DuckDB API compatibility helpers for BaseTableRef and PreparedStatement::GetNames().
//
// DuckDB v1.5.4 / v1.5-variegata:
//   BaseTableRef has direct string fields: catalog_name / schema_name / table_name.
//   PreparedStatement::GetNames() returns vector<string>.
//
// DuckDB main+:
//   BaseTableRef has no direct string fields; uses private QualifiedName with
//   SetQualifiedName(Identifier, Identifier, Identifier) and GetQualifiedName().
//   PreparedStatement::GetNames() returns vector<Identifier>;
//   Identifier::GetIdentifierName() extracts the raw string.
//
// IMPORTANT: duckdb::Identifier and duckdb::QualifiedName do not exist in v1.5.4 headers.
// To avoid non-dependent name lookup failures on v1.5.4, this header:
//   - never references duckdb::Identifier or duckdb::QualifiedName by name
//   - makes all new-API expressions dependent on the template parameter (RefT or NameT)
//     so they are only instantiated when the new-API branch is actually taken
//   - passes catalog/schema/table as const char* to SetQualifiedName, relying on
//     Identifier's implicit const char* constructor (no explicit type name needed)

#include "duckdb/parser/tableref/basetableref.hpp"

#include <string>
#include <type_traits>

namespace ducksync {

namespace detail {

// Own void_t so we don't depend on std::void_t (C++17 std lib) or duckdb::void_t (internal).
template <typename...>
using void_t = void;

// Detect whether BaseTableRef has the legacy catalog_name string field (old API).
// True  → v1.5.4 / v1.5-variegata: use direct string fields.
// False → DuckDB main+: use SetQualifiedName / GetQualifiedName.
template <typename T, typename = void>
struct has_catalog_name : std::false_type {};

template <typename T>
struct has_catalog_name<T, void_t<decltype(std::declval<T>().catalog_name)>> : std::true_type {};

// Detect whether a name type has GetIdentifierName() — the DuckDB Identifier accessor.
// True  → new DuckDB Identifier type: call GetIdentifierName().
// False → plain std::string: return directly.
template <typename T, typename = void>
struct has_getidentifiername : std::false_type {};

template <typename T>
struct has_getidentifiername<T, void_t<decltype(std::declval<T>().GetIdentifierName())>> : std::true_type {};

} // namespace detail

// Build the fully-qualified table name from a BaseTableRef.
// All member accesses on `base` are dependent on RefT → only looked up at instantiation.
template <typename RefT = duckdb::BaseTableRef>
std::string GetFullTableName(const RefT &base) {
	if constexpr (detail::has_catalog_name<RefT>::value) {
		// Old API (v1.5.4 / v1.5-variegata): direct string fields
		std::string full;
		if (!base.catalog_name.empty()) {
			full += base.catalog_name + ".";
		}
		if (!base.schema_name.empty()) {
			full += base.schema_name + ".";
		}
		full += base.table_name;
		return full;
	} else {
		// New API (main+): QualifiedName accessors.
		// Use const auto& so the type is dependent on RefT (deferred to instantiation).
		const auto &qn = base.GetQualifiedName();
		std::string full;
		if (!qn.Catalog().empty()) {
			full += qn.Catalog().GetIdentifierName() + ".";
		}
		if (!qn.Schema().empty()) {
			full += qn.Schema().GetIdentifierName() + ".";
		}
		full += qn.Name().GetIdentifierName();
		return full;
	}
}

// Set catalog / schema / table on a BaseTableRef.
// New-API branch uses SetQualifiedName with const char* so that duckdb::Identifier's
// implicit const char* constructor is invoked without naming the Identifier type
// (which doesn't exist in v1.5.4 headers and would fail non-dependent name lookup).
template <typename RefT = duckdb::BaseTableRef>
void SetTableRefFields(RefT &base, const std::string &catalog, const std::string &schema, const std::string &table) {
	if constexpr (detail::has_catalog_name<RefT>::value) {
		// Old API (v1.5.4 / v1.5-variegata): direct string fields
		base.catalog_name = catalog;
		base.schema_name = schema;
		base.table_name = table;
	} else {
		// New API (main+): SetQualifiedName(Identifier, Identifier, Identifier).
		// Pass c_str() so Identifier's implicit const char* ctor is used —
		// avoids naming duckdb::Identifier (unavailable in old DuckDB headers).
		base.SetQualifiedName(catalog.c_str(), schema.c_str(), table.c_str());
	}
}

// Convert a GetNames() element to std::string.
// Old API: element is already std::string → return directly.
// New API: element is duckdb::Identifier → call GetIdentifierName().
// The `else` branch return converts the Identifier to string via its explicit string
// operator — this branch is discarded when has_getidentifiername is false (string case),
// so the ill-formed explicit conversion is never instantiated.
template <typename NameT>
std::string ToStringName(const NameT &name) {
	if constexpr (detail::has_getidentifiername<NameT>::value) {
		return name.GetIdentifierName();
	} else {
		return name; // NOLINT: plain std::string
	}
}

} // namespace ducksync
