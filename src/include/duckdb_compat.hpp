#pragma once

// DuckDB API compatibility helpers for BaseTableRef and PreparedStatement::GetNames().
//
// DuckDB v1.5.4: BaseTableRef has separate string fields catalog_name / schema_name / table_name.
//                PreparedStatement::GetNames() returns vector<string>.
//
// DuckDB main+:  BaseTableRef has only table_name as a plain string (no catalog_name / schema_name).
//                PreparedStatement::GetNames() returns vector<string> (same) but kept for safety.
//
// All helpers are function templates so that if constexpr properly discards inapplicable
// branches — the discarded branch is never type-checked for the given template parameters.

#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

#include <string>
#include <type_traits>

namespace ducksync {

namespace detail {

// Own void_t so we don't depend on std::void_t (C++17) or duckdb::void_t (internal).
template <typename...>
using void_t = void;

// Detect whether BaseTableRef has the legacy catalog_name field.
template <typename T, typename = void>
struct has_catalog_name : std::false_type {};

template <typename T>
struct has_catalog_name<T, void_t<decltype(std::declval<T>().catalog_name)>> : std::true_type {};

// Detect whether a name type has a GetName() method (Identifier vs string).
template <typename T, typename = void>
struct has_getname : std::false_type {};

template <typename T>
struct has_getname<T, void_t<decltype(std::declval<T>().GetName())>> : std::true_type {};

} // namespace detail

// Build the fully-qualified table name from a BaseTableRef.
// Old API: joins catalog_name + schema_name + table_name.
// New API: table_name already contains the (possibly plain) name.
template <typename RefT = duckdb::BaseTableRef>
std::string GetFullTableName(const RefT &base) {
	if constexpr (detail::has_catalog_name<RefT>::value) {
		// Old API: separate string fields
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
		// New API: table_name is the full (or plain) name from the parser
		return base.table_name;
	}
}

// Set catalog / schema / table on a BaseTableRef.
// Old API: writes separate string fields.
// New API: encodes as a fully-qualified dotted string in table_name.
template <typename RefT = duckdb::BaseTableRef>
void SetTableRefFields(RefT &base, const std::string &catalog, const std::string &schema,
                       const std::string &table) {
	if constexpr (detail::has_catalog_name<RefT>::value) {
		// Old API
		base.catalog_name = catalog;
		base.schema_name = schema;
		base.table_name = table;
	} else {
		// New API: encode as fully-qualified dotted string
		base.table_name = catalog + "." + schema + "." + table;
	}
}

// Convert a GetNames() element to std::string.
// Old API: element is already std::string, return directly.
// New API: element is duckdb::Identifier, call GetName().
template <typename NameT>
std::string ToStringName(const NameT &name) {
	if constexpr (detail::has_getname<NameT>::value) {
		return name.GetName();
	} else {
		return name;
	}
}

} // namespace ducksync
