#include "query_router.hpp"
#include "refresh_orchestrator.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/common/exception.hpp"
#include <sstream>

namespace duckdb {

// Helper to create a connection from context
static Connection MakeConnection(ClientContext &context) {
	return Connection(*context.db);
}

// Static key for DuckSync client context state
static const char *DUCKSYNC_STATE_KEY = "ducksync_state";

DuckSyncState &GetDuckSyncState(ClientContext &context) {
	auto state = context.registered_state->GetOrCreate<DuckSyncState>(DUCKSYNC_STATE_KEY);
	return *state;
}

void QueryRouter::Register(DatabaseInstance &db) {
	// Register replacement scan
	auto &config = DBConfig::GetConfig(db);
	config.replacement_scans.emplace_back(ReplacementScan, nullptr);
}

unique_ptr<TableRef> QueryRouter::ReplacementScan(ClientContext &context, ReplacementScanInput &input,
                                                  optional_ptr<ReplacementScanData> data) {
	auto &state = GetDuckSyncState(context);

	// If DuckSync is not initialized, skip (pass through to DuckDB)
	if (!state.initialized || !state.metadata_manager) {
		return nullptr;
	}

	std::string table_name = input.table_name;

	// Layer 1: Check if this table name matches a registered cache
	CacheDefinition cache;
	if (!FindCache(context, table_name, cache)) {
		// Not a DuckSync cache - pass through to DuckDB
		return nullptr;
	}

	// Layer 2: Check TTL and auto-refresh if expired
	if (!IsCacheValid(context, cache)) {
		// Cache expired or never refreshed - trigger synchronous refresh
		AutoRefreshCache(context, cache);
	}

	// Layer 3: Return reference to DuckLake cached table
	return GetCacheTableRef(context, cache);
}

bool QueryRouter::FindCache(ClientContext &context, const std::string &table_name, CacheDefinition &out) {
	auto &state = GetDuckSyncState(context);

	if (!state.metadata_manager) {
		return false;
	}

	try {
		return state.metadata_manager->GetCache(table_name, out);
	} catch (...) {
		return false;
	}
}

bool QueryRouter::IsCacheValid(ClientContext &context, const CacheDefinition &cache) {
	auto &state = GetDuckSyncState(context);

	if (!state.metadata_manager) {
		return false;
	}

	CacheState cache_state;
	if (!state.metadata_manager->GetState(cache.cache_name, cache_state)) {
		return false; // Never refreshed
	}

	// If never refreshed, not valid
	if (!cache_state.HasLastRefresh()) {
		return false;
	}

	// If no TTL, always valid (once refreshed)
	if (!cache.has_ttl) {
		return true;
	}

	// Check if expired
	if (!cache_state.HasExpiresAt()) {
		return false;
	}

	// Compare expires_at to current time using SQL
	auto conn = MakeConnection(context);

	std::ostringstream sql;
	sql << "SELECT CASE WHEN TIMESTAMP '" << cache_state.expires_at
	    << "' >= CURRENT_TIMESTAMP THEN TRUE ELSE FALSE END;";

	auto result = conn.Query(sql.str());
	if (result->HasError() || result->RowCount() == 0) {
		return false;
	}

	return result->GetValue(0, 0).GetValue<bool>();
}

void QueryRouter::AutoRefreshCache(ClientContext &context, const CacheDefinition &cache) {
	auto &state = GetDuckSyncState(context);

	if (!state.metadata_manager || !state.storage_manager) {
		throw InternalException("DuckSync not properly initialized for auto-refresh");
	}

	// Create orchestrator and perform synchronous refresh
	RefreshOrchestrator orchestrator(context, *state.metadata_manager, *state.storage_manager);
	auto status = orchestrator.Refresh(cache.cache_name, false); // force=false, use smart check

	if (status.result == RefreshResult::ERROR) {
		throw IOException("Auto-refresh failed for cache '" + cache.cache_name + "': " + status.message);
	}
	// SKIPPED or REFRESHED are both fine - proceed with query
}

unique_ptr<TableRef> QueryRouter::GetCacheTableRef(ClientContext &context, const CacheDefinition &cache) {
	auto &state = GetDuckSyncState(context);

	if (!state.storage_manager) {
		throw InternalException("StorageManager not initialized");
	}

	// Get the DuckLake table name: ducksync_data.{source_name}.{cache_name}
	std::string full_table_name = state.storage_manager->GetDuckLakeTableName(cache.cache_name, cache.source_name);

	// Create a BaseTableRef pointing to the DuckLake table
	auto table_ref = make_uniq<BaseTableRef>();

	// Split the qualified table name into parts
	std::vector<std::string> parts;
	std::istringstream iss(full_table_name);
	std::string part;
	while (std::getline(iss, part, '.')) {
		parts.push_back(part);
	}

	if (parts.size() >= 3) {
		table_ref->catalog_name = parts[0];
		table_ref->schema_name = parts[1];
		table_ref->table_name = parts[2];
	} else if (parts.size() == 2) {
		table_ref->schema_name = parts[0];
		table_ref->table_name = parts[1];
	} else {
		table_ref->table_name = full_table_name;
	}

	return std::move(table_ref);
}

unique_ptr<TableRef> QueryRouter::HandlePassthrough(ClientContext &context, const std::string &table_name,
                                                    const SourceDefinition &source) {
	// For passthrough, create a snowflake_query call
	// This creates: SELECT * FROM snowflake_query('{secret_name}', 'SELECT * FROM {table_name}')

	auto table_function = make_uniq<TableFunctionRef>();

	vector<unique_ptr<ParsedExpression>> args;
	args.push_back(make_uniq<ConstantExpression>(Value(source.secret_name)));
	args.push_back(make_uniq<ConstantExpression>(Value("SELECT * FROM " + table_name)));

	table_function->function = make_uniq<FunctionExpression>("snowflake_query", std::move(args));

	return std::move(table_function);
}

} // namespace duckdb
