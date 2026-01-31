#include "metadata_manager.hpp"
#include "duckdb/main/connection.hpp"
#include <sstream>

namespace duckdb {

DuckSyncMetadataManager::DuckSyncMetadataManager(ClientContext &context) : context_(context), initialized_(false) {
}

std::string DuckSyncMetadataManager::TableName(const std::string &table) const {
	// Returns: ducklake_name.ducksync.table_name
	return ducklake_name_ + ".ducksync." + table;
}

void DuckSyncMetadataManager::ExecuteSQL(const std::string &sql) {
	Connection conn(*context_.db);
	auto result = conn.Query(sql);
	if (result->HasError()) {
		throw InternalException("DuckSync SQL error: %s\nQuery: %s", result->GetError().c_str(), sql.c_str());
	}
}

unique_ptr<MaterializedQueryResult> DuckSyncMetadataManager::QuerySQL(const std::string &sql) {
	Connection conn(*context_.db);
	auto result = conn.Query(sql);
	if (result->HasError()) {
		throw InternalException("DuckSync SQL error: %s\nQuery: %s", result->GetError().c_str(), sql.c_str());
	}
	return unique_ptr<MaterializedQueryResult>(static_cast<MaterializedQueryResult *>(result.release()));
}

void DuckSyncMetadataManager::Initialize(const std::string &ducklake_name) {
	if (initialized_) {
		return;
	}

	ducklake_name_ = ducklake_name;

	// Create ducksync schema in the DuckLake catalog
	ExecuteSQL("CREATE SCHEMA IF NOT EXISTS " + ducklake_name_ + ".ducksync;");

	// Create sources table (DuckLake: no PRIMARY KEY, no DEFAULT expressions)
	std::ostringstream sources_sql;
	sources_sql << "CREATE TABLE IF NOT EXISTS " << TableName("sources") << " ("
	            << "source_name VARCHAR, "
	            << "driver_type VARCHAR, "
	            << "secret_name VARCHAR, "
	            << "passthrough_enabled BOOLEAN, "
	            << "created_at TIMESTAMP"
	            << ");";
	ExecuteSQL(sources_sql.str());

	// Create caches table
	std::ostringstream caches_sql;
	caches_sql << "CREATE TABLE IF NOT EXISTS " << TableName("caches") << " ("
	           << "cache_name VARCHAR, "
	           << "source_name VARCHAR, "
	           << "source_query VARCHAR, "
	           << "monitor_tables VARCHAR[], "
	           << "ttl_seconds BIGINT, "
	           << "created_at TIMESTAMP"
	           << ");";
	ExecuteSQL(caches_sql.str());

	// Create state table
	std::ostringstream state_sql;
	state_sql << "CREATE TABLE IF NOT EXISTS " << TableName("state") << " ("
	          << "cache_name VARCHAR, "
	          << "last_refresh TIMESTAMP, "
	          << "source_state_hash VARCHAR, "
	          << "expires_at TIMESTAMP, "
	          << "refresh_count BIGINT"
	          << ");";
	ExecuteSQL(state_sql.str());

	initialized_ = true;
}

//===--------------------------------------------------------------------===//
// Source Operations
//===--------------------------------------------------------------------===//

void DuckSyncMetadataManager::CreateSource(const SourceDefinition &source) {
	if (!initialized_) {
		throw InternalException("DuckSyncMetadataManager not initialized");
	}

	// DuckLake doesn't support ON CONFLICT, so delete then insert
	std::ostringstream delete_sql;
	delete_sql << "DELETE FROM " << TableName("sources") << " WHERE source_name = '" << source.source_name << "';";
	ExecuteSQL(delete_sql.str());

	std::ostringstream sql;
	sql << "INSERT INTO " << TableName("sources")
	    << " (source_name, driver_type, secret_name, passthrough_enabled, created_at) VALUES ("
	    << "'" << source.source_name << "', "
	    << "'" << source.driver_type << "', "
	    << "'" << source.secret_name << "', " << (source.passthrough_enabled ? "TRUE" : "FALSE") << ", "
	    << "CURRENT_TIMESTAMP);";

	ExecuteSQL(sql.str());
}

bool DuckSyncMetadataManager::GetSource(const std::string &source_name, SourceDefinition &out) {
	if (!initialized_) {
		throw InternalException("DuckSyncMetadataManager not initialized");
	}

	std::ostringstream sql;
	sql << "SELECT source_name, driver_type, secret_name, passthrough_enabled, created_at "
	    << "FROM " << TableName("sources") << " WHERE source_name = '" << source_name << "';";

	auto result = QuerySQL(sql.str());

	if (result->RowCount() == 0) {
		return false;
	}

	out.source_name = result->GetValue(0, 0).ToString();
	out.driver_type = result->GetValue(1, 0).ToString();
	out.secret_name = result->GetValue(2, 0).ToString();
	out.passthrough_enabled = result->GetValue(3, 0).GetValue<bool>();
	out.created_at = result->GetValue(4, 0).ToString();

	return true;
}

std::vector<SourceDefinition> DuckSyncMetadataManager::ListSources() {
	if (!initialized_) {
		throw InternalException("DuckSyncMetadataManager not initialized");
	}

	std::vector<SourceDefinition> sources;

	std::ostringstream sql;
	sql << "SELECT source_name, driver_type, secret_name, passthrough_enabled, created_at "
	    << "FROM " << TableName("sources") << " ORDER BY source_name;";

	auto result = QuerySQL(sql.str());

	for (idx_t row = 0; row < result->RowCount(); row++) {
		SourceDefinition source;
		source.source_name = result->GetValue(0, row).ToString();
		source.driver_type = result->GetValue(1, row).ToString();
		source.secret_name = result->GetValue(2, row).ToString();
		source.passthrough_enabled = result->GetValue(3, row).GetValue<bool>();
		source.created_at = result->GetValue(4, row).ToString();
		sources.push_back(source);
	}

	return sources;
}

void DuckSyncMetadataManager::DeleteSource(const std::string &source_name) {
	if (!initialized_) {
		throw InternalException("DuckSyncMetadataManager not initialized");
	}

	std::ostringstream sql;
	sql << "DELETE FROM " << TableName("sources") << " WHERE source_name = '" << source_name << "';";
	ExecuteSQL(sql.str());
}

//===--------------------------------------------------------------------===//
// Cache Operations
//===--------------------------------------------------------------------===//

void DuckSyncMetadataManager::CreateCache(const CacheDefinition &cache) {
	if (!initialized_) {
		throw InternalException("DuckSyncMetadataManager not initialized");
	}

	Connection conn(*context_.db);

	// DuckLake doesn't support ON CONFLICT, so delete then insert
	auto delete_stmt = conn.Prepare("DELETE FROM " + TableName("caches") + " WHERE cache_name = $1");
	delete_stmt->Execute(cache.cache_name);

	// Build monitor_tables as DuckDB LIST value
	vector<Value> table_values;
	for (const auto &table : cache.monitor_tables) {
		table_values.push_back(Value(table));
	}
	Value tables_list = Value::LIST(LogicalType::VARCHAR, table_values);

	// Use prepared statement for safe parameter binding
	auto insert_stmt = conn.Prepare("INSERT INTO " + TableName("caches") +
	                                " (cache_name, source_name, source_query, monitor_tables, ttl_seconds, created_at) "
	                                "VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)");

	Value ttl_value = cache.has_ttl ? Value::INTEGER(cache.ttl_seconds) : Value(LogicalType::INTEGER);

	auto result = insert_stmt->Execute(cache.cache_name, cache.source_name, cache.source_query, tables_list, ttl_value);
	if (result->HasError()) {
		throw InternalException("Failed to create cache: %s", result->GetError().c_str());
	}
}

bool DuckSyncMetadataManager::GetCache(const std::string &cache_name, CacheDefinition &out) {
	if (!initialized_) {
		throw InternalException("DuckSyncMetadataManager not initialized");
	}

	std::ostringstream sql;
	sql << "SELECT cache_name, source_name, source_query, monitor_tables, ttl_seconds, created_at "
	    << "FROM " << TableName("caches") << " WHERE cache_name = '" << cache_name << "';";

	auto result = QuerySQL(sql.str());

	if (result->RowCount() == 0) {
		return false;
	}

	out.cache_name = result->GetValue(0, 0).ToString();
	out.source_name = result->GetValue(1, 0).ToString();
	out.source_query = result->GetValue(2, 0).ToString();

	// Parse monitor_tables from list
	out.monitor_tables.clear();
	auto tables_value = result->GetValue(3, 0);
	if (tables_value.type().id() == LogicalTypeId::LIST) {
		auto &list_children = ListValue::GetChildren(tables_value);
		for (auto &child : list_children) {
			out.monitor_tables.push_back(child.ToString());
		}
	}

	auto ttl_value = result->GetValue(4, 0);
	if (!ttl_value.IsNull()) {
		out.ttl_seconds = ttl_value.GetValue<int64_t>();
		out.has_ttl = true;
	} else {
		out.ttl_seconds = 0;
		out.has_ttl = false;
	}

	out.created_at = result->GetValue(5, 0).ToString();

	return true;
}

bool DuckSyncMetadataManager::GetCacheByMonitorTable(const std::string &table_name, CacheDefinition &out) {
	if (!initialized_) {
		throw InternalException("DuckSyncMetadataManager not initialized");
	}

	// Normalize table name for comparison (uppercase)
	std::string upper_table = table_name;
	std::transform(upper_table.begin(), upper_table.end(), upper_table.begin(), ::toupper);

	// Search all caches for one that monitors this table
	auto caches = ListCaches();
	for (auto &cache : caches) {
		for (auto &monitor_table : cache.monitor_tables) {
			std::string upper_monitor = monitor_table;
			std::transform(upper_monitor.begin(), upper_monitor.end(), upper_monitor.begin(), ::toupper);
			if (upper_monitor == upper_table) {
				out = cache;
				return true;
			}
		}
	}

	return false;
}

std::vector<CacheDefinition> DuckSyncMetadataManager::ListCaches() {
	if (!initialized_) {
		throw InternalException("DuckSyncMetadataManager not initialized");
	}

	std::vector<CacheDefinition> caches;

	std::ostringstream sql;
	sql << "SELECT cache_name, source_name, source_query, monitor_tables, ttl_seconds, created_at "
	    << "FROM " << TableName("caches") << " ORDER BY cache_name;";

	auto result = QuerySQL(sql.str());

	for (idx_t row = 0; row < result->RowCount(); row++) {
		CacheDefinition cache;
		cache.cache_name = result->GetValue(0, row).ToString();
		cache.source_name = result->GetValue(1, row).ToString();
		cache.source_query = result->GetValue(2, row).ToString();

		auto tables_value = result->GetValue(3, row);
		if (tables_value.type().id() == LogicalTypeId::LIST) {
			auto &list_children = ListValue::GetChildren(tables_value);
			for (auto &child : list_children) {
				cache.monitor_tables.push_back(child.ToString());
			}
		}

		auto ttl_value = result->GetValue(4, row);
		if (!ttl_value.IsNull()) {
			cache.ttl_seconds = ttl_value.GetValue<int64_t>();
			cache.has_ttl = true;
		} else {
			cache.ttl_seconds = 0;
			cache.has_ttl = false;
		}

		cache.created_at = result->GetValue(5, row).ToString();
		caches.push_back(cache);
	}

	return caches;
}

void DuckSyncMetadataManager::DeleteCache(const std::string &cache_name) {
	if (!initialized_) {
		throw InternalException("DuckSyncMetadataManager not initialized");
	}

	std::ostringstream sql;
	sql << "DELETE FROM " << TableName("caches") << " WHERE cache_name = '" << cache_name << "';";
	ExecuteSQL(sql.str());
}

//===--------------------------------------------------------------------===//
// State Operations
//===--------------------------------------------------------------------===//

void DuckSyncMetadataManager::InitializeState(const std::string &cache_name) {
	if (!initialized_) {
		throw InternalException("DuckSyncMetadataManager not initialized");
	}

	// Check if state already exists
	CacheState existing;
	if (GetState(cache_name, existing)) {
		return; // Already initialized
	}

	std::ostringstream sql;
	sql << "INSERT INTO " << TableName("state") << " (cache_name, refresh_count) VALUES ('" << cache_name << "', 0);";
	ExecuteSQL(sql.str());
}

void DuckSyncMetadataManager::UpdateState(const CacheState &state) {
	if (!initialized_) {
		throw InternalException("DuckSyncMetadataManager not initialized");
	}

	// Get current refresh_count before deleting
	int64_t refresh_count = 0;
	CacheState existing;
	if (GetState(state.cache_name, existing)) {
		// Query refresh_count separately since GetState doesn't return it
		std::ostringstream count_sql;
		count_sql << "SELECT refresh_count FROM " << TableName("state") << " WHERE cache_name = '" << state.cache_name
		          << "';";
		auto count_result = QuerySQL(count_sql.str());
		if (count_result->RowCount() > 0 && !count_result->GetValue(0, 0).IsNull()) {
			refresh_count = count_result->GetValue(0, 0).GetValue<int64_t>();
		}
	}

	// DuckLake doesn't support ON CONFLICT, so delete then insert
	std::ostringstream delete_sql;
	delete_sql << "DELETE FROM " << TableName("state") << " WHERE cache_name = '" << state.cache_name << "';";
	ExecuteSQL(delete_sql.str());

	std::ostringstream sql;
	sql << "INSERT INTO " << TableName("state")
	    << " (cache_name, last_refresh, source_state_hash, expires_at, refresh_count) VALUES ("
	    << "'" << state.cache_name << "', ";

	if (state.HasLastRefresh()) {
		sql << "'" << state.last_refresh << "'";
	} else {
		sql << "NULL";
	}

	sql << ", ";

	if (state.HasStateHash()) {
		sql << "'" << state.source_state_hash << "'";
	} else {
		sql << "NULL";
	}

	sql << ", ";

	if (state.HasExpiresAt()) {
		sql << "'" << state.expires_at << "'";
	} else {
		sql << "NULL";
	}

	sql << ", " << (refresh_count + 1) << ");";

	ExecuteSQL(sql.str());
}

bool DuckSyncMetadataManager::GetState(const std::string &cache_name, CacheState &out) {
	if (!initialized_) {
		throw InternalException("DuckSyncMetadataManager not initialized");
	}

	std::ostringstream sql;
	sql << "SELECT cache_name, last_refresh, source_state_hash, expires_at "
	    << "FROM " << TableName("state") << " WHERE cache_name = '" << cache_name << "';";

	auto result = QuerySQL(sql.str());

	if (result->RowCount() == 0) {
		return false;
	}

	out.cache_name = result->GetValue(0, 0).ToString();

	auto last_refresh = result->GetValue(1, 0);
	out.last_refresh = last_refresh.IsNull() ? "" : last_refresh.ToString();

	auto state_hash = result->GetValue(2, 0);
	out.source_state_hash = state_hash.IsNull() ? "" : state_hash.ToString();

	auto expires_at = result->GetValue(3, 0);
	out.expires_at = expires_at.IsNull() ? "" : expires_at.ToString();

	return true;
}

} // namespace duckdb
