#include "metadata_manager.hpp"
#include "duckdb/main/connection.hpp"
#include <sstream>

namespace duckdb {

DuckSyncMetadataManager::DuckSyncMetadataManager(ClientContext &context) : context_(context), initialized_(false) {
}

std::string DuckSyncMetadataManager::TableName(const std::string &table) const {
	// Returns: ducklake_name.schema_name.table_name
	return ducklake_name_ + "." + schema_name_ + "." + table;
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

void DuckSyncMetadataManager::Initialize(const std::string &ducklake_name, const std::string &schema_name) {
	if (initialized_) {
		return;
	}

	ducklake_name_ = ducklake_name;
	schema_name_ = schema_name;

	// Create metadata schema in the DuckLake catalog
	ExecuteSQL("CREATE SCHEMA IF NOT EXISTS " + ducklake_name_ + "." + schema_name_ + ";");

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

	Connection conn(*context_.db);

	// DuckLake doesn't support ON CONFLICT, so delete then insert
	auto delete_stmt = conn.Prepare("DELETE FROM " + TableName("sources") + " WHERE source_name = $1");
	auto delete_result = delete_stmt->Execute(source.source_name);
	if (delete_result->HasError()) {
		throw InternalException("Failed to delete existing source: %s", delete_result->GetError().c_str());
	}

	auto insert_stmt = conn.Prepare("INSERT INTO " + TableName("sources") +
	                                " (source_name, driver_type, secret_name, passthrough_enabled, created_at) "
	                                "VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP)");
	auto insert_result =
	    insert_stmt->Execute(source.source_name, source.driver_type, source.secret_name, source.passthrough_enabled);
	if (insert_result->HasError()) {
		throw InternalException("Failed to create source: %s", insert_result->GetError().c_str());
	}
}

bool DuckSyncMetadataManager::GetSource(const std::string &source_name, SourceDefinition &out) {
	if (!initialized_) {
		throw InternalException("DuckSyncMetadataManager not initialized");
	}

	Connection conn(*context_.db);
	auto stmt = conn.Prepare("SELECT source_name, driver_type, secret_name, passthrough_enabled, created_at "
	                         "FROM " +
	                         TableName("sources") + " WHERE source_name = $1");
	vector<Value> params = {Value(source_name)};
	auto result = stmt->Execute(params, false);
	if (result->HasError()) {
		throw InternalException("Failed to get source: %s", result->GetError().c_str());
	}

	auto &materialized = result->Cast<MaterializedQueryResult>();
	if (materialized.RowCount() == 0) {
		return false;
	}

	out.source_name = materialized.GetValue(0, 0).ToString();
	out.driver_type = materialized.GetValue(1, 0).ToString();
	out.secret_name = materialized.GetValue(2, 0).ToString();
	out.passthrough_enabled = materialized.GetValue(3, 0).GetValue<bool>();
	out.created_at = materialized.GetValue(4, 0).ToString();

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

	Connection conn(*context_.db);
	auto stmt = conn.Prepare("DELETE FROM " + TableName("sources") + " WHERE source_name = $1");
	auto result = stmt->Execute(source_name);
	if (result->HasError()) {
		throw InternalException("Failed to delete source: %s", result->GetError().c_str());
	}
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

	Value ttl_value = cache.has_ttl ? Value::BIGINT(cache.ttl_seconds) : Value(LogicalType::BIGINT);

	auto result = insert_stmt->Execute(cache.cache_name, cache.source_name, cache.source_query, tables_list, ttl_value);
	if (result->HasError()) {
		throw InternalException("Failed to create cache: %s", result->GetError().c_str());
	}
}

bool DuckSyncMetadataManager::GetCache(const std::string &cache_name, CacheDefinition &out) {
	if (!initialized_) {
		throw InternalException("DuckSyncMetadataManager not initialized");
	}

	Connection conn(*context_.db);
	auto stmt = conn.Prepare("SELECT cache_name, source_name, source_query, monitor_tables, ttl_seconds, created_at "
	                         "FROM " +
	                         TableName("caches") + " WHERE cache_name = $1");
	vector<Value> params = {Value(cache_name)};
	auto result = stmt->Execute(params, false);
	if (result->HasError()) {
		throw InternalException("Failed to get cache: %s", result->GetError().c_str());
	}

	auto &materialized = result->Cast<MaterializedQueryResult>();
	if (materialized.RowCount() == 0) {
		return false;
	}

	out.cache_name = materialized.GetValue(0, 0).ToString();
	out.source_name = materialized.GetValue(1, 0).ToString();
	out.source_query = materialized.GetValue(2, 0).ToString();

	// Parse monitor_tables from list
	out.monitor_tables.clear();
	auto tables_value = materialized.GetValue(3, 0);
	if (tables_value.type().id() == LogicalTypeId::LIST) {
		auto &list_children = ListValue::GetChildren(tables_value);
		for (auto &child : list_children) {
			out.monitor_tables.push_back(child.ToString());
		}
	}

	auto ttl_value = materialized.GetValue(4, 0);
	if (!ttl_value.IsNull()) {
		out.ttl_seconds = ttl_value.GetValue<int64_t>();
		out.has_ttl = true;
	} else {
		out.ttl_seconds = 0;
		out.has_ttl = false;
	}

	out.created_at = materialized.GetValue(5, 0).ToString();

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

	Connection conn(*context_.db);
	auto stmt = conn.Prepare("DELETE FROM " + TableName("caches") + " WHERE cache_name = $1");
	auto result = stmt->Execute(cache_name);
	if (result->HasError()) {
		throw InternalException("Failed to delete cache: %s", result->GetError().c_str());
	}
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

	Connection conn(*context_.db);
	auto stmt = conn.Prepare("INSERT INTO " + TableName("state") + " (cache_name, refresh_count) VALUES ($1, 0)");
	auto result = stmt->Execute(cache_name);
	if (result->HasError()) {
		throw InternalException("Failed to initialize state: %s", result->GetError().c_str());
	}
}

void DuckSyncMetadataManager::UpdateState(const CacheState &state) {
	if (!initialized_) {
		throw InternalException("DuckSyncMetadataManager not initialized");
	}

	Connection conn(*context_.db);

	// Get current refresh_count before deleting
	int64_t refresh_count = 0;
	{
		auto count_stmt =
		    conn.Prepare("SELECT refresh_count FROM " + TableName("state") + " WHERE cache_name = $1");
		vector<Value> count_params = {Value(state.cache_name)};
		auto count_result = count_stmt->Execute(count_params, false);
		if (!count_result->HasError()) {
			auto &count_mat = count_result->Cast<MaterializedQueryResult>();
			if (count_mat.RowCount() > 0 && !count_mat.GetValue(0, 0).IsNull()) {
				refresh_count = count_mat.GetValue(0, 0).GetValue<int64_t>();
			}
		}
	}

	// DuckLake doesn't support ON CONFLICT, so delete then insert
	auto delete_stmt = conn.Prepare("DELETE FROM " + TableName("state") + " WHERE cache_name = $1");
	auto delete_result = delete_stmt->Execute(state.cache_name);
	if (delete_result->HasError()) {
		throw InternalException("Failed to delete state: %s", delete_result->GetError().c_str());
	}

	auto insert_stmt = conn.Prepare(
	    "INSERT INTO " + TableName("state") +
	    " (cache_name, last_refresh, source_state_hash, expires_at, refresh_count) VALUES ($1, $2, $3, $4, $5)");

	Value last_refresh_val = state.HasLastRefresh() ? Value(state.last_refresh) : Value(LogicalType::VARCHAR);
	Value state_hash_val = state.HasStateHash() ? Value(state.source_state_hash) : Value(LogicalType::VARCHAR);
	Value expires_at_val = state.HasExpiresAt() ? Value(state.expires_at) : Value(LogicalType::VARCHAR);
	Value refresh_count_val = Value::BIGINT(refresh_count + 1);

	auto insert_result =
	    insert_stmt->Execute(state.cache_name, last_refresh_val, state_hash_val, expires_at_val, refresh_count_val);
	if (insert_result->HasError()) {
		throw InternalException("Failed to update state: %s", insert_result->GetError().c_str());
	}
}

bool DuckSyncMetadataManager::GetState(const std::string &cache_name, CacheState &out) {
	if (!initialized_) {
		throw InternalException("DuckSyncMetadataManager not initialized");
	}

	Connection conn(*context_.db);
	auto stmt = conn.Prepare("SELECT cache_name, last_refresh, source_state_hash, expires_at "
	                         "FROM " +
	                         TableName("state") + " WHERE cache_name = $1");
	vector<Value> params = {Value(cache_name)};
	auto result = stmt->Execute(params, false);
	if (result->HasError()) {
		throw InternalException("Failed to get state: %s", result->GetError().c_str());
	}

	auto &materialized = result->Cast<MaterializedQueryResult>();
	if (materialized.RowCount() == 0) {
		return false;
	}

	out.cache_name = materialized.GetValue(0, 0).ToString();

	auto last_refresh = materialized.GetValue(1, 0);
	out.last_refresh = last_refresh.IsNull() ? "" : last_refresh.ToString();

	auto state_hash = materialized.GetValue(2, 0);
	out.source_state_hash = state_hash.IsNull() ? "" : state_hash.ToString();

	auto expires_at = materialized.GetValue(3, 0);
	out.expires_at = expires_at.IsNull() ? "" : expires_at.ToString();

	return true;
}

} // namespace duckdb
