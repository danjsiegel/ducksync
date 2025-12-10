#include "storage_manager.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/exception.hpp"
#include <sstream>

namespace duckdb {

DuckSyncStorageManager::DuckSyncStorageManager(ClientContext &context)
    : context_(context), ducklake_attached_(false), ducklake_name_("ducksync") {
}

DuckSyncStorageManager::~DuckSyncStorageManager() {
}

Connection DuckSyncStorageManager::GetConnection() {
	return Connection(*context_.db);
}

void DuckSyncStorageManager::SetupStorage(const std::string &pg_connection_string, const std::string &data_path) {
	config_.pg_connection_string = pg_connection_string;
	config_.data_path = data_path;

	AttachDuckLake();
}

void DuckSyncStorageManager::UseExistingCatalog(const std::string &catalog_name) {
	if (ducklake_attached_) {
		return;
	}

	// Install required extensions (DuckLake + Snowflake)
	InstallRequiredExtensions();

	auto conn = GetConnection();

	// Verify the catalog exists by querying information_schema
	std::ostringstream check_sql;
	check_sql << "SELECT COUNT(*) FROM information_schema.schemata WHERE catalog_name = '" << catalog_name << "';";

	auto result = conn.Query(check_sql.str());
	if (result->HasError()) {
		throw IOException("Failed to verify catalog '" + catalog_name + "': " + result->GetError());
	}

	if (result->RowCount() == 0 || result->GetValue(0, 0).GetValue<int64_t>() == 0) {
		throw IOException("Catalog '" + catalog_name + "' not found. Make sure DuckLake is attached first.");
	}

	ducklake_name_ = catalog_name;
	ducklake_attached_ = true;
}

void DuckSyncStorageManager::InstallRequiredExtensions() {
	auto conn = GetConnection();

	// Install and load Snowflake extension
	auto sf_install = conn.Query("INSTALL snowflake FROM community;");
	if (sf_install->HasError()) {
		throw IOException("Failed to install Snowflake extension: " + sf_install->GetError());
	}

	auto sf_load = conn.Query("LOAD snowflake;");
	if (sf_load->HasError()) {
		throw IOException("Failed to load Snowflake extension: " + sf_load->GetError());
	}

	// Install and load DuckLake extension
	auto dl_install = conn.Query("INSTALL ducklake;");
	if (dl_install->HasError()) {
		throw IOException("Failed to install DuckLake extension: " + dl_install->GetError());
	}

	auto dl_load = conn.Query("LOAD ducklake;");
	if (dl_load->HasError()) {
		throw IOException("Failed to load DuckLake extension: " + dl_load->GetError());
	}
}

void DuckSyncStorageManager::AttachDuckLake() {
	if (ducklake_attached_) {
		return;
	}

	// Install required extensions (DuckLake + Snowflake)
	InstallRequiredExtensions();

	auto conn = GetConnection();

	// Attach DuckLake with PostgreSQL catalog and data path
	// Syntax: ATTACH 'ducklake:postgres:connection_string' AS name (DATA_PATH 'path');
	// See: https://ducklake.select/docs/stable/duckdb/usage/connecting
	std::ostringstream attach_sql;
	attach_sql << "ATTACH 'ducklake:postgres:" << config_.pg_connection_string << "' AS " << ducklake_name_
	           << " (DATA_PATH '" << config_.data_path << "');";

	auto attach_result = conn.Query(attach_sql.str());
	if (attach_result->HasError()) {
		throw IOException("Failed to attach DuckLake: " + attach_result->GetError());
	}

	ducklake_attached_ = true;
}

std::string DuckSyncStorageManager::GetDuckLakeTableName(const std::string &cache_name,
                                                         const std::string &source_name) {
	// Format: ducksync.source_name.cache_name
	std::ostringstream table_name;
	table_name << ducklake_name_ << "." << source_name << "." << cache_name;
	return table_name.str();
}

bool DuckSyncStorageManager::TableExists(const std::string &cache_name, const std::string &source_name) {
	if (!ducklake_attached_) {
		return false;
	}

	auto conn = GetConnection();

	std::ostringstream sql;
	sql << "SELECT COUNT(*) FROM information_schema.tables "
	    << "WHERE table_catalog = '" << ducklake_name_ << "' "
	    << "AND table_schema = '" << source_name << "' "
	    << "AND table_name = '" << cache_name << "';";

	auto result = conn.Query(sql.str());
	if (result->HasError()) {
		return false;
	}

	if (result->RowCount() == 0) {
		return false;
	}

	auto count = result->GetValue(0, 0).GetValue<int64_t>();
	return count > 0;
}

void DuckSyncStorageManager::CreateCacheTable(const std::string &cache_name, const std::string &source_name,
                                              const std::string &query_result) {
	if (!ducklake_attached_) {
		throw IOException("DuckLake not attached");
	}

	auto conn = GetConnection();

	// Create schema if it doesn't exist
	std::ostringstream create_schema;
	create_schema << "CREATE SCHEMA IF NOT EXISTS " << ducklake_name_ << "." << source_name << ";";
	auto schema_result = conn.Query(create_schema.str());
	if (schema_result->HasError()) {
		throw IOException("Failed to create schema: " + schema_result->GetError());
	}

	// Create or replace the cache table
	std::string table_name = GetDuckLakeTableName(cache_name, source_name);

	std::ostringstream create_table;
	create_table << "CREATE OR REPLACE TABLE " << table_name << " AS " << query_result << ";";

	auto result = conn.Query(create_table.str());
	if (result->HasError()) {
		throw IOException("Failed to create cache table: " + result->GetError());
	}
}

void DuckSyncStorageManager::WriteQueryResult(const std::string &cache_name, const std::string &source_name,
                                              MaterializedQueryResult &result) {
	if (!ducklake_attached_) {
		throw IOException("DuckLake not attached");
	}

	auto conn = GetConnection();

	// Create schema if it doesn't exist
	std::ostringstream create_schema;
	create_schema << "CREATE SCHEMA IF NOT EXISTS " << ducklake_name_ << "." << source_name << ";";
	auto schema_result = conn.Query(create_schema.str());
	if (schema_result->HasError()) {
		throw IOException("Failed to create schema: " + schema_result->GetError());
	}

	std::string table_name = GetDuckLakeTableName(cache_name, source_name);

	// Build CREATE TABLE statement based on result columns
	std::ostringstream create_table;
	create_table << "CREATE OR REPLACE TABLE " << table_name << " (";

	auto &types = result.types;
	auto &names = result.names;

	for (idx_t i = 0; i < types.size(); i++) {
		if (i > 0)
			create_table << ", ";
		create_table << "\"" << names[i] << "\" " << types[i].ToString();
	}
	create_table << ");";

	auto create_result = conn.Query(create_table.str());
	if (create_result->HasError()) {
		throw IOException("Failed to create cache table: " + create_result->GetError());
	}

	// Insert data from result
	if (result.RowCount() == 0) {
		return; // No data to insert
	}

	std::ostringstream insert_sql;
	insert_sql << "INSERT INTO " << table_name << " VALUES ";

	bool first_row = true;
	for (idx_t row = 0; row < result.RowCount(); row++) {
		if (!first_row)
			insert_sql << ", ";
		insert_sql << "(";

		for (idx_t col = 0; col < types.size(); col++) {
			if (col > 0)
				insert_sql << ", ";
			auto val = result.GetValue(col, row);
			if (val.IsNull()) {
				insert_sql << "NULL";
			} else if (types[col].IsNumeric()) {
				insert_sql << val.ToString();
			} else {
				// Escape single quotes
				std::string str_val = val.ToString();
				std::ostringstream escaped;
				for (char c : str_val) {
					if (c == '\'')
						escaped << "''";
					else
						escaped << c;
				}
				insert_sql << "'" << escaped.str() << "'";
			}
		}
		insert_sql << ")";
		first_row = false;
	}
	insert_sql << ";";

	auto insert_result = conn.Query(insert_sql.str());
	if (insert_result->HasError()) {
		throw IOException("Failed to insert data into cache table: " + insert_result->GetError());
	}
}

} // namespace duckdb
