#include "refresh_orchestrator.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/exception.hpp"
#include <sstream>
#include <chrono>
#include <iomanip>
#include <openssl/sha.h>

namespace duckdb {

// Helper to create a connection from context
static Connection MakeConnection(ClientContext &context) {
	return Connection(*context.db);
}

RefreshOrchestrator::RefreshOrchestrator(ClientContext &context, DuckSyncMetadataManager &metadata_manager,
                                         DuckSyncStorageManager &storage_manager)
    : context_(context), metadata_manager_(metadata_manager), storage_manager_(storage_manager) {
}

RefreshOrchestrator::~RefreshOrchestrator() {
}

RefreshStatus RefreshOrchestrator::Refresh(const std::string &cache_name, bool force) {
	RefreshStatus status;
	auto start_time = std::chrono::high_resolution_clock::now();

	try {
		// Step 1: Get cache definition
		CacheDefinition cache;
		if (!metadata_manager_.GetCache(cache_name, cache)) {
			status.result = RefreshResult::ERROR;
			status.message = "Cache '" + cache_name + "' not found";
			return status;
		}

		// Step 2: Get source definition
		SourceDefinition source;
		if (!metadata_manager_.GetSource(cache.source_name, source)) {
			status.result = RefreshResult::ERROR;
			status.message = "Source '" + cache.source_name + "' not found";
			return status;
		}

		// Step 3: Get current state
		CacheState state;
		bool has_state = metadata_manager_.GetState(cache_name, state);

		// Step 4: Check if force refresh or TTL expired
		bool needs_refresh = force;

		if (!force && has_state) {
			if (IsTTLExpired(state, cache)) {
				needs_refresh = true;
			}
		} else if (!has_state) {
			// No state means never refreshed
			needs_refresh = true;
		}

		// Step 5: Smart check - query source metadata
		if (!needs_refresh && has_state && state.HasStateHash()) {
			auto source_metadata = GetSourceTableMetadata(source.secret_name, cache.monitor_tables);
			auto new_hash = GenerateStateHash(source_metadata);

			if (new_hash != state.source_state_hash) {
				needs_refresh = true;
			}
		} else if (!force) {
			// First time or no hash stored
			needs_refresh = true;
		}

		if (!needs_refresh) {
			status.result = RefreshResult::SKIPPED;
			status.message = "Cache is fresh, no refresh needed";
			return status;
		}

		// Step 6: Execute refresh
		int64_t rows = ExecuteRefresh(cache, source);

		// Step 7: Get new source metadata hash
		auto source_metadata = GetSourceTableMetadata(source.secret_name, cache.monitor_tables);
		auto state_hash = GenerateStateHash(source_metadata);

		// Step 8: Update state
		UpdateCacheState(cache_name, state_hash, cache);

		auto end_time = std::chrono::high_resolution_clock::now();
		auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

		status.result = RefreshResult::REFRESHED;
		status.message = "Cache refreshed successfully";
		status.rows_refreshed = rows;
		status.has_rows = true;
		status.duration_ms = static_cast<double>(duration.count());
		status.has_duration = true;

	} catch (const std::exception &e) {
		status.result = RefreshResult::ERROR;
		status.message = std::string("Refresh failed: ") + e.what();
	}

	return status;
}

bool RefreshOrchestrator::IsTTLExpired(const CacheState &state, const CacheDefinition &cache) {
	// If no TTL set, never expires
	if (!cache.has_ttl) {
		return false;
	}

	// If no expires_at set, consider expired
	if (!state.HasExpiresAt()) {
		return true;
	}

	// Parse expires_at and compare to now
	// For simplicity, we'll use a SQL query to check
	auto conn = MakeConnection(context_);

	std::ostringstream sql;
	sql << "SELECT CASE WHEN TIMESTAMP '" << state.expires_at << "' < CURRENT_TIMESTAMP "
	    << "THEN TRUE ELSE FALSE END;";

	auto result = conn.Query(sql.str());
	if (result->HasError() || result->RowCount() == 0) {
		return true; // Err on side of refreshing
	}

	return result->GetValue(0, 0).GetValue<bool>();
}

std::unordered_map<std::string, std::string>
RefreshOrchestrator::GetSourceTableMetadata(const std::string &secret_name,
                                            const std::vector<std::string> &monitor_tables) {

	std::unordered_map<std::string, std::string> metadata;
	auto conn = MakeConnection(context_);

	// Build IN clause for table names
	std::ostringstream tables_in;
	tables_in << "(";
	for (size_t i = 0; i < monitor_tables.size(); i++) {
		if (i > 0)
			tables_in << ", ";
		// Extract just the table name (last part after dots)
		std::string table_name = monitor_tables[i];
		size_t last_dot = table_name.rfind('.');
		if (last_dot != std::string::npos) {
			table_name = table_name.substr(last_dot + 1);
		}
		tables_in << "'" << table_name << "'";
	}
	tables_in << ")";

	// Query Snowflake information_schema via snowflake_query
	std::ostringstream query;
	query << "SELECT * FROM snowflake_query('" << secret_name << "', '"
	      << "SELECT table_catalog || ''.'' || table_schema || ''.'' || table_name as full_name, "
	      << "last_altered FROM information_schema.tables "
	      << "WHERE table_name IN " << tables_in.str() << "');";

	auto result = conn.Query(query.str());
	if (result->HasError()) {
		throw IOException("Failed to query Snowflake metadata: " + result->GetError());
	}

	// Build metadata map
	for (idx_t row = 0; row < result->RowCount(); row++) {
		auto table_name = result->GetValue(0, row).ToString();
		auto last_altered = result->GetValue(1, row).ToString();
		metadata[table_name] = last_altered;
	}

	return metadata;
}

std::string RefreshOrchestrator::GenerateStateHash(const std::unordered_map<std::string, std::string> &metadata) {

	// Build a sorted JSON-like string for consistent hashing
	std::vector<std::pair<std::string, std::string>> sorted_metadata(metadata.begin(), metadata.end());
	std::sort(sorted_metadata.begin(), sorted_metadata.end());

	std::ostringstream json;
	json << "{";
	for (size_t i = 0; i < sorted_metadata.size(); i++) {
		if (i > 0)
			json << ",";
		json << "\"" << sorted_metadata[i].first << "\":\"" << sorted_metadata[i].second << "\"";
	}
	json << "}";

	// Generate SHA256 hash
	std::string input = json.str();
	unsigned char hash[SHA256_DIGEST_LENGTH];
	SHA256(reinterpret_cast<const unsigned char *>(input.c_str()), input.length(), hash);

	// Convert to hex string
	std::ostringstream hex;
	hex << std::hex << std::setfill('0');
	for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
		hex << std::setw(2) << static_cast<int>(hash[i]);
	}

	return hex.str();
}

int64_t RefreshOrchestrator::ExecuteRefresh(const CacheDefinition &cache, const SourceDefinition &source) {
	auto conn = MakeConnection(context_);

	// Execute query via Snowflake extension
	std::ostringstream query;
	query << "SELECT * FROM snowflake_query('" << source.secret_name << "', '" << cache.source_query << "');";

	auto result = conn.Query(query.str());
	if (result->HasError()) {
		throw IOException("Failed to execute source query: " + result->GetError());
	}

	int64_t row_count = result->RowCount();

	// Create or replace cache table in DuckLake
	if (!storage_manager_.IsAttached()) {
		throw IOException("DuckLake storage not attached");
	}

	std::string table_name = storage_manager_.GetDuckLakeTableName(cache.cache_name, cache.source_name);

	// Create schema if needed
	std::ostringstream create_schema;
	create_schema << "CREATE SCHEMA IF NOT EXISTS " << storage_manager_.GetDuckLakeName() << "." << cache.source_name
	              << ";";
	auto schema_result = conn.Query(create_schema.str());
	if (schema_result->HasError()) {
		throw IOException("Failed to create schema: " + schema_result->GetError());
	}

	// Create table from query
	std::ostringstream create_table;
	create_table << "CREATE OR REPLACE TABLE " << table_name << " AS "
	             << "SELECT * FROM snowflake_query('" << source.secret_name << "', '" << cache.source_query << "');";

	auto create_result = conn.Query(create_table.str());
	if (create_result->HasError()) {
		throw IOException("Failed to create cache table: " + create_result->GetError());
	}

	return row_count;
}

void RefreshOrchestrator::UpdateCacheState(const std::string &cache_name, const std::string &state_hash,
                                           const CacheDefinition &cache) {
	CacheState state;
	state.cache_name = cache_name;
	state.source_state_hash = state_hash;

	// Set last_refresh to current time
	auto conn = MakeConnection(context_);
	auto now_result = conn.Query("SELECT CURRENT_TIMESTAMP::VARCHAR;");
	if (!now_result->HasError() && now_result->RowCount() > 0) {
		state.last_refresh = now_result->GetValue(0, 0).ToString();
	}

	// Calculate expires_at if TTL is set
	if (cache.has_ttl) {
		std::ostringstream expires_query;
		expires_query << "SELECT (CURRENT_TIMESTAMP + INTERVAL '" << cache.ttl_seconds << " seconds')::VARCHAR;";
		auto expires_result = conn.Query(expires_query.str());
		if (!expires_result->HasError() && expires_result->RowCount() > 0) {
			state.expires_at = expires_result->GetValue(0, 0).ToString();
		}
	}

	metadata_manager_.UpdateState(state);
}

} // namespace duckdb
