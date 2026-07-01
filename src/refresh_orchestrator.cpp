#include "refresh_orchestrator.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include <sstream>
#include <chrono>
#include <iomanip>
#include <openssl/sha.h>

namespace duckdb {

// Helper to create a connection from context
static Connection MakeConnection(ClientContext &context) {
	return Connection(*context.db);
}

struct ParsedMonitorTable {
	std::string database_name;
	std::string schema_name;
	std::string table_name;
	std::string full_name;
};

static std::string EscapeSqlStringLiteral(const std::string &value) {
	std::string escaped;
	escaped.reserve(value.size());
	for (char c : value) {
		if (c == '\'') {
			escaped += "''";
		} else {
			escaped += c;
		}
	}
	return escaped;
}

static std::string ToLower(const std::string &value) {
	std::string lower = value;
	std::transform(lower.begin(), lower.end(), lower.begin(),
	               [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
	return lower;
}

static ParsedMonitorTable ParseMonitorTableName(const std::string &monitor_table) {
	auto parts = StringUtil::Split(monitor_table, ".");
	if (parts.size() != 3) {
		throw InvalidInputException("Monitor table '" + monitor_table +
		                            "' must be fully qualified as DATABASE.SCHEMA.TABLE");
	}

	ParsedMonitorTable parsed;
	parsed.database_name = parts[0];
	parsed.schema_name = parts[1];
	parsed.table_name = parts[2];
	parsed.full_name = parts[0] + "." + parts[1] + "." + parts[2];
	return parsed;
}

template <typename SnapshotType>
static void SaveSnapshots(DuckSyncMetadataManager &metadata_manager, const std::string &cache_name,
                          const std::unordered_map<std::string, SnapshotType> &snapshots) {
	metadata_manager.DeleteTableSnapshots(cache_name);
	for (const auto &entry : snapshots) {
		metadata_manager.SaveTableSnapshot(cache_name, entry.first, entry.second.rows, entry.second.bytes);
	}
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

		// Step 4: Force / manual dispatch
		if (force) {
			int64_t rows = ExecuteRefresh(cache, source);
			std::string state_hash;
			if (cache.invalidation_mode == "last_altered" || cache.invalidation_mode == "two_stage") {
				auto source_metadata = GetSourceTableMetadata(source.secret_name, cache.monitor_tables);
				state_hash = GenerateStateHash(source_metadata);
			}
			if (cache.invalidation_mode == "two_stage") {
				auto rows_bytes = GetSourceTableRowsAndBytes(cache.metadata_secret_name, cache.monitor_tables);
				SaveSnapshots(metadata_manager_, cache_name, rows_bytes);
			}
			UpdateCacheState(cache_name, state_hash, cache);

			auto end_time = std::chrono::high_resolution_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
			status.result = RefreshResult::REFRESHED;
			status.message = "Cache refreshed successfully";
			status.rows_refreshed = rows;
			status.has_rows = true;
			status.duration_ms = static_cast<double>(duration.count());
			status.has_duration = true;
			return status;
		}

		if (cache.invalidation_mode == "manual") {
			status.result = RefreshResult::SKIPPED;
			status.message = "Cache refresh skipped because invalidation_mode is manual";
			return status;
		}

		if (!has_state) {
			int64_t rows = ExecuteRefresh(cache, source);
			std::string state_hash;
			if (cache.invalidation_mode == "last_altered" || cache.invalidation_mode == "two_stage") {
				auto source_metadata = GetSourceTableMetadata(source.secret_name, cache.monitor_tables);
				state_hash = GenerateStateHash(source_metadata);
			}
			if (cache.invalidation_mode == "two_stage") {
				auto rows_bytes = GetSourceTableRowsAndBytes(cache.metadata_secret_name, cache.monitor_tables);
				SaveSnapshots(metadata_manager_, cache_name, rows_bytes);
			}
			UpdateCacheState(cache_name, state_hash, cache);

			auto end_time = std::chrono::high_resolution_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
			status.result = RefreshResult::REFRESHED;
			status.message = "Cache refreshed successfully";
			status.rows_refreshed = rows;
			status.has_rows = true;
			status.duration_ms = static_cast<double>(duration.count());
			status.has_duration = true;
			return status;
		}

		if (IsTTLExpired(state, cache)) {
			int64_t rows = ExecuteRefresh(cache, source);
			std::string state_hash;
			if (cache.invalidation_mode == "last_altered" || cache.invalidation_mode == "two_stage") {
				auto source_metadata = GetSourceTableMetadata(source.secret_name, cache.monitor_tables);
				state_hash = GenerateStateHash(source_metadata);
			}
			if (cache.invalidation_mode == "two_stage") {
				auto rows_bytes = GetSourceTableRowsAndBytes(cache.metadata_secret_name, cache.monitor_tables);
				SaveSnapshots(metadata_manager_, cache_name, rows_bytes);
			}
			UpdateCacheState(cache_name, state_hash, cache);

			auto end_time = std::chrono::high_resolution_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
			status.result = RefreshResult::REFRESHED;
			status.message = "Cache refreshed successfully";
			status.rows_refreshed = rows;
			status.has_rows = true;
			status.duration_ms = static_cast<double>(duration.count());
			status.has_duration = true;
			return status;
		}

		if (cache.invalidation_mode == "ttl_only") {
			status.result = RefreshResult::SKIPPED;
			status.message = "Cache TTL is still valid, no refresh needed";
			return status;
		}

		if (cache.invalidation_mode == "two_stage") {
			auto stored_snapshots = metadata_manager_.GetTableSnapshot(cache_name);
			auto current_snapshots = GetSourceTableRowsAndBytes(cache.metadata_secret_name, cache.monitor_tables);

			bool stage1_changed = stored_snapshots.size() != current_snapshots.size();
			if (!stage1_changed) {
				for (const auto &entry : current_snapshots) {
					auto stored = stored_snapshots.find(entry.first);
					if (stored == stored_snapshots.end() || stored->second.rows != entry.second.rows ||
					    stored->second.bytes != entry.second.bytes) {
						stage1_changed = true;
						break;
					}
				}
			}

			if (!stage1_changed) {
				status.result = RefreshResult::SKIPPED;
				status.message = "Stage 1 rows/bytes snapshot unchanged, no refresh needed";
				return status;
			}

			auto source_metadata = GetSourceTableMetadata(source.secret_name, cache.monitor_tables);
			auto new_hash = GenerateStateHash(source_metadata);
			if (state.HasStateHash() && new_hash == state.source_state_hash) {
				SaveSnapshots(metadata_manager_, cache_name, current_snapshots);
				status.result = RefreshResult::SKIPPED;
				status.message = "Stage 1 changed but Stage 2 last_altered did not; skipping false positive refresh";
				return status;
			}

			int64_t rows = ExecuteRefresh(cache, source);
			auto refreshed_metadata = GetSourceTableMetadata(source.secret_name, cache.monitor_tables);
			auto state_hash = GenerateStateHash(refreshed_metadata);
			auto refreshed_snapshots = GetSourceTableRowsAndBytes(cache.metadata_secret_name, cache.monitor_tables);
			SaveSnapshots(metadata_manager_, cache_name, refreshed_snapshots);
			UpdateCacheState(cache_name, state_hash, cache);

			auto end_time = std::chrono::high_resolution_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
			status.result = RefreshResult::REFRESHED;
			status.message = "Cache refreshed successfully";
			status.rows_refreshed = rows;
			status.has_rows = true;
			status.duration_ms = static_cast<double>(duration.count());
			status.has_duration = true;
			return status;
		}

		if (!state.HasStateHash()) {
			int64_t rows = ExecuteRefresh(cache, source);
			auto source_metadata = GetSourceTableMetadata(source.secret_name, cache.monitor_tables);
			auto state_hash = GenerateStateHash(source_metadata);
			UpdateCacheState(cache_name, state_hash, cache);

			auto end_time = std::chrono::high_resolution_clock::now();
			auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
			status.result = RefreshResult::REFRESHED;
			status.message = "Cache refreshed successfully";
			status.rows_refreshed = rows;
			status.has_rows = true;
			status.duration_ms = static_cast<double>(duration.count());
			status.has_duration = true;
			return status;
		}

		auto source_metadata = GetSourceTableMetadata(source.secret_name, cache.monitor_tables);
		auto new_hash = GenerateStateHash(source_metadata);
		if (new_hash == state.source_state_hash) {
			status.result = RefreshResult::SKIPPED;
			status.message = "Cache is fresh, no refresh needed";
			return status;
		}

		int64_t rows = ExecuteRefresh(cache, source);
		auto refreshed_metadata = GetSourceTableMetadata(source.secret_name, cache.monitor_tables);
		auto state_hash = GenerateStateHash(refreshed_metadata);
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

	for (const auto &monitor_table : monitor_tables) {
		auto parsed = ParseMonitorTableName(monitor_table);

		std::ostringstream sf_query;
		sf_query << "SELECT CONCAT(table_catalog, ''.'', table_schema, ''.'', table_name) AS full_name, last_altered "
		         << "FROM " << parsed.database_name << ".information_schema.tables "
		         << "WHERE table_schema = ''" << EscapeSqlStringLiteral(parsed.schema_name) << "'' "
		         << "AND table_name = ''" << EscapeSqlStringLiteral(parsed.table_name) << "''";

		std::ostringstream query;
		query << "SELECT * FROM snowflake_query('" << sf_query.str() << "', '" << EscapeSqlStringLiteral(secret_name)
		      << "');";

		auto result = conn.Query(query.str());
		if (result->HasError()) {
			throw IOException("Failed to query Snowflake metadata: " + result->GetError());
		}
		if (result->RowCount() == 0) {
			throw IOException("No Snowflake metadata returned for source table '" + monitor_table + "'");
		}

		for (idx_t row = 0; row < result->RowCount(); row++) {
			auto table_name = result->GetValue(0, row).ToString();
			auto last_altered = result->GetValue(1, row).ToString();
			metadata[table_name] = last_altered;
		}
	}

	return metadata;
}

std::unordered_map<std::string, RowsBytesSnapshot>
RefreshOrchestrator::GetSourceTableRowsAndBytes(const std::string &metadata_secret_name,
                                                const std::vector<std::string> &monitor_tables) {
	std::unordered_map<std::string, RowsBytesSnapshot> snapshots;
	auto conn = MakeConnection(context_);

	for (const auto &monitor_table : monitor_tables) {
		auto parsed = ParseMonitorTableName(monitor_table);

		std::ostringstream show_tables_sql;
		show_tables_sql << "SHOW TABLES LIKE ''" << EscapeSqlStringLiteral(parsed.table_name) << "'' IN SCHEMA "
		                << parsed.database_name << "." << parsed.schema_name;

		std::ostringstream query;
		query << "SELECT * FROM snowflake_query('" << show_tables_sql.str() << "', '"
		      << EscapeSqlStringLiteral(metadata_secret_name) << "');";

		auto result = conn.Query(query.str());
		if (result->HasError()) {
			throw IOException("Failed to query Snowflake SHOW TABLES metadata: " + result->GetError());
		}
		if (result->RowCount() == 0) {
			throw IOException("SHOW TABLES returned no metadata for source table '" + monitor_table + "'");
		}

		idx_t rows_col = DConstants::INVALID_INDEX;
		idx_t bytes_col = DConstants::INVALID_INDEX;
		for (idx_t col = 0; col < result->names.size(); col++) {
			auto column_name = ToLower(result->names[col]);
			if (column_name == "rows") {
				rows_col = col;
			} else if (column_name == "bytes") {
				bytes_col = col;
			}
		}

		if (rows_col == DConstants::INVALID_INDEX || bytes_col == DConstants::INVALID_INDEX) {
			throw IOException("SHOW TABLES metadata for '" + monitor_table +
			                  "' did not include required rows/bytes columns");
		}

		RowsBytesSnapshot snapshot;
		snapshot.rows = result->GetValue(rows_col, 0).GetValue<int64_t>();
		snapshot.bytes = result->GetValue(bytes_col, 0).GetValue<int64_t>();
		snapshots[parsed.full_name] = snapshot;
	}

	return snapshots;
}

std::string RefreshOrchestrator::GenerateStateHash(const std::unordered_map<std::string, std::string> &metadata) {
	// Build a sorted JSON-like string for consistent hashing
	std::vector<std::pair<std::string, std::string>> sorted_metadata(metadata.begin(), metadata.end());
	std::sort(sorted_metadata.begin(), sorted_metadata.end());

	std::ostringstream json;
	json << "{";
	for (size_t i = 0; i < sorted_metadata.size(); i++) {
		if (i > 0) {
			json << ",";
		}
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

	// Escape single quotes in source query for snowflake_query()
	std::string escaped_query;
	for (char c : cache.source_query) {
		if (c == '\'') {
			escaped_query += "''";
		} else {
			escaped_query += c;
		}
	}

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

	// Single Snowflake query: CREATE TABLE AS SELECT (no double fetch)
	std::ostringstream create_table;
	create_table << "CREATE OR REPLACE TABLE " << table_name << " AS "
	             << "SELECT * FROM snowflake_query('" << escaped_query << "', '" << source.secret_name << "');";

	auto create_result = conn.Query(create_table.str());
	if (create_result->HasError()) {
		throw IOException("Failed to create cache table: " + create_result->GetError());
	}

	// Count rows from the local cache table (no Snowflake round-trip)
	std::ostringstream count_sql;
	count_sql << "SELECT COUNT(*) FROM " << table_name << ";";
	auto count_result = conn.Query(count_sql.str());
	if (!count_result->HasError() && count_result->RowCount() > 0) {
		return count_result->GetValue(0, 0).GetValue<int64_t>();
	}

	return 0;
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
