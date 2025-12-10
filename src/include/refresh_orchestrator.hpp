#pragma once

#include "duckdb.hpp"
#include "metadata_manager.hpp"
#include "storage_manager.hpp"
#include <string>
#include <memory>
#include <unordered_map>

namespace duckdb {

// Result of a refresh operation
enum class RefreshResult {
	SKIPPED,   // No refresh needed (data is fresh)
	REFRESHED, // Data was refreshed
	ERROR      // Error occurred during refresh
};

struct RefreshStatus {
	RefreshResult result;
	std::string message;
	int64_t rows_refreshed;
	double duration_ms;
	bool has_rows;
	bool has_duration;

	RefreshStatus()
	    : result(RefreshResult::ERROR), rows_refreshed(0), duration_ms(0), has_rows(false), has_duration(false) {
	}
};

// Orchestrates smart refresh logic for DuckSync
class RefreshOrchestrator {
public:
	RefreshOrchestrator(ClientContext &context, DuckSyncMetadataManager &metadata_manager,
	                    DuckSyncStorageManager &storage_manager);
	~RefreshOrchestrator();

	// Main refresh function
	RefreshStatus Refresh(const std::string &cache_name, bool force = false);

private:
	ClientContext &context_;
	DuckSyncMetadataManager &metadata_manager_;
	DuckSyncStorageManager &storage_manager_;

	// Check if TTL has expired
	bool IsTTLExpired(const CacheState &state, const CacheDefinition &cache);

	// Query Snowflake for table metadata
	std::unordered_map<std::string, std::string> GetSourceTableMetadata(const std::string &secret_name,
	                                                                    const std::vector<std::string> &monitor_tables);

	// Generate hash from table metadata
	std::string GenerateStateHash(const std::unordered_map<std::string, std::string> &metadata);

	// Execute source query and write to DuckLake
	int64_t ExecuteRefresh(const CacheDefinition &cache, const SourceDefinition &source);

	// Update cache state after refresh
	void UpdateCacheState(const std::string &cache_name, const std::string &state_hash, const CacheDefinition &cache);
};

} // namespace duckdb
