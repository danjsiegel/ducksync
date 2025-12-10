#pragma once

#include "duckdb.hpp"
#include "storage_manager.hpp"
#include <string>

namespace duckdb {

// Result of a cleanup operation
struct CleanupResult {
	int64_t snapshots_expired;
	int64_t files_cleaned;
	int64_t orphans_deleted;
	std::string message;
};

// Manages DuckLake cleanup operations
class CleanupManager {
public:
	explicit CleanupManager(ClientContext &context, DuckSyncStorageManager &storage_manager);
	~CleanupManager();

	// Run cleanup for a specific cache
	CleanupResult CleanupCache(const std::string &cache_name, const std::string &source_name);

	// Run cleanup for all caches
	CleanupResult CleanupAll();

	// Individual cleanup operations
	int64_t ExpireSnapshots(const std::string &cache_name, const std::string &source_name);
	int64_t CleanupOldFiles(const std::string &cache_name, const std::string &source_name, int64_t older_than_days = 7);
	int64_t DeleteOrphanedFiles(const std::string &cache_name, const std::string &source_name);

private:
	ClientContext &context_;
	DuckSyncStorageManager &storage_manager_;
};

} // namespace duckdb
