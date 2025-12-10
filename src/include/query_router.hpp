#pragma once

#include "duckdb.hpp"
#include "metadata_manager.hpp"
#include "storage_manager.hpp"
#include <string>
#include <memory>

namespace duckdb {

// Query router state stored in the database instance
struct DuckSyncState : public ClientContextState {
	std::unique_ptr<DuckSyncMetadataManager> metadata_manager;
	std::unique_ptr<DuckSyncStorageManager> storage_manager;
	std::string postgres_connection_string;
	bool initialized = false;

	void QueryEnd() override {
	}
};

// Implements transparent query routing via replacement_scan
class QueryRouter {
public:
	// Register replacement_scan hook with DuckDB
	static void Register(DatabaseInstance &db);

	// Replacement scan function
	static unique_ptr<TableRef> ReplacementScan(ClientContext &context, ReplacementScanInput &input,
	                                            optional_ptr<ReplacementScanData> data);

private:
	// Layer 1: Check if table name matches a registered cache
	static bool FindCache(ClientContext &context, const std::string &table_name, CacheDefinition &out);

	// Layer 2: Check if cache is valid (not expired, has been refreshed)
	static bool IsCacheValid(ClientContext &context, const CacheDefinition &cache);

	// Layer 2b: Auto-refresh cache if expired (synchronous)
	static void AutoRefreshCache(ClientContext &context, const CacheDefinition &cache);

	// Layer 3: Get DuckLake table reference for cache
	static unique_ptr<TableRef> GetCacheTableRef(ClientContext &context, const CacheDefinition &cache);

	// Handle passthrough to Snowflake (not used in MVP)
	static unique_ptr<TableRef> HandlePassthrough(ClientContext &context, const std::string &table_name,
	                                              const SourceDefinition &source);
};

// Get or create DuckSync state for context
DuckSyncState &GetDuckSyncState(ClientContext &context);

} // namespace duckdb
