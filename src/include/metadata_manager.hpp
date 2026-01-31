#pragma once

#include "duckdb.hpp"
#include <string>
#include <vector>

namespace duckdb {

struct SourceDefinition {
	std::string source_name;
	std::string driver_type;
	std::string secret_name;
	bool passthrough_enabled;
	std::string created_at;
};

struct CacheDefinition {
	std::string cache_name;
	std::string source_name;
	std::string source_query;
	std::vector<std::string> monitor_tables;
	int64_t ttl_seconds;
	bool has_ttl;
	std::string created_at;
};

struct CacheState {
	std::string cache_name;
	std::string last_refresh;
	std::string source_state_hash;
	std::string expires_at;

	bool HasLastRefresh() const {
		return !last_refresh.empty();
	}
	bool HasStateHash() const {
		return !source_state_hash.empty();
	}
	bool HasExpiresAt() const {
		return !expires_at.empty();
	}
};

// Manages DuckSync metadata stored in the DuckLake catalog (PostgreSQL)
// All queries run through the attached DuckLake connection
class DuckSyncMetadataManager {
public:
	explicit DuckSyncMetadataManager(ClientContext &context);

	// Initialize schema in the DuckLake catalog
	void Initialize(const std::string &ducklake_name);

	// Source CRUD
	void CreateSource(const SourceDefinition &source);
	bool GetSource(const std::string &source_name, SourceDefinition &out);
	std::vector<SourceDefinition> ListSources();
	void DeleteSource(const std::string &source_name);

	// Cache CRUD
	void CreateCache(const CacheDefinition &cache);
	bool GetCache(const std::string &cache_name, CacheDefinition &out);
	bool GetCacheByMonitorTable(const std::string &table_name, CacheDefinition &out);
	std::vector<CacheDefinition> ListCaches();
	void DeleteCache(const std::string &cache_name);

	// Get the DuckLake name for constructing table paths
	std::string GetDuckLakeName() const {
		return ducklake_name_;
	}

	// State operations
	void InitializeState(const std::string &cache_name);
	void UpdateState(const CacheState &state);
	bool GetState(const std::string &cache_name, CacheState &out);

private:
	ClientContext &context_;
	std::string ducklake_name_; // e.g., "my_lake" - the attached DuckLake
	bool initialized_;

	void ExecuteSQL(const std::string &sql);
	unique_ptr<MaterializedQueryResult> QuerySQL(const std::string &sql);

	// Full table name: ducklake_name.ducksync.table_name
	std::string TableName(const std::string &table) const;
};

} // namespace duckdb
