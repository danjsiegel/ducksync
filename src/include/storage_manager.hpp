#pragma once

#include "duckdb.hpp"
#include <string>

namespace duckdb {

struct StorageConfig {
	std::string pg_connection_string; // PostgreSQL for DuckLake catalog
	std::string data_path;            // S3 or local path for parquet files
};

// Manages DuckLake attachment and cache data storage
class DuckSyncStorageManager {
public:
	explicit DuckSyncStorageManager(ClientContext &context);
	~DuckSyncStorageManager();

	// Setup storage by attaching DuckLake (full setup)
	void SetupStorage(const std::string &pg_connection_string, const std::string &data_path);

	// Use an existing DuckLake catalog (simpler init)
	void UseExistingCatalog(const std::string &catalog_name);

	// Get the DuckLake catalog name (for use in queries)
	const std::string &GetDuckLakeName() const {
		return ducklake_name_;
	}

	// Check if DuckLake is attached
	bool IsAttached() const {
		return ducklake_attached_;
	}

	// Table operations
	bool TableExists(const std::string &cache_name, const std::string &source_name);
	void CreateCacheTable(const std::string &cache_name, const std::string &source_name,
	                      const std::string &query_result);
	void WriteQueryResult(const std::string &cache_name, const std::string &source_name,
	                      MaterializedQueryResult &result);

	// Get fully qualified table name for a cache
	std::string GetDuckLakeTableName(const std::string &cache_name, const std::string &source_name);

private:
	ClientContext &context_;
	StorageConfig config_;
	bool ducklake_attached_;
	std::string ducklake_name_; // Name of the attached DuckLake catalog

	Connection GetConnection();
	void InstallRequiredExtensions();
	void AttachDuckLake();
};

} // namespace duckdb
