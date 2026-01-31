#pragma once

#include "duckdb.hpp"
#include "metadata_manager.hpp"
#include "storage_manager.hpp"
#include <memory>
#include <string>

namespace duckdb {

struct DuckSyncState : public ClientContextState {
	std::unique_ptr<DuckSyncMetadataManager> metadata_manager;
	std::unique_ptr<DuckSyncStorageManager> storage_manager;
	std::string postgres_connection_string;
	bool initialized = false;

	void QueryEnd() override {
	}
};

class QueryRouter {
public:
	static void Register(DatabaseInstance &db);
};

DuckSyncState &GetDuckSyncState(ClientContext &context);

} // namespace duckdb
