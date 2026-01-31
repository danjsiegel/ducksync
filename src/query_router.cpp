#include "query_router.hpp"

namespace duckdb {

static const char *const DUCKSYNC_STATE_KEY = "ducksync_state";

DuckSyncState &GetDuckSyncState(ClientContext &context) {
	auto state = context.registered_state->GetOrCreate<DuckSyncState>(DUCKSYNC_STATE_KEY);
	return *state;
}

void QueryRouter::Register(DatabaseInstance &db) {
}

} // namespace duckdb
