#include "query_router.hpp"
#include "duckdb_compat.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/replacement_scan.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

#include <unordered_set>

namespace duckdb {

static const char *const DUCKSYNC_STATE_KEY = "ducksync_state";

static bool CaseInsensitiveEquals(const string &left, const string &right) {
	return StringUtil::Lower(left) == StringUtil::Lower(right);
}

static void AddLookupCandidate(vector<string> &candidates, std::unordered_set<string> &seen, const string &candidate) {
	if (candidate.empty()) {
		return;
	}
	auto normalized = StringUtil::Lower(candidate);
	if (seen.insert(normalized).second) {
		candidates.push_back(candidate);
	}
}

static vector<string> BuildLookupCandidates(ReplacementScanInput &input) {
	vector<string> candidates;
	std::unordered_set<string> seen;
	AddLookupCandidate(candidates, seen, ReplacementScan::GetFullPath(input));
	if (!input.schema_name.empty()) {
		AddLookupCandidate(candidates, seen, input.schema_name + "." + input.table_name);
	}
	AddLookupCandidate(candidates, seen, input.table_name);
	return candidates;
}

static bool MonitorTableMatchesInput(const string &monitor_table, const vector<string> &candidates) {
	for (const auto &candidate : candidates) {
		if (CaseInsensitiveEquals(monitor_table, candidate)) {
			return true;
		}
	}

	auto parts = StringUtil::Split(monitor_table, ".");
	if (parts.size() >= 2) {
		auto schema_table = parts[parts.size() - 2] + "." + parts[parts.size() - 1];
		for (const auto &candidate : candidates) {
			if (CaseInsensitiveEquals(schema_table, candidate)) {
				return true;
			}
		}
	}
	if (!parts.empty()) {
		const auto &table_name = parts.back();
		for (const auto &candidate : candidates) {
			if (CaseInsensitiveEquals(table_name, candidate)) {
				return true;
			}
		}
	}
	return false;
}

static bool LookupCacheForInput(DuckSyncMetadataManager &metadata_manager, ReplacementScanInput &input,
                                CacheDefinition &cache) {
	if (metadata_manager.GetCache(input.table_name, cache)) {
		return true;
	}

	auto candidates = BuildLookupCandidates(input);
	auto caches = metadata_manager.ListCaches();
	for (auto &candidate_cache : caches) {
		if (CaseInsensitiveEquals(candidate_cache.cache_name, input.table_name)) {
			cache = candidate_cache;
			return true;
		}
		for (const auto &monitor_table : candidate_cache.monitor_tables) {
			if (MonitorTableMatchesInput(monitor_table, candidates)) {
				cache = candidate_cache;
				return true;
			}
		}
	}
	return false;
}

static unique_ptr<TableRef> DuckSyncReplacementScan(ClientContext &context, ReplacementScanInput &input,
                                                    optional_ptr<ReplacementScanData> data) {
	(void)data;

	auto &state = GetDuckSyncState(context);
	if (!state.initialized || !state.metadata_manager || !state.storage_manager) {
		return nullptr;
	}

	CacheDefinition cache;
	if (!LookupCacheForInput(*state.metadata_manager, input, cache)) {
		return nullptr;
	}

	CacheState cache_state;
	if (!state.metadata_manager->GetState(cache.cache_name, cache_state) || !cache_state.HasLastRefresh()) {
		auto requested_name = ReplacementScan::GetFullPath(input);
		throw InvalidInputException("DuckSync table '" + requested_name + "' is monitored by cache '" +
		                            cache.cache_name + "' but is not yet cached. Run SELECT * FROM ducksync_refresh('" +
		                            cache.cache_name + "'); or query it explicitly with ducksync_query(...).");
	}

	auto table_ref = make_uniq<BaseTableRef>();
	ducksync::SetTableRefFields(*table_ref, state.storage_manager->GetDuckLakeName(), cache.source_name,
	                            cache.cache_name);
	return std::move(table_ref);
}

DuckSyncState &GetDuckSyncState(ClientContext &context) {
	auto state = context.registered_state->GetOrCreate<DuckSyncState>(DUCKSYNC_STATE_KEY);
	return *state;
}

void QueryRouter::Register(DatabaseInstance &db) {
	auto &config = DBConfig::GetConfig(db);
	config.replacement_scans.emplace_back(DuckSyncReplacementScan);
}

} // namespace duckdb
