#define DUCKDB_EXTENSION_MAIN

#include "ducksync_extension.hpp"
#include "metadata_manager.hpp"
#include "storage_manager.hpp"
#include "refresh_orchestrator.hpp"
#include "query_router.hpp"
#include "cleanup_manager.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

#include <sstream>

namespace duckdb {

//===--------------------------------------------------------------------===//
// ducksync_setup_storage(pg_connection_string, data_path)
// - pg_connection_string: PostgreSQL connection for DuckLake catalog
// - data_path: S3 or local path for parquet file storage
//===--------------------------------------------------------------------===//
struct SetupStorageBindData : public TableFunctionData {
	std::string pg_connection_string;
	std::string data_path;
	bool done = false;
};

static unique_ptr<FunctionData> DuckSyncSetupStorageBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<SetupStorageBindData>();

	if (input.inputs.size() < 2) {
		throw InvalidInputException("ducksync_setup_storage requires 2 arguments: pg_connection_string, data_path");
	}

	result->pg_connection_string = input.inputs[0].GetValue<string>();
	result->data_path = input.inputs[1].GetValue<string>();

	// Don't do any work here - defer to the Function execution phase
	names.emplace_back("status");
	return_types.emplace_back(LogicalType::VARCHAR);

	return std::move(result);
}

static void DuckSyncSetupStorageFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<SetupStorageBindData>();

	if (bind_data.done) {
		output.SetCardinality(0);
		return;
	}

	// Do the actual setup work here in the execution phase
	auto &state = GetDuckSyncState(context);
	if (!state.storage_manager) {
		state.storage_manager = make_uniq<DuckSyncStorageManager>(context);
	}
	state.storage_manager->SetupStorage(bind_data.pg_connection_string, bind_data.data_path);

	// Initialize metadata manager (uses the attached DuckLake for storage)
	if (!state.metadata_manager) {
		state.metadata_manager = make_uniq<DuckSyncMetadataManager>(context);
	}
	state.metadata_manager->Initialize(state.storage_manager->GetDuckLakeName());

	state.initialized = true;
	bind_data.done = true;

	output.SetCardinality(1);
	output.SetValue(0, 0, Value("DuckSync storage configured successfully"));
}

//===--------------------------------------------------------------------===//
// ducksync_add_source(source_name, driver_type, secret_name, passthrough_enabled)
//===--------------------------------------------------------------------===//
struct AddSourceBindData : public TableFunctionData {
	std::string source_name;
	std::string driver_type;
	std::string secret_name;
	bool passthrough_enabled;
	bool done = false;
};

static unique_ptr<FunctionData> DuckSyncAddSourceBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<AddSourceBindData>();

	if (input.inputs.size() < 3) {
		throw InvalidInputException(
		    "ducksync_add_source requires at least 3 arguments: source_name, driver_type, secret_name");
	}

	result->source_name = input.inputs[0].GetValue<string>();
	result->driver_type = input.inputs[1].GetValue<string>();
	result->secret_name = input.inputs[2].GetValue<string>();
	result->passthrough_enabled = false;

	// Handle optional passthrough_enabled parameter
	for (auto &kv : input.named_parameters) {
		if (kv.first == "passthrough_enabled") {
			result->passthrough_enabled = kv.second.GetValue<bool>();
		}
	}

	// Validate driver type (Phase 1: Snowflake only)
	if (result->driver_type != "snowflake") {
		throw InvalidInputException("driver_type must be 'snowflake' (Phase 1)");
	}

	names.emplace_back("status");
	return_types.emplace_back(LogicalType::VARCHAR);

	return std::move(result);
}

static void DuckSyncAddSourceFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<AddSourceBindData>();

	if (bind_data.done) {
		output.SetCardinality(0);
		return;
	}

	auto &state = GetDuckSyncState(context);
	if (!state.metadata_manager) {
		throw InvalidInputException("DuckSync not initialized. Call ducksync_setup_storage first.");
	}

	SourceDefinition source;
	source.source_name = bind_data.source_name;
	source.driver_type = bind_data.driver_type;
	source.secret_name = bind_data.secret_name;
	source.passthrough_enabled = bind_data.passthrough_enabled;

	state.metadata_manager->CreateSource(source);
	bind_data.done = true;

	output.SetCardinality(1);
	output.SetValue(0, 0, Value("Source added successfully"));
}

//===--------------------------------------------------------------------===//
// ducksync_create_cache(cache_name, source_name, source_query, monitor_tables, ttl_seconds)
//===--------------------------------------------------------------------===//
struct CreateCacheBindData : public TableFunctionData {
	std::string cache_name;
	std::string source_name;
	std::string source_query;
	std::vector<std::string> monitor_tables;
	int64_t ttl_seconds;
	bool has_ttl;
	bool done = false;

	CreateCacheBindData() : ttl_seconds(-1), has_ttl(false) {
	}
};

static unique_ptr<FunctionData> DuckSyncCreateCacheBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<CreateCacheBindData>();

	if (input.inputs.size() < 4) {
		throw InvalidInputException("ducksync_create_cache requires at least 4 arguments: cache_name, source_name, "
		                            "source_query, monitor_tables");
	}

	result->cache_name = input.inputs[0].GetValue<string>();
	result->source_name = input.inputs[1].GetValue<string>();
	result->source_query = input.inputs[2].GetValue<string>();

	// Parse monitor_tables from list
	auto monitor_list = ListValue::GetChildren(input.inputs[3]);
	for (auto &table : monitor_list) {
		result->monitor_tables.push_back(table.GetValue<string>());
	}

	// Handle optional TTL parameter
	if (input.inputs.size() > 4 && !input.inputs[4].IsNull()) {
		result->ttl_seconds = input.inputs[4].GetValue<int64_t>();
		result->has_ttl = true;
	}

	names.emplace_back("status");
	return_types.emplace_back(LogicalType::VARCHAR);

	return std::move(result);
}

static void DuckSyncCreateCacheFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<CreateCacheBindData>();

	if (bind_data.done) {
		output.SetCardinality(0);
		return;
	}

	auto &state = GetDuckSyncState(context);
	if (!state.metadata_manager) {
		throw InvalidInputException("DuckSync not initialized. Call ducksync_setup_storage first.");
	}

	// Verify source exists
	SourceDefinition source;
	if (!state.metadata_manager->GetSource(bind_data.source_name, source)) {
		throw InvalidInputException("Source '" + bind_data.source_name + "' does not exist");
	}

	CacheDefinition cache;
	cache.cache_name = bind_data.cache_name;
	cache.source_name = bind_data.source_name;
	cache.source_query = bind_data.source_query;
	cache.monitor_tables = bind_data.monitor_tables;
	cache.ttl_seconds = bind_data.ttl_seconds;
	cache.has_ttl = bind_data.has_ttl;

	state.metadata_manager->CreateCache(cache);
	state.metadata_manager->InitializeState(bind_data.cache_name);
	bind_data.done = true;

	output.SetCardinality(1);
	output.SetValue(0, 0, Value("Cache created successfully"));
}

//===--------------------------------------------------------------------===//
// ducksync_refresh(cache_name, force)
//===--------------------------------------------------------------------===//
struct RefreshBindData : public TableFunctionData {
	std::string cache_name;
	bool force;
};

static unique_ptr<FunctionData> DuckSyncRefreshBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RefreshBindData>();

	if (input.inputs.empty()) {
		throw InvalidInputException("ducksync_refresh requires cache_name argument");
	}

	result->cache_name = input.inputs[0].GetValue<string>();
	result->force = false;

	// Handle optional force parameter
	for (auto &kv : input.named_parameters) {
		if (kv.first == "force") {
			result->force = kv.second.GetValue<bool>();
		}
	}

	names.emplace_back("result");
	names.emplace_back("message");
	names.emplace_back("rows_refreshed");
	names.emplace_back("duration_ms");
	return_types.emplace_back(LogicalType::VARCHAR);
	return_types.emplace_back(LogicalType::VARCHAR);
	return_types.emplace_back(LogicalType::BIGINT);
	return_types.emplace_back(LogicalType::DOUBLE);

	return std::move(result);
}

static void DuckSyncRefreshFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<RefreshBindData>();
	auto &state = GetDuckSyncState(context);

	if (!state.metadata_manager || !state.storage_manager) {
		throw InvalidInputException("DuckSync not initialized");
	}

	RefreshOrchestrator orchestrator(context, *state.metadata_manager, *state.storage_manager);
	auto status = orchestrator.Refresh(bind_data.cache_name, bind_data.force);

	output.SetCardinality(1);

	// Convert result to string
	std::string result_str;
	switch (status.result) {
	case RefreshResult::SKIPPED:
		result_str = "SKIPPED";
		break;
	case RefreshResult::REFRESHED:
		result_str = "REFRESHED";
		break;
	case RefreshResult::ERROR:
		result_str = "ERROR";
		break;
	}

	output.SetValue(0, 0, Value(result_str));
	output.SetValue(1, 0, Value(status.message));

	if (status.has_rows) {
		output.SetValue(2, 0, Value::BIGINT(status.rows_refreshed));
	} else {
		output.SetValue(2, 0, Value());
	}

	if (status.has_duration) {
		output.SetValue(3, 0, Value::DOUBLE(status.duration_ms));
	} else {
		output.SetValue(3, 0, Value());
	}
}

//===--------------------------------------------------------------------===//
// Extension Load - Using ExtensionLoader API
//===--------------------------------------------------------------------===//
static void LoadInternal(ExtensionLoader &loader) {
	// Register ducksync_setup_storage
	TableFunction setup_storage_func("ducksync_setup_storage", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                 DuckSyncSetupStorageFunction, DuckSyncSetupStorageBind);
	loader.RegisterFunction(setup_storage_func);

	// Register ducksync_add_source
	TableFunction add_source_func("ducksync_add_source",
	                              {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                              DuckSyncAddSourceFunction, DuckSyncAddSourceBind);
	add_source_func.named_parameters["passthrough_enabled"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(add_source_func);

	// Register ducksync_create_cache
	TableFunction create_cache_func("ducksync_create_cache",
	                                {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                                 LogicalType::LIST(LogicalType::VARCHAR), LogicalType::BIGINT},
	                                DuckSyncCreateCacheFunction, DuckSyncCreateCacheBind);
	loader.RegisterFunction(create_cache_func);

	// Register ducksync_refresh
	TableFunction refresh_func("ducksync_refresh", {LogicalType::VARCHAR}, DuckSyncRefreshFunction,
	                           DuckSyncRefreshBind);
	refresh_func.named_parameters["force"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(refresh_func);

	// Register replacement_scan hook for transparent routing
	auto &db = loader.GetDatabaseInstance();
	QueryRouter::Register(db);
}

void DucksyncExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string DucksyncExtension::Name() {
	return "ducksync";
}

std::string DucksyncExtension::Version() const {
#ifdef EXT_VERSION_DUCKSYNC
	return EXT_VERSION_DUCKSYNC;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(ducksync, loader) {
	duckdb::LoadInternal(loader);
}
}
