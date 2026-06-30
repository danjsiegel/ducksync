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
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

#include <sstream>
#include <algorithm>
#include <cctype>
#include <unordered_set>
#include <iostream>

namespace duckdb {

//===--------------------------------------------------------------------===//
// ducksync_setup_storage(pg_connection_string, data_path)
// - pg_connection_string: PostgreSQL connection for DuckLake catalog
// - data_path: S3 or local path for parquet file storage
//===--------------------------------------------------------------------===//
struct SetupStorageBindData : public TableFunctionData {
	std::string pg_connection_string;
	std::string data_path;
	std::string schema_name = "ducksync"; // optional 3rd arg; defaults to "ducksync"
	bool done = false;
};

static unique_ptr<FunctionData> DuckSyncSetupStorageBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<SetupStorageBindData>();

	if (input.inputs.size() < 2) {
		throw InvalidInputException(
		    "ducksync_setup_storage requires at least 2 arguments: pg_connection_string, data_path[, schema_name]");
	}

	result->pg_connection_string = input.inputs[0].GetValue<string>();
	result->data_path = input.inputs[1].GetValue<string>();

	// Optional 3rd argument: schema_name (default "ducksync")
	if (input.inputs.size() >= 3) {
		result->schema_name = input.inputs[2].GetValue<string>();
	}

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
	state.metadata_manager->Initialize(state.storage_manager->GetDuckLakeName(), bind_data.schema_name);

	state.initialized = true;
	bind_data.done = true;

	output.SetCardinality(1);
	output.SetValue(0, 0, Value("DuckSync storage configured successfully"));
}

//===--------------------------------------------------------------------===//
// ducksync_init(catalog_name[, schema_name])
// - Use an existing DuckLake catalog for DuckSync storage
// - schema_name defaults to "ducksync" for backward compatibility
//===--------------------------------------------------------------------===//
struct InitBindData : public TableFunctionData {
	std::string catalog_name;
	std::string schema_name = "ducksync"; // optional 2nd arg; defaults to "ducksync"
	bool done = false;
};

static unique_ptr<FunctionData> DuckSyncInitBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<InitBindData>();

	if (input.inputs.empty()) {
		throw InvalidInputException("ducksync_init requires at least 1 argument: catalog_name[, schema_name]");
	}

	result->catalog_name = input.inputs[0].GetValue<string>();

	// Optional 2nd argument: schema_name (default "ducksync")
	if (input.inputs.size() >= 2) {
		result->schema_name = input.inputs[1].GetValue<string>();
	}

	names.emplace_back("status");
	return_types.emplace_back(LogicalType::VARCHAR);

	return std::move(result);
}

static void DuckSyncInitFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<InitBindData>();

	if (bind_data.done) {
		output.SetCardinality(0);
		return;
	}

	// Use existing DuckLake catalog
	auto &state = GetDuckSyncState(context);
	if (!state.storage_manager) {
		state.storage_manager = make_uniq<DuckSyncStorageManager>(context);
	}
	state.storage_manager->UseExistingCatalog(bind_data.catalog_name);

	// Initialize metadata manager (creates metadata schema and tables)
	if (!state.metadata_manager) {
		state.metadata_manager = make_uniq<DuckSyncMetadataManager>(context);
	}
	state.metadata_manager->Initialize(bind_data.catalog_name, bind_data.schema_name);

	state.initialized = true;
	bind_data.done = true;

	output.SetCardinality(1);
	output.SetValue(0, 0,
	                Value("DuckSync initialized with catalog '" + bind_data.catalog_name + "'" +
	                      (bind_data.schema_name != "ducksync" ? " (schema: '" + bind_data.schema_name + "')" : "")));
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
		throw InvalidInputException("DuckSync not initialized. Call ducksync_init or ducksync_setup_storage first.");
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
	std::string invalidation_mode = "last_altered";
	std::string metadata_secret_name;
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
	if (input.inputs.size() > 5) {
		throw InvalidInputException("ducksync_create_cache accepts at most 5 positional arguments");
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

	for (auto &kv : input.named_parameters) {
		if (kv.first == "invalidation_mode") {
			result->invalidation_mode = kv.second.GetValue<string>();
		} else if (kv.first == "metadata_secret") {
			result->metadata_secret_name = kv.second.GetValue<string>();
		}
	}

	if (result->invalidation_mode != "last_altered" && result->invalidation_mode != "two_stage" &&
	    result->invalidation_mode != "ttl_only" && result->invalidation_mode != "manual") {
		throw InvalidInputException("invalidation_mode must be one of: last_altered, two_stage, ttl_only, manual");
	}
	if (result->invalidation_mode == "two_stage" && result->metadata_secret_name.empty()) {
		throw InvalidInputException(
		    "ducksync_create_cache with invalidation_mode='two_stage' requires metadata_secret");
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
		throw InvalidInputException("DuckSync not initialized. Call ducksync_init or ducksync_setup_storage first.");
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
	cache.invalidation_mode = bind_data.invalidation_mode;
	cache.metadata_secret_name = bind_data.metadata_secret_name;

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
	bool done = false;
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
	auto &bind_data = data_p.bind_data->CastNoConst<RefreshBindData>();
	auto &state = GetDuckSyncState(context);

	if (bind_data.done) {
		output.SetCardinality(0);
		return;
	}

	if (!state.metadata_manager || !state.storage_manager) {
		throw InvalidInputException("DuckSync not initialized");
	}

	RefreshOrchestrator orchestrator(context, *state.metadata_manager, *state.storage_manager);
	auto status = orchestrator.Refresh(bind_data.cache_name, bind_data.force);

	bind_data.done = true;
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
// Table Extraction and AST Rewriting using DuckDB Parser
//===--------------------------------------------------------------------===//

// Rewrite info: catalog, schema, table_name
struct TableRewrite {
	std::string catalog;
	std::string schema;
	std::string table_name;
};

// Forward declarations
static void ExtractTablesFromTableRef(TableRef &ref, std::unordered_set<std::string> &tables);
static void RewriteTablesInTableRef(TableRef &ref, const std::unordered_map<std::string, TableRewrite> &rewrites);

// Helper to build fully qualified table name for matching
static std::string BuildFullTableName(const BaseTableRef &base) {
	std::string full_name;
	if (!base.catalog_name.empty()) {
		full_name += base.catalog_name + ".";
	}
	if (!base.schema_name.empty()) {
		full_name += base.schema_name + ".";
	}
	full_name += base.table_name;
	return full_name;
}

// Helper to uppercase a string for case-insensitive comparison
static std::string ToUpper(const std::string &str) {
	std::string upper = str;
	std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
	return upper;
}

static void ExtractTablesFromQueryNode(QueryNode &node, std::unordered_set<std::string> &tables) {
	if (node.type == QueryNodeType::SELECT_NODE) {
		auto &select = node.Cast<SelectNode>();
		if (select.from_table) {
			ExtractTablesFromTableRef(*select.from_table, tables);
		}
	} else if (node.type == QueryNodeType::SET_OPERATION_NODE) {
		// Handle UNION, INTERSECT, EXCEPT recursively
		auto &setop = node.Cast<SetOperationNode>();
		for (auto &child : setop.children) {
			if (child) {
				ExtractTablesFromQueryNode(*child, tables);
			}
		}
	}
}

static void ExtractTablesFromTableRef(TableRef &ref, std::unordered_set<std::string> &tables) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE: {
		auto &base = ref.Cast<BaseTableRef>();
		tables.insert(BuildFullTableName(base));
		break;
	}
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		ExtractTablesFromTableRef(*join.left, tables);
		ExtractTablesFromTableRef(*join.right, tables);
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &subquery = ref.Cast<SubqueryRef>();
		ExtractTablesFromQueryNode(*subquery.subquery->node, tables);
		break;
	}
	default:
		break;
	}
}

// Rewrite table references in the AST (modifies in place)
static void RewriteTablesInQueryNode(QueryNode &node, const std::unordered_map<std::string, TableRewrite> &rewrites) {
	if (node.type == QueryNodeType::SELECT_NODE) {
		auto &select = node.Cast<SelectNode>();
		if (select.from_table) {
			RewriteTablesInTableRef(*select.from_table, rewrites);
		}
	} else if (node.type == QueryNodeType::SET_OPERATION_NODE) {
		// Handle UNION, INTERSECT, EXCEPT recursively
		auto &setop = node.Cast<SetOperationNode>();
		for (auto &child : setop.children) {
			if (child) {
				RewriteTablesInQueryNode(*child, rewrites);
			}
		}
	}
}

static void RewriteTablesInTableRef(TableRef &ref, const std::unordered_map<std::string, TableRewrite> &rewrites) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE: {
		auto &base = ref.Cast<BaseTableRef>();
		std::string full_name = ToUpper(BuildFullTableName(base));

		// Check if this table should be rewritten (case-insensitive)
		auto it = rewrites.find(full_name);
		if (it != rewrites.end()) {
			// Rewrite to DuckLake table: catalog.schema.table_name
			base.catalog_name = it->second.catalog;
			base.schema_name = it->second.schema;
			base.table_name = it->second.table_name;
		}
		break;
	}
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		RewriteTablesInTableRef(*join.left, rewrites);
		RewriteTablesInTableRef(*join.right, rewrites);
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &subquery = ref.Cast<SubqueryRef>();
		RewriteTablesInQueryNode(*subquery.subquery->node, rewrites);
		break;
	}
	default:
		break;
	}
}

// Extract all table references from a SQL query using DuckDB's parser
static std::vector<std::string> ExtractTableReferences(const std::string &sql) {
	std::unordered_set<std::string> tables;

	try {
		Parser parser;
		parser.ParseQuery(sql);

		for (auto &stmt : parser.statements) {
			if (stmt->type == StatementType::SELECT_STATEMENT) {
				auto &select = stmt->Cast<SelectStatement>();
				ExtractTablesFromQueryNode(*select.node, tables);
			}
		}
	} catch (const std::exception &e) {
		// Parse failed - query will passthrough to Snowflake
		std::cerr << "[DuckSync] Warning: Failed to parse SQL for table extraction: " << e.what() << std::endl;
	} catch (...) {
		// Unknown parse error - query will passthrough to Snowflake
		std::cerr << "[DuckSync] Warning: Unknown error parsing SQL for table extraction" << std::endl;
	}

	return std::vector<std::string>(tables.begin(), tables.end());
}

// Rewrite SQL query by modifying the AST and regenerating SQL
static std::string RewriteQueryWithAST(const std::string &sql,
                                       const std::unordered_map<std::string, TableRewrite> &rewrites) {
	try {
		Parser parser;
		parser.ParseQuery(sql);

		for (auto &stmt : parser.statements) {
			if (stmt->type == StatementType::SELECT_STATEMENT) {
				auto &select = stmt->Cast<SelectStatement>();
				RewriteTablesInQueryNode(*select.node, rewrites);
			}
		}

		// Regenerate SQL from modified AST
		if (!parser.statements.empty()) {
			return parser.statements[0]->ToString();
		}
	} catch (const std::exception &e) {
		// Rewrite failed - use original SQL (will passthrough to Snowflake)
		std::cerr << "[DuckSync] Warning: Failed to rewrite SQL: " << e.what() << std::endl;
	} catch (...) {
		// Unknown rewrite error - use original SQL
		std::cerr << "[DuckSync] Warning: Unknown error rewriting SQL" << std::endl;
	}
	return sql;
}

//===--------------------------------------------------------------------===//
// Query-backed DuckSync table functions
//===--------------------------------------------------------------------===//
struct DuckSyncStreamingQueryGlobalState : public GlobalTableFunctionState {
	unique_ptr<QueryResult> result;
	unique_ptr<DataChunk> current_chunk; // Keep chunk alive while output references it
	idx_t current_row = 0;
	bool finished = false;
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

static void InstallAndLoadExtension(Connection &conn, const std::string &extension_name) {
	auto install_result = conn.Query("INSTALL " + extension_name + ";");
	if (install_result->HasError()) {
		throw IOException("Failed to INSTALL " + extension_name + ": " + install_result->GetError());
	}

	auto load_result = conn.Query("LOAD " + extension_name + ";");
	if (load_result->HasError()) {
		throw IOException("Failed to LOAD " + extension_name + ": " + load_result->GetError());
	}
}

static unique_ptr<GlobalTableFunctionState> InitStreamingQueryGlobalState(ClientContext &context,
                                                                          const std::string &execution_query) {
	auto state = make_uniq<DuckSyncStreamingQueryGlobalState>();
	auto conn = Connection(*context.db);
	state->result = conn.Query(execution_query);

	if (state->result->HasError()) {
		throw IOException("Query execution failed: " + state->result->GetError());
	}

	return std::move(state);
}

static void StreamQueryResultToOutput(GlobalTableFunctionState &global_state, DataChunk &output) {
	auto &gstate = global_state.Cast<DuckSyncStreamingQueryGlobalState>();

	if (gstate.finished || !gstate.result) {
		output.SetCardinality(0);
		return;
	}

	auto &result = *gstate.result;
	gstate.current_chunk = result.Fetch();
	if (!gstate.current_chunk || gstate.current_chunk->size() == 0) {
		gstate.finished = true;
		output.SetCardinality(0);
		return;
	}

	output.SetCardinality(gstate.current_chunk->size());
	for (idx_t col = 0; col < gstate.current_chunk->ColumnCount(); col++) {
		output.data[col].Reference(gstate.current_chunk->data[col]);
	}
}

//===--------------------------------------------------------------------===//
// ducksync_serve(listen_uri)
// Explicitly starts a Quack server for the current DuckSync instance
//===--------------------------------------------------------------------===//
struct DuckSyncServeBindData : public TableFunctionData {
	std::string execution_query;
};

static unique_ptr<FunctionData> DuckSyncServeBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DuckSyncServeBindData>();

	if (input.inputs.empty()) {
		throw InvalidInputException("ducksync_serve requires at least 1 argument: listen_uri[, token := ..., "
		                            "allow_other_hostname := ..., disable_ssl := ...]");
	}

	auto &state = GetDuckSyncState(context);
	if (!state.initialized || !state.storage_manager || !state.metadata_manager) {
		throw InvalidInputException("DuckSync not initialized. Call ducksync_init or ducksync_setup_storage first.");
	}

	if (!state.storage_manager->IsPostgresBacked()) {
		std::cerr << "[DuckSync] Warning: ducksync_serve is running without a PostgreSQL-backed DuckLake control plane."
		          << std::endl;
	}

	std::ostringstream sql;
	sql << "SELECT * FROM quack_serve('" << EscapeSqlStringLiteral(input.inputs[0].GetValue<string>()) << "'";

	auto token_entry = input.named_parameters.find("token");
	if (token_entry != input.named_parameters.end()) {
		sql << ", token := '" << EscapeSqlStringLiteral(token_entry->second.GetValue<string>()) << "'";
	}

	auto allow_other_entry = input.named_parameters.find("allow_other_hostname");
	if (allow_other_entry != input.named_parameters.end()) {
		sql << ", allow_other_hostname := " << (allow_other_entry->second.GetValue<bool>() ? "true" : "false");
	}

	auto disable_ssl_entry = input.named_parameters.find("disable_ssl");
	if (disable_ssl_entry != input.named_parameters.end()) {
		sql << ", disable_ssl := " << (disable_ssl_entry->second.GetValue<bool>() ? "true" : "false");
	}

	sql << ")";
	result->execution_query = sql.str();

	auto conn = Connection(*context.db);
	InstallAndLoadExtension(conn, "quack");

	auto prepared = conn.Prepare(result->execution_query);
	if (prepared->HasError()) {
		throw IOException("ducksync_serve bind failed: " + prepared->GetError());
	}

	auto &prep_types = prepared->GetTypes();
	auto &prep_names = prepared->GetNames();
	for (idx_t i = 0; i < prep_types.size(); i++) {
		return_types.push_back(prep_types[i]);
		names.push_back(prep_names[i]);
	}

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DuckSyncServeInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<DuckSyncServeBindData>();
	return InitStreamingQueryGlobalState(context, bind_data.execution_query);
}

static void DuckSyncServeFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	StreamQueryResultToOutput(*data_p.global_state, output);
}

//===--------------------------------------------------------------------===//
// ducksync_stop(listen_uri)
// Explicitly stops a Quack server for the current DuckSync instance
//===--------------------------------------------------------------------===//
struct DuckSyncStopBindData : public TableFunctionData {
	std::string listen_uri;
	bool done = false;
};

static unique_ptr<FunctionData> DuckSyncStopBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DuckSyncStopBindData>();

	if (input.inputs.empty()) {
		throw InvalidInputException("ducksync_stop requires 1 argument: listen_uri");
	}

	auto &state = GetDuckSyncState(context);
	if (!state.initialized || !state.storage_manager || !state.metadata_manager) {
		throw InvalidInputException("DuckSync not initialized. Call ducksync_init or ducksync_setup_storage first.");
	}

	result->listen_uri = input.inputs[0].GetValue<string>();
	names.emplace_back("status");
	return_types.emplace_back(LogicalType::VARCHAR);
	return std::move(result);
}

static void DuckSyncStopFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<DuckSyncStopBindData>();
	if (bind_data.done) {
		output.SetCardinality(0);
		return;
	}

	auto conn = Connection(*context.db);
	InstallAndLoadExtension(conn, "quack");

	auto stop_result = conn.Query("CALL quack_stop('" + EscapeSqlStringLiteral(bind_data.listen_uri) + "')");
	if (stop_result->HasError()) {
		throw IOException("ducksync_stop failed: " + stop_result->GetError());
	}

	std::string status = "Quack stop completed for " + bind_data.listen_uri;
	if (stop_result->RowCount() > 0 && stop_result->ColumnCount() > 0) {
		status = stop_result->GetValue(0, 0).ToString();
	}

	output.SetCardinality(1);
	output.SetValue(0, 0, Value(status));
	bind_data.done = true;
}

//===--------------------------------------------------------------------===//
// ducksync_query(sql, source_name)
// Smart query routing: cache if all tables cached, passthrough otherwise
// Returns actual query data, not status messages
//===--------------------------------------------------------------------===//

struct DuckSyncQueryBindData : public TableFunctionData {
	std::string sql_query;
	std::string source_name;
	std::string execution_query; // The actual query to run (rewritten or passthrough)
	bool use_cache = false;      // Whether we're using cache or passthrough
	vector<LogicalType> result_types;
	vector<string> result_names;
};

static unique_ptr<FunctionData> DuckSyncQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DuckSyncQueryBindData>();

	if (input.inputs.size() < 2) {
		throw InvalidInputException("ducksync_query requires 2 arguments: sql_query, source_name");
	}

	result->sql_query = input.inputs[0].GetValue<string>();
	result->source_name = input.inputs[1].GetValue<string>();

	auto &state = GetDuckSyncState(context);
	if (!state.metadata_manager || !state.storage_manager) {
		throw InvalidInputException("DuckSync not initialized. Call ducksync_init or ducksync_setup_storage first.");
	}

	// Get the source configuration
	SourceDefinition source;
	if (!state.metadata_manager->GetSource(result->source_name, source)) {
		throw InvalidInputException("Source '" + result->source_name + "' not found");
	}

	// Extract table references using DuckDB parser
	auto tables = ExtractTableReferences(result->sql_query);

	// Check cache coverage and TTL validity
	// Map: UPPER(original_table) -> TableRewrite for AST rewrite
	std::unordered_map<std::string, TableRewrite> rewrites;
	std::vector<CacheDefinition> caches_to_refresh;
	bool all_cached = !tables.empty();

	for (auto &table : tables) {
		CacheDefinition cache;
		bool found = false;

		// First check if it's a cache name directly
		if (state.metadata_manager->GetCache(table, cache)) {
			found = true;
		}
		// Then check if it's a monitored table
		else if (state.metadata_manager->GetCacheByMonitorTable(table, cache)) {
			found = true;
		}

		if (found) {
			// Check TTL - if expired, mark for refresh
			CacheState cache_state;
			bool needs_refresh = false;

			if (!state.metadata_manager->GetState(cache.cache_name, cache_state)) {
				// Never refreshed
				needs_refresh = true;
			} else if (cache.has_ttl && cache_state.HasExpiresAt()) {
				// Check if TTL expired by comparing timestamps
				// Get current timestamp from DuckDB and compare with expires_at
				auto conn = Connection(*context.db);
				auto now_result = conn.Query("SELECT CURRENT_TIMESTAMP::VARCHAR;");
				if (!now_result->HasError() && now_result->RowCount() > 0) {
					auto now_str = now_result->GetValue(0, 0).ToString();
					// Simple string comparison works for ISO-format timestamps
					needs_refresh = (cache_state.expires_at < now_str);
				}
			}

			if (needs_refresh) {
				caches_to_refresh.push_back(cache);
			}

			// Store rewrite info for AST modification
			// DuckLake tables are: {catalog}.{source_name}.{cache_name}
			TableRewrite rewrite;
			rewrite.catalog = state.storage_manager->GetDuckLakeName();
			rewrite.schema = cache.source_name;
			rewrite.table_name = cache.cache_name;
			rewrites[ToUpper(table)] = rewrite;
		} else {
			all_cached = false;
			break;
		}
	}

	// Refresh any expired caches before executing query
	if (!caches_to_refresh.empty()) {
		RefreshOrchestrator orchestrator(context, *state.metadata_manager, *state.storage_manager);
		for (auto &cache : caches_to_refresh) {
			orchestrator.Refresh(cache.cache_name, false); // smart refresh
		}
	}

	// Determine execution strategy
	if (all_cached && !rewrites.empty()) {
		// Rewrite query using AST modification (safe - only modifies table references)
		result->use_cache = true;
		result->execution_query = RewriteQueryWithAST(result->sql_query, rewrites);
	} else {
		// Pass through to Snowflake
		result->use_cache = false;
		result->execution_query = "SELECT * FROM snowflake_query('" + EscapeSqlStringLiteral(result->sql_query) +
		                          "', '" + EscapeSqlStringLiteral(source.secret_name) + "')";
	}

	// Use Prepare() to discover schema without executing the query
	// This avoids double-execution (bind + init_global) which would
	// hit Snowflake twice for passthrough queries
	auto conn = Connection(*context.db);
	auto prepared = conn.Prepare(result->execution_query);

	if (prepared->HasError()) {
		throw IOException("Query failed: " + prepared->GetError());
	}

	// Get schema from prepared statement
	auto &prep_types = prepared->GetTypes();
	auto &prep_names = prepared->GetNames();
	for (idx_t i = 0; i < prep_types.size(); i++) {
		return_types.push_back(prep_types[i]);
		names.push_back(prep_names[i]);
		result->result_types.push_back(prep_types[i]);
		result->result_names.push_back(prep_names[i]);
	}

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DuckSyncQueryInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<DuckSyncQueryBindData>();
	return InitStreamingQueryGlobalState(context, bind_data.execution_query);
}

static void DuckSyncQueryFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	StreamQueryResultToOutput(*data_p.global_state, output);
}

//===--------------------------------------------------------------------===//
// Extension Load - Using ExtensionLoader API
//===--------------------------------------------------------------------===//
static void LoadInternal(ExtensionLoader &loader) {
	// Register ducksync_init (use existing DuckLake catalog)
	// 1-arg: ducksync_init(catalog_name) - backward compatible, schema defaults to "ducksync"
	TableFunction init_func_1("ducksync_init", {LogicalType::VARCHAR}, DuckSyncInitFunction, DuckSyncInitBind);
	loader.RegisterFunction(init_func_1);
	// 2-arg: ducksync_init(catalog_name, schema_name) - custom metadata schema (e.g. for GizmoSQL)
	TableFunction init_func_2("ducksync_init", {LogicalType::VARCHAR, LogicalType::VARCHAR}, DuckSyncInitFunction,
	                          DuckSyncInitBind);
	loader.RegisterFunction(init_func_2);

	// Register ducksync_setup_storage (full setup - attaches DuckLake)
	// 2-arg: ducksync_setup_storage(pg_conn, data_path) - backward compatible, schema defaults to "ducksync"
	TableFunction setup_storage_func_2("ducksync_setup_storage", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                   DuckSyncSetupStorageFunction, DuckSyncSetupStorageBind);
	loader.RegisterFunction(setup_storage_func_2);
	// 3-arg: ducksync_setup_storage(pg_conn, data_path, schema_name) - custom metadata schema
	TableFunction setup_storage_func_3("ducksync_setup_storage",
	                                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                   DuckSyncSetupStorageFunction, DuckSyncSetupStorageBind);
	loader.RegisterFunction(setup_storage_func_3);

	// Register ducksync_add_source
	TableFunction add_source_func("ducksync_add_source",
	                              {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                              DuckSyncAddSourceFunction, DuckSyncAddSourceBind);
	add_source_func.named_parameters["passthrough_enabled"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(add_source_func);

	// Register ducksync_create_cache
	TableFunction create_cache_func(
	    "ducksync_create_cache",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)},
	    DuckSyncCreateCacheFunction, DuckSyncCreateCacheBind);
	create_cache_func.varargs = LogicalType::BIGINT;
	create_cache_func.named_parameters["invalidation_mode"] = LogicalType::VARCHAR;
	create_cache_func.named_parameters["metadata_secret"] = LogicalType::VARCHAR;
	loader.RegisterFunction(create_cache_func);

	// Register ducksync_refresh
	TableFunction refresh_func("ducksync_refresh", {LogicalType::VARCHAR}, DuckSyncRefreshFunction,
	                           DuckSyncRefreshBind);
	refresh_func.named_parameters["force"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(refresh_func);

	// Register ducksync_query (new smart routing function)
	TableFunction query_func("ducksync_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, DuckSyncQueryFunction,
	                         DuckSyncQueryBind, DuckSyncQueryInitGlobal);
	loader.RegisterFunction(query_func);

	// Register ducksync_serve
	TableFunction serve_func("ducksync_serve", {LogicalType::VARCHAR}, DuckSyncServeFunction, DuckSyncServeBind,
	                         DuckSyncServeInitGlobal);
	serve_func.named_parameters["token"] = LogicalType::VARCHAR;
	serve_func.named_parameters["allow_other_hostname"] = LogicalType::BOOLEAN;
	serve_func.named_parameters["disable_ssl"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(serve_func);

	// Register ducksync_stop
	TableFunction stop_func("ducksync_stop", {LogicalType::VARCHAR}, DuckSyncStopFunction, DuckSyncStopBind);
	loader.RegisterFunction(stop_func);

	// Register replacement_scan hook
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
