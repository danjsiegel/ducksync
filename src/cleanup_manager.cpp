#include "cleanup_manager.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/exception.hpp"
#include <sstream>

namespace duckdb {

// Helper to create a connection from context
static Connection MakeConnection(ClientContext &context) {
	return Connection(*context.db);
}

CleanupManager::CleanupManager(ClientContext &context, DuckSyncStorageManager &storage_manager)
    : context_(context), storage_manager_(storage_manager) {
}

CleanupManager::~CleanupManager() {
}

CleanupResult CleanupManager::CleanupCache(const std::string &cache_name, const std::string &source_name) {
	CleanupResult result;
	result.snapshots_expired = 0;
	result.files_cleaned = 0;
	result.orphans_deleted = 0;

	try {
		// Run all cleanup operations
		result.snapshots_expired = ExpireSnapshots(cache_name, source_name);
		result.files_cleaned = CleanupOldFiles(cache_name, source_name);
		result.orphans_deleted = DeleteOrphanedFiles(cache_name, source_name);

		std::ostringstream msg;
		msg << "Cleanup completed: " << result.snapshots_expired << " snapshots expired, " << result.files_cleaned
		    << " old files cleaned, " << result.orphans_deleted << " orphaned files deleted";
		result.message = msg.str();

	} catch (const std::exception &e) {
		result.message = std::string("Cleanup error: ") + e.what();
	}

	return result;
}

CleanupResult CleanupManager::CleanupAll() {
	CleanupResult total_result;
	total_result.snapshots_expired = 0;
	total_result.files_cleaned = 0;
	total_result.orphans_deleted = 0;

	if (!storage_manager_.IsAttached()) {
		total_result.message = "DuckLake not attached, no cleanup performed";
		return total_result;
	}

	auto conn = MakeConnection(context_);
	std::string ducklake_name = storage_manager_.GetDuckLakeName();

	try {
		// Expire old snapshots globally
		std::ostringstream expire_sql;
		expire_sql << "CALL ducklake_expire_snapshots('" << ducklake_name << "', older_than => INTERVAL '1 day');";
		auto expire_result = conn.Query(expire_sql.str());
		if (!expire_result->HasError()) {
			total_result.snapshots_expired = expire_result->RowCount();
		}

		// Cleanup old files globally
		std::ostringstream cleanup_sql;
		cleanup_sql << "CALL ducklake_cleanup_old_files('" << ducklake_name << "', older_than => INTERVAL '7 days');";
		auto cleanup_result = conn.Query(cleanup_sql.str());
		if (!cleanup_result->HasError()) {
			total_result.files_cleaned = cleanup_result->RowCount();
		}

		// Delete orphaned files globally
		std::ostringstream orphan_sql;
		orphan_sql << "CALL ducklake_delete_orphaned_files('" << ducklake_name << "');";
		auto orphan_result = conn.Query(orphan_sql.str());
		if (!orphan_result->HasError()) {
			total_result.orphans_deleted = orphan_result->RowCount();
		}

		std::ostringstream msg;
		msg << "Global cleanup completed: " << total_result.snapshots_expired << " snapshots expired, "
		    << total_result.files_cleaned << " old files cleaned, " << total_result.orphans_deleted
		    << " orphaned files deleted";
		total_result.message = msg.str();

	} catch (const std::exception &e) {
		total_result.message = std::string("Global cleanup error: ") + e.what();
	}

	return total_result;
}

int64_t CleanupManager::ExpireSnapshots(const std::string &cache_name, const std::string &source_name) {
	if (!storage_manager_.IsAttached()) {
		return 0;
	}

	auto conn = MakeConnection(context_);

	// Get the table name
	std::string table_name = storage_manager_.GetDuckLakeTableName(cache_name, source_name);

	// Call DuckLake expire snapshots for this table
	std::ostringstream sql;
	sql << "CALL ducklake_expire_snapshots('" << table_name << "', "
	    << "older_than => INTERVAL '1 day');";

	auto result = conn.Query(sql.str());
	if (result->HasError()) {
		// DuckLake cleanup functions may not exist or table may not have snapshots
		// This is not a fatal error
		return 0;
	}

	return result->RowCount();
}

int64_t CleanupManager::CleanupOldFiles(const std::string &cache_name, const std::string &source_name,
                                        int64_t older_than_days) {
	if (!storage_manager_.IsAttached()) {
		return 0;
	}

	auto conn = MakeConnection(context_);

	// Get the table name
	std::string table_name = storage_manager_.GetDuckLakeTableName(cache_name, source_name);

	// Call DuckLake cleanup old files for this table
	std::ostringstream sql;
	sql << "CALL ducklake_cleanup_old_files('" << table_name << "', "
	    << "older_than => INTERVAL '" << older_than_days << " days');";

	auto result = conn.Query(sql.str());
	if (result->HasError()) {
		// Not a fatal error if cleanup function doesn't exist
		return 0;
	}

	return result->RowCount();
}

int64_t CleanupManager::DeleteOrphanedFiles(const std::string &cache_name, const std::string &source_name) {
	if (!storage_manager_.IsAttached()) {
		return 0;
	}

	auto conn = MakeConnection(context_);

	// Get the table name
	std::string table_name = storage_manager_.GetDuckLakeTableName(cache_name, source_name);

	// Call DuckLake delete orphaned files for this table
	std::ostringstream sql;
	sql << "CALL ducklake_delete_orphaned_files('" << table_name << "');";

	auto result = conn.Query(sql.str());
	if (result->HasError()) {
		// Not a fatal error if cleanup function doesn't exist
		return 0;
	}

	return result->RowCount();
}

} // namespace duckdb
