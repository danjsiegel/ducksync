PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=ducksync
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

.PHONY: test integration-test test-docker-up test-docker-down clean-test-data clean-all

test: release test-docker-up
	@chmod +x $(PROJ_DIR)test/run_tests.sh
	@$(PROJ_DIR)test/run_tests.sh
	@$(MAKE) clean-test-data

integration-test: test

test-docker-up:
	@cd $(PROJ_DIR)test && docker compose up -d postgres
	@echo "Waiting for PostgreSQL..."
	@sleep 3
	@echo "PostgreSQL running on localhost:5432"

test-docker-down:
	@cd $(PROJ_DIR)test && docker compose down -v

# Clean up test data (local parquet files)
clean-test-data:
	@echo "Cleaning up test data..."
	@rm -rf $(PROJ_DIR)test/data
	@rm -rf $(PROJ_DIR)duckdb_unittest_tempdir
	@rm -f $(PROJ_DIR)test/*.wal $(PROJ_DIR)test/*.db
	@echo "Test data cleaned"

# Full cleanup: Docker volumes + test data + build
clean-all: test-docker-down clean-test-data
	@echo "Cleaning build artifacts..."
	@rm -rf $(PROJ_DIR)build
	@echo "Full cleanup complete"

# Reset test environment (fresh start)
reset-test: test-docker-down clean-test-data test-docker-up
	@echo "Test environment reset complete"

# Update DuckDB version - run: make update-version VERSION=v1.4.5
.PHONY: update-version
update-version:
ifndef VERSION
	$(error VERSION is required. Usage: make update-version VERSION=v1.4.5)
endif
	@echo "Updating DuckDB to $(VERSION)..."
	@echo ""
	@echo "Step 1: Updating submodules..."
	cd $(PROJ_DIR)duckdb && git fetch --tags && git checkout $(VERSION)
	cd $(PROJ_DIR)extension-ci-tools && git fetch --tags && git checkout $(VERSION) || echo "Note: ci-tools may use different tag"
	@echo ""
	@echo "Step 2: Update these files manually:"
	@echo "  - .github/workflows/MainDistributionPipeline.yml"
	@echo "    Change duckdb_version and ci_tools_version to $(VERSION)"
	@echo "  - README.md"
	@echo "    Update version references"
	@echo ""
	@echo "Step 3: Test the build:"
	@echo "  make clean-all && make release && make test"
	@echo ""
	@echo "Step 4: Create PR:"
	@echo "  git checkout -b update-duckdb-$(VERSION)"
	@echo "  git add -A"
	@echo "  git commit -m 'chore: update DuckDB to $(VERSION)'"
	@echo "  git push -u origin update-duckdb-$(VERSION)"
	@echo ""
	@echo "Done! Submodules updated to $(VERSION)"
