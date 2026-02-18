PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=ducksync
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

.PHONY: test integration-test test-docker-up test-docker-down clean-test-data clean-all update-version test-compat

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

# Update DuckDB version - run: make update-version VERSION=v1.5.0
# Automatically updates submodules, CI workflow, and README.
update-version:
ifndef VERSION
	$(error VERSION is required. Usage: make update-version VERSION=v1.5.0)
endif
	@echo "Updating DuckDB to $(VERSION)..."
	@echo ""
	@echo "Step 1: Updating submodules..."
	cd $(PROJ_DIR)duckdb && git fetch --tags && git checkout $(VERSION)
	cd $(PROJ_DIR)extension-ci-tools && git fetch --tags && git checkout $(VERSION) || echo "Note: ci-tools may use different tag"
	@echo ""
	@echo "Step 2: Updating CI workflow..."
	@sed -i '' 's/@v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/@$(VERSION)/g' $(PROJ_DIR).github/workflows/MainDistributionPipeline.yml
	@sed -i '' 's/duckdb_version: v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/duckdb_version: $(VERSION)/g' $(PROJ_DIR).github/workflows/MainDistributionPipeline.yml
	@sed -i '' 's/ci_tools_version: v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/ci_tools_version: $(VERSION)/g' $(PROJ_DIR).github/workflows/MainDistributionPipeline.yml
	@echo "Step 3: Updating README..."
	@sed -i '' 's/DuckDB v[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*/DuckDB $(VERSION)/g' $(PROJ_DIR)README.md
	@echo ""
	@echo "Done! Next steps:"
	@echo "  make clean-all && make release && make test"
	@echo "  git checkout -b update-duckdb-$(VERSION)"
	@echo "  git add -A && git commit -m 'chore: update DuckDB to $(VERSION)'"
	@echo "  git push -u origin update-duckdb-$(VERSION)"

# Test compatibility against a specific DuckDB branch or tag without requiring Snowflake.
# Usage: make test-compat BRANCH=origin/v1.5-variegata
#        make test-compat BRANCH=v1.5.0
test-compat:
ifndef BRANCH
	$(error BRANCH is required. Usage: make test-compat BRANCH=origin/v1.5-variegata)
endif
	@echo "Testing compatibility with DuckDB branch: $(BRANCH)"
	@echo "Step 1: Checking out DuckDB submodule to $(BRANCH)..."
	@cd $(PROJ_DIR)duckdb && git fetch && git checkout $(BRANCH)
	@echo "Step 2: Cleaning build artifacts..."
	@$(MAKE) clean-all
	@echo "Step 3: Building extension..."
	@$(MAKE) release
	@echo ""
	@echo "Build succeeded for $(BRANCH)."
	@echo "To run unit tests (no Snowflake required):"
	@echo "  $(PROJ_DIR)test/run_tests.sh --unit-only"
	@echo ""
	@echo "NOTE: Restore submodule to stable version when done:"
	@echo "  cd duckdb && git checkout v1.4.4"
