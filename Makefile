PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=ducksync
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

.PHONY: test integration-test test-docker-up test-docker-down

test: release test-docker-up
	@chmod +x $(PROJ_DIR)test/run_tests.sh
	@$(PROJ_DIR)test/run_tests.sh

integration-test: test

test-docker-up:
	@cd $(PROJ_DIR)test && docker compose up -d postgres
	@echo "Waiting for PostgreSQL..."
	@sleep 3
	@echo "PostgreSQL running on localhost:5432"

test-docker-down:
	@cd $(PROJ_DIR)test && docker compose down -v
