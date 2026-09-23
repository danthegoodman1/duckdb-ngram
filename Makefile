PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=ngram
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# DuckDB 2.0's CMake derives no release version from git: a build reports
# v2.0.0-dev<n> unless it names one, and the rowid guard accepts only the pinned
# release. DUCKDB_EXPLICIT_VERSION also outranks the DUCKDB_VERSION environment
# variable, which the distribution workflow sets to the submodule's commit.
EXT_FLAGS += -DDUCKDB_EXPLICIT_VERSION=v2.0.0

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Run the deterministic preprocessing/checkpoint gap harness beside the
# SQLLogicTests on Linux and macOS. The harness uses POSIX process control and
# is not built on Windows, whose environment sets OS=Windows_NT for make, and it
# follows SKIP_TESTS, which the distribution workflow sets for the pass it makes
# outside its build container. The submodule's makefile runs the tests in
# test_<type>_internal; the newer one the distribution workflow checks out runs
# them in test_<type>.
ifneq ($(OS),Windows_NT)
ifneq ($(SKIP_TESTS),1)
test_release test_release_internal: ngram_checkpoint_gap_release
test_debug test_debug_internal: ngram_checkpoint_gap_debug
test_reldebug test_reldebug_internal: ngram_checkpoint_gap_reldebug
endif
endif

.PHONY: ngram_checkpoint_gap_release ngram_checkpoint_gap_debug ngram_checkpoint_gap_reldebug
ngram_checkpoint_gap_release:
	cmake --build build/release --config Release --target run_ngram_checkpoint_gap_test unittest
ngram_checkpoint_gap_debug:
	cmake --build build/debug --config Debug --target run_ngram_checkpoint_gap_test unittest
ngram_checkpoint_gap_reldebug:
	cmake --build build/reldebug --config RelWithDebInfo --target run_ngram_checkpoint_gap_test unittest
