PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=ngram
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Run the deterministic preprocessing/checkpoint gap harness beside the
# SQLLogicTests on Linux and macOS. The harness uses POSIX process control and
# is not built on Windows, whose environment sets OS=Windows_NT for make.
ifneq ($(OS),Windows_NT)
test_release_internal: ngram_checkpoint_gap_release
test_debug_internal: ngram_checkpoint_gap_debug
test_reldebug_internal: ngram_checkpoint_gap_reldebug
endif

.PHONY: ngram_checkpoint_gap_release ngram_checkpoint_gap_debug ngram_checkpoint_gap_reldebug
ngram_checkpoint_gap_release:
	cmake --build build/release --config Release --target run_ngram_checkpoint_gap_test unittest
ngram_checkpoint_gap_debug:
	cmake --build build/debug --config Debug --target run_ngram_checkpoint_gap_test unittest
ngram_checkpoint_gap_reldebug:
	cmake --build build/reldebug --config RelWithDebInfo --target run_ngram_checkpoint_gap_test unittest
