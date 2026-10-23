# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(ngram
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)
# Link it into the shell and unittest, which the SQL tests and the release
# evidence tool run; DuckDB 2.0 links only the extensions a config names here.
duckdb_extension_statically_link(ngram)

# The DuckLake tests (test/sql/lake) need DuckLake and, for catalogs shared
# between processes, the SQLite scanner. A build configured with
# NGRAM_DUCKLAKE=1 in the environment links both, at the commits DuckDB v2.0.0
# pins; every other build skips those tests. DuckLake needs CRoaring, which
# scripts/build_croaring.sh installs under build/deps.
if("$ENV{NGRAM_DUCKLAKE}" STREQUAL "1")
    if(NOT roaring_DIR)
        set(roaring_DIR "${CMAKE_CURRENT_LIST_DIR}/build/deps/croaring/lib/cmake/roaring")
    endif()
    duckdb_extension_load(ducklake
        GIT_URL https://github.com/duckdb/ducklake
        GIT_TAG 417a9117f590e8141742ac0579d02f6a90162e37
    )
    duckdb_extension_statically_link(ducklake)
    duckdb_extension_load(sqlite_scanner
        GIT_URL https://github.com/duckdb/duckdb-sqlite
        GIT_TAG e223dc01493414cef6d5960744f1e048d59fc592
        SUBMODULES database-connector
    )
    duckdb_extension_statically_link(sqlite_scanner)
endif()
