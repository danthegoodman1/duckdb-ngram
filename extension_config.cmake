# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(ngram
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)
# Link it into the shell and unittest, which the SQL tests and the release
# evidence tool run; DuckDB 2.0 links only the extensions a config names here.
duckdb_extension_statically_link(ngram)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)