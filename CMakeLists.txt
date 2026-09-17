cmake_minimum_required(VERSION 3.5...3.29)

# Set extension name here
set(TARGET_NAME ngram)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD
    "17"
    CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)

set(EXTENSION_SOURCES
    src/ngram_extension.cpp
    src/settings.cpp
    src/gram.cpp
    src/postings.cpp
    src/catalog.cpp
    src/index_state.cpp
    src/rowid_guard.cpp
    src/fence.cpp
    src/build_sql.cpp
    src/maintenance_sql.cpp
    src/pragmas.cpp
    src/probe.cpp
    src/search_core.cpp
    src/search_functions.cpp
    src/rewrite.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Warnings are errors for the extension's own sources only: the host's tree and
# the third-party code it vendors are built with the host's flags. CI turns this
# on; a distribution build keeps the compiler's defaults.
option(NGRAM_STRICT_WARNINGS
       "Compile the extension sources with -Wall -Wextra -Werror" OFF)
if(NGRAM_STRICT_WARNINGS AND NOT MSVC)
  foreach(ngram_target ${EXTENSION_NAME} ${LOADABLE_EXTENSION_NAME})
    target_compile_options(${ngram_target} PRIVATE -Wall -Wextra -Werror
                                                   -Wno-unused-parameter)
  endforeach()
endif()

# A small C++ harness is needed to pause between pragma preprocessing and the
# generated transaction, which SQLLogicTest cannot do deterministically.
add_executable(ngram_checkpoint_gap_test EXCLUDE_FROM_ALL
               test/cpp/ngram_maintenance_checkpoint_gap.cpp)
target_include_directories(ngram_checkpoint_gap_test
                           PRIVATE duckdb/extension/core_functions/include)
target_link_libraries(ngram_checkpoint_gap_test duckdb ngram_extension)
set(NGRAM_CHECKPOINT_GAP_DB
    "${CMAKE_CURRENT_BINARY_DIR}/ngram_checkpoint_gap_test.db")
add_custom_target(
  run_ngram_checkpoint_gap_test
  COMMAND $<TARGET_FILE:ngram_checkpoint_gap_test> "${NGRAM_CHECKPOINT_GAP_DB}"
          "${CMAKE_CURRENT_SOURCE_DIR}/test/fixtures"
  DEPENDS ngram_checkpoint_gap_test
  VERBATIM)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
