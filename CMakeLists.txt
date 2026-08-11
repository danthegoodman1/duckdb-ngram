cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME ngram)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD "17" CACHE STRING "C++ standard to enforce")
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)

set(EXTENSION_SOURCES src/ngram_extension.cpp src/trigram.cpp src/trigrams_function.cpp src/postings_codec.cpp
                      src/pack_postings.cpp src/index_pragmas.cpp src/ngram_maintenance.cpp src/ngram_search.cpp src/ngram_rewrite.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# A small C++ harness is needed to pause between pragma preprocessing and the
# generated transaction, which SQLLogicTest cannot do deterministically.
add_executable(ngram_checkpoint_gap_test EXCLUDE_FROM_ALL test/cpp/ngram_maintenance_checkpoint_gap.cpp)
target_link_libraries(ngram_checkpoint_gap_test duckdb)
set(NGRAM_CHECKPOINT_GAP_DB "${CMAKE_CURRENT_BINARY_DIR}/ngram_checkpoint_gap_test.db")
add_custom_target(
  run_ngram_checkpoint_gap_test
  COMMAND ${CMAKE_COMMAND} -E rm -f "${NGRAM_CHECKPOINT_GAP_DB}" "${NGRAM_CHECKPOINT_GAP_DB}.wal"
  COMMAND $<TARGET_FILE:ngram_checkpoint_gap_test> "${NGRAM_CHECKPOINT_GAP_DB}"
  COMMAND ${CMAKE_COMMAND} -E rm -f "${NGRAM_CHECKPOINT_GAP_DB}" "${NGRAM_CHECKPOINT_GAP_DB}.wal"
  DEPENDS ngram_checkpoint_gap_test
  VERBATIM)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
