add_executable (native_log native_log.cpp native_log_cli_parser.cpp)

target_link_libraries (native_log PRIVATE nlog loggers)

target_include_directories(native_log
        PRIVATE
        "${ClickHouse_SOURCE_DIR}/src")

# Test range query lower/upper bound seek performance
add_executable (rocksdb_seek_perf rocksdb_seek_perf.cpp)
target_link_libraries (rocksdb_seek_perf PRIVATE ch_contrib::rocksdb)
target_include_directories(rocksdb_seek_perf
        SYSTEM PRIVATE
        "${ROCKSDB_INCLUDE_DIR}")

# Copy one rocksdb example from rocksdb repo for easier experiments
add_executable (rocksdb_simple_example rocksdb_simple_example.cpp)
target_link_libraries (rocksdb_simple_example PRIVATE ch_contrib::rocksdb)
target_include_directories(rocksdb_simple_example
        SYSTEM PRIVATE
        "${ROCKSDB_INCLUDE_DIR}")

