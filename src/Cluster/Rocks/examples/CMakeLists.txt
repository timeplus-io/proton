# Test range query lower/upper bound seek performance
proton_add_executable(rocksdb_seek_perf rocksdb_seek_perf.cpp)
target_link_libraries(rocksdb_seek_perf PRIVATE ch_contrib::rocksdb)
target_include_directories(rocksdb_seek_perf
        SYSTEM PRIVATE
        "${ROCKSDB_INCLUDE_DIR}")

# Copy one rocksdb example from rocksdb repo for easier experiments
proton_add_executable(rocksdb_simple_example rocksdb_simple_example.cpp)
target_link_libraries(rocksdb_simple_example PRIVATE ch_contrib::rocksdb)
target_include_directories(rocksdb_simple_example
        SYSTEM PRIVATE
        "${ROCKSDB_INCLUDE_DIR}")

proton_add_executable(rocksdb_prefix_seek rocksdb_prefix_seek.cpp)
target_link_libraries(rocksdb_prefix_seek PRIVATE ch_contrib::rocksdb)
target_include_directories(rocksdb_prefix_seek
        SYSTEM PRIVATE
        "${ROCKSDB_INCLUDE_DIR}")

proton_add_executable(neutron_rocks neutron_rocksdb.cpp)
target_link_libraries(neutron_rocks PRIVATE ch_contrib::rocksdb)
target_include_directories(neutron_rocks
        SYSTEM PRIVATE
        "${ROCKSDB_INCLUDE_DIR}")

proton_add_executable(rocksdb_merge rocksdb_merge.cpp)
target_link_libraries(rocksdb_merge PRIVATE ch_contrib::rocksdb)
target_include_directories(rocksdb_merge
        SYSTEM PRIVATE
        "${ROCKSDB_INCLUDE_DIR}")
