proton_add_executable(benchmark_string_copy benchmark_string_copy.cpp)
target_link_libraries(benchmark_string_copy PRIVATE ch_contrib::gbenchmark_all)

proton_add_executable(integer_hash_tables_and_hashes integer_hash_tables_and_hashes.cpp)
target_link_libraries(integer_hash_tables_and_hashes PRIVATE
        ch_contrib::gbenchmark_all
        dbms
        ch_contrib::abseil_swiss_tables
        ch_contrib::sparsehash
        ch_contrib::wyhash
        ch_contrib::farmhash
        ch_contrib::xxHash)

proton_add_executable(benchmark_stack_unwinding benchmark_stack_unwinding.cpp)
target_link_libraries(benchmark_stack_unwinding PRIVATE
        ch_contrib::gbenchmark_all
        dbms)

proton_add_executable(benchmark_hybrid_hash_table benchmark_hybrid_hash_table.cpp)

if (TARGET ch_contrib::jemalloc)
target_link_libraries(benchmark_hybrid_hash_table PRIVATE
        ch_contrib::gbenchmark_all
        ch_contrib::rocksdb
        ch_contrib::jemalloc
        ch_contrib::abseil_swiss_tables
        clickhouse_common_io
        rocks)
else()
target_link_libraries(benchmark_hybrid_hash_table PRIVATE
        ch_contrib::gbenchmark_all
        ch_contrib::rocksdb
        ch_contrib::abseil_swiss_tables
        clickhouse_common_io
        rocks)
endif()

proton_add_executable(benchmark_hybrid_hash_table_lookup benchmark_hybrid_hash_table_lookup.cpp)

if (TARGET ch_contrib::jemalloc)
target_link_libraries(benchmark_hybrid_hash_table_lookup PRIVATE
        ch_contrib::gbenchmark_all
        ch_contrib::rocksdb
        ch_contrib::jemalloc
        ch_contrib::abseil_swiss_tables
        clickhouse_common_io
        rocks)
else()
target_link_libraries(benchmark_hybrid_hash_table_lookup PRIVATE
        ch_contrib::gbenchmark_all
        ch_contrib::rocksdb
        ch_contrib::abseil_swiss_tables
        clickhouse_common_io
        rocks)
endif()


proton_add_executable(benchmark_memory_allocate benchmark_memory_allocate.cpp)

if (TARGET ch_contrib::jemalloc)
    target_link_libraries(benchmark_memory_allocate PRIVATE
            ch_contrib::gbenchmark_all
            ch_contrib::jemalloc
            clickhouse_common_io)
else ()
    target_link_libraries(benchmark_memory_allocate PRIVATE
            ch_contrib::gbenchmark_all
            clickhouse_common_io)
endif ()
