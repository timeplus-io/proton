proton_add_executable (string_pool string_pool.cpp)
target_link_libraries (string_pool PRIVATE clickhouse_common_io ch_contrib::sparsehash)

proton_add_executable (field field.cpp)
target_link_libraries (field PRIVATE dbms)

proton_add_executable (string_ref_hash string_ref_hash.cpp)
target_link_libraries (string_ref_hash PRIVATE clickhouse_common_io)

proton_add_executable (coro coro.cpp)
target_link_libraries (coro PRIVATE clickhouse_common_io)
