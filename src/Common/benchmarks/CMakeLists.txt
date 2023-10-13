proton_add_executable (benchmark_string_copy benchmark_string_copy.cpp)
target_link_libraries (benchmark_string_copy PRIVATE ch_contrib::gbenchmark_all)

proton_add_executable(integer_hash_tables_and_hashes integer_hash_tables_and_hashes.cpp)
target_link_libraries (integer_hash_tables_and_hashes PRIVATE
    ch_contrib::gbenchmark_all
    dbms
    ch_contrib::abseil_swiss_tables
    ch_contrib::sparsehash
    ch_contrib::wyhash
    ch_contrib::farmhash
    ch_contrib::xxHash)
