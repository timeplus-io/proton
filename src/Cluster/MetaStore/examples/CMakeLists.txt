proton_add_executable (rocksdb_save rocksdb_save.cpp)
target_link_libraries (rocksdb_save PRIVATE cluster_rocks)
