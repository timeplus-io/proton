proton_add_executable (kcat kcat.cpp kcat/kcat.c kcat/format.c kcat/tools.c kcat/input.c)

target_link_libraries (kcat PRIVATE ch_contrib::rdkafka)
target_compile_options(kcat PRIVATE -w)

#target_include_directories(rocksdb_seek_perf
#        SYSTEM PRIVATE
#        "${ROCKSDB_INCLUDE_DIR}")
