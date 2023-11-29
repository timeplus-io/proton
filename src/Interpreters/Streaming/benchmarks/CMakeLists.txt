proton_add_executable (vector_deque_perf vector_deque_perf.cpp)
target_link_libraries (vector_deque_perf PRIVATE ch_contrib::gbenchmark_all)

proton_add_executable (data_blocks_perf data_blocks_perf.cpp)
target_link_libraries (data_blocks_perf PRIVATE ch_contrib::gbenchmark_all dbms)
