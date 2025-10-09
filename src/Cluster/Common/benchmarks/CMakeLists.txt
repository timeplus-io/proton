proton_add_executable (benchmark_timer benchmark_timer.cpp)

target_link_libraries (benchmark_timer PRIVATE ch_contrib::gbenchmark_all cluster_common ch_contrib::uv)
