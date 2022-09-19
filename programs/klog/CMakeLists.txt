set(CLICKHOUSE_KLOG_BENCHMARK_SOURCES klog.cpp)

set (CLICKHOUSE_KLOG_BENCHMARK_LINK
    PRIVATE
        dbms
)

proton_program_add(klog-benchmark)
