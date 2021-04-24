set(CLICKHOUSE_DWAL_BENCHMARK_SOURCES DistributedWal.cpp)

set (CLICKHOUSE_DWAL_BENCHMARK_LINK
    PRIVATE
        dbms
)

clickhouse_program_add(dwal-benchmark)
