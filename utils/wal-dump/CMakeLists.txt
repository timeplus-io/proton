proton_add_executable (wal-dump main.cpp)
target_link_libraries(wal-dump PRIVATE
    dbms
    daemon
    loggers
    clickhouse_aggregate_functions
    clickhouse_functions
    clickhouse_table_functions
    boost::program_options)
