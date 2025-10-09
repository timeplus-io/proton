proton_add_executable (check-marks main.cpp)
target_link_libraries(check-marks PRIVATE
    dbms
    daemon
    loggers
    clickhouse_aggregate_functions
    clickhouse_functions
    clickhouse_table_functions
    boost::program_options)
