proton_add_executable (wal_action_metadata wal_action_metadata.cpp)
target_link_libraries (wal_action_metadata PRIVATE
    dbms
    clickhouse_aggregate_functions
    clickhouse_functions
    clickhouse_table_functions)
