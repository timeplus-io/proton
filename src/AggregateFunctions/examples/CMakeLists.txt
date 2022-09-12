add_executable (quantile-t-digest quantile-t-digest.cpp)
target_link_libraries (quantile-t-digest PRIVATE dbms clickhouse_aggregate_functions)

add_executable (counted_value_map counted_value_map.cpp)
target_link_libraries (counted_value_map PRIVATE ch_contrib::abseil_swiss_tables)

add_executable (counted_arg_value_map counted_arg_value_map.cpp)
target_link_libraries (counted_arg_value_map PRIVATE ch_contrib::abseil_swiss_tables)


