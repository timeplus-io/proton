add_headers_and_sources(loggers .)
add_library(loggers ${loggers_sources} ${loggers_headers})
target_link_libraries(loggers PRIVATE dbms clickhouse_common_io)

# Lightweight version doesn't work with textlog and also doesn't depend on DBMS
add_library(loggers_no_text_log ${loggers_sources} ${loggers_headers})
target_link_libraries(loggers_no_text_log PRIVATE clickhouse_common_io)
