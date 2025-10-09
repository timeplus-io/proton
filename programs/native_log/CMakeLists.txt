set(CLICKHOUSE_NLOG_SOURCES native_log.cpp native_log_cli_parser.cpp)

set(CLICKHOUSE_NLOG_LINK PRIVATE nlog loggers)

proton_program_add(nlog)
