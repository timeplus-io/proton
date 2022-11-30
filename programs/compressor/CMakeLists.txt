set (CLICKHOUSE_COMPRESSOR_SOURCES Compressor.cpp)

set (CLICKHOUSE_COMPRESSOR_LINK
    PRIVATE
        boost::program_options
        clickhouse_parsers
        dbms
)

proton_program_add(compressor)