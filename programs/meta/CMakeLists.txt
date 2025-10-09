include(${proton_SOURCE_DIR}/cmake/embed_binary.cmake)

set(CLICKHOUSE_META_SOURCES
    meta.cpp
    meta_cli_parser.cpp
)

set (CLICKHOUSE_META_LINK
        PRIVATE
        dbms
)

proton_program_add(meta)
