include(${ClickHouse_SOURCE_DIR}/cmake/embed_binary.cmake)

set(CLICKHOUSE_METASTORE_SOURCES
    MetaStore.cpp
)

if (OS_LINUX)
    set (LINK_RESOURCE_LIB INTERFACE "-Wl,${WHOLE_ARCHIVE} $<TARGET_FILE:clickhouse_metastore_configs> -Wl,${NO_WHOLE_ARCHIVE}")
endif ()

set (CLICKHOUSE_METASTORE_LINK
    PRIVATE
        clickhouse_common_config
        clickhouse_common_io
        daemon
        dbms

    ${LINK_RESOURCE_LIB}
)

clickhouse_program_add(metastore)

install (FILES metastore_config.yaml users.yaml DESTINATION "${CLICKHOUSE_ETC_DIR}/clickhouse-metastore" COMPONENT proton-metastore)

clickhouse_embed_binaries(
    TARGET clickhouse_metastore_configs
    RESOURCES metastore_config.yaml metastore_embedded.yaml
)

add_dependencies(clickhouse-metastore-lib clickhouse_metastore_configs)
