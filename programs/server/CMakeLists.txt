set(CLICKHOUSE_SERVER_SOURCES
    MetricsTransmitter.cpp
    Server.cpp
    Server_Python.cpp
    Server_V8.cpp
)

set (CLICKHOUSE_SERVER_LINK
    PRIVATE
        clickhouse_aggregate_functions
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_common_zookeeper
        clickhouse_functions
        clickhouse_parsers
        clickhouse_storages_system
        clickhouse_table_functions
        string_utils

    PUBLIC
        daemon
)
if (TARGET ch_contrib::jemalloc)
    list(APPEND CLICKHOUSE_SERVER_LINK PRIVATE ch_contrib::jemalloc)
endif()

if (TARGET ch_contrib::azure_sdk)
    list(APPEND CLICKHOUSE_SERVER_LINK PRIVATE ch_contrib::azure_sdk)
endif()

if (ENABLE_PYTHON_UDF)
    set(CLICKHOUSE_SERVER_DEFINITIONS Py_BUILD_CORE=1)
endif ()


proton_program_add(server)

install(FILES config.yaml users.yaml DESTINATION "${CLICKHOUSE_ETC_DIR}/clickhouse-server" COMPONENT proton)
# play.html, embedded.xml, config.yaml, users.yaml, and grok-patterns are
# embedded into their respective consumers via C23 `#embed` (no .incbin
# static lib, no -rdynamic dlsym lookup). Server.cpp hosts the
# `embedded.xml` fallback (registered with ConfigProcessor at startup).
set_source_files_properties(Server.cpp PROPERTIES COMPILE_FLAGS -Wno-c23-extensions)
