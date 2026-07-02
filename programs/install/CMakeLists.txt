set (CLICKHOUSE_INSTALL_SOURCES Install.cpp)

# Install.cpp embeds default config.yaml / users.yaml / grok-patterns via C23 `#embed`.
set_source_files_properties(Install.cpp PROPERTIES COMPILE_FLAGS -Wno-c23-extensions)

set (CLICKHOUSE_INSTALL_LINK
    PRIVATE
        boost::program_options
        clickhouse_common_config
        clickhouse_common_io
        dbms
        readpassphrase
)

proton_program_add_library(install)
