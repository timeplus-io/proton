set (SRCS
    AbstractConfigurationComparison.cpp
    ConfigProcessor.cpp
    configReadClient.cpp
    ConfigReloader.cpp
    YAMLParser.cpp
    ConfigHelper.cpp
    ExternalGrokPatterns.cpp
)

# ExternalGrokPatterns.cpp embeds grok-patterns via C23 `#embed`.
set_source_files_properties(ExternalGrokPatterns.cpp PROPERTIES COMPILE_FLAGS -Wno-c23-extensions)

add_library(clickhouse_common_config ${SRCS})
target_link_libraries(clickhouse_common_config
    PUBLIC
        clickhouse_common_zookeeper
        common
        Poco::XML
    PRIVATE
        string_utils
)

if (TARGET ch_contrib::yaml_cpp)
    target_link_libraries(clickhouse_common_config PUBLIC ch_contrib::yaml_cpp)
endif()
