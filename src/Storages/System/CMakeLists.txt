# The file StorageSystemContributors.cpp is generated at release time and committed to the source tree.
# You can also regenerate it manually this way:
# execute_process(COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/StorageSystemContributors.sh")

get_property (BUILD_COMPILE_DEFINITIONS DIRECTORY ${proton_SOURCE_DIR} PROPERTY COMPILE_DEFINITIONS)
get_property(TZDATA_VERSION GLOBAL PROPERTY TZDATA_VERSION_PROP)

# proton: start.
string (TIMESTAMP BUILD_TIME "%Y-%m-%dT%H:%M:%SZ" UTC)
# proton: end.

function(generate_system_build_options)
    include(${proton_SOURCE_DIR}/src/configure_config.cmake)
    configure_file(StorageSystemBuildOptions.cpp.in StorageSystemBuildOptions.generated.cpp)
endfunction()

generate_system_build_options()

include("${proton_SOURCE_DIR}/cmake/dbms_glob_sources.cmake")
add_headers_and_sources(storages_system .)
list (APPEND storages_system_sources StorageSystemBuildOptions.generated.cpp)

add_custom_target(generate-contributors
    ./StorageSystemContributors.sh
    SOURCES StorageSystemContributors.sh
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    # BYPRODUCTS StorageSystemContributors.generated.cpp
)

if(NOT TARGET generate-source)
    add_custom_target(generate-source)
endif()

add_dependencies(generate-source generate-contributors)

set(GENERATED_LICENSES_SRC "${CMAKE_CURRENT_BINARY_DIR}/StorageSystemLicenses.generated.cpp")

add_custom_command(
    OUTPUT StorageSystemLicenses.generated.cpp
    COMMAND ./StorageSystemLicenses.sh > ${GENERATED_LICENSES_SRC}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

list (APPEND storages_system_sources ${GENERATED_LICENSES_SRC})
# auto_time_zones[] is now provided by the `tzdata` static lib generated in
# contrib/cctz-cmake and pulled in transitively via _cctz.

# Overlength strings
set_source_files_properties(${GENERATED_LICENSES_SRC} PROPERTIES COMPILE_FLAGS -w)

# InformationSchema/{schemata,tables,views,columns}.sql and Cluster/v_*.sql are
# embedded directly into attachInformationSchemaTables.cpp and
# Interpreters/BuiltinSchemasProvisioner.cpp via C23 `#embed`.
set_source_files_properties(
    attachInformationSchemaTables.cpp
    PROPERTIES COMPILE_FLAGS -Wno-c23-extensions)

list (SORT storages_system_sources) # Reproducible build
add_library(clickhouse_storages_system ${storages_system_sources})

target_link_libraries(clickhouse_storages_system PRIVATE
    dbms
    common
    string_utils
    clickhouse_common_zookeeper
    clickhouse_parsers
    Poco::JSON
)
