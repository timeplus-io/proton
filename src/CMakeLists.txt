if (USE_INCLUDE_WHAT_YOU_USE)
    set (CMAKE_CXX_INCLUDE_WHAT_YOU_USE ${IWYU_PATH})
endif ()

if (USE_CLANG_TIDY)
    set (CMAKE_CXX_CLANG_TIDY "${CLANG_TIDY_PATH}")
endif ()

if(COMPILER_PIPE)
    set(MAX_COMPILER_MEMORY 2500)
else()
    set(MAX_COMPILER_MEMORY 1500)
endif()
set(MAX_LINKER_MEMORY 3500)
include(../cmake/limit_jobs.cmake)

include (../cmake/version.cmake)
message (STATUS "Will build ${VERSION_FULL} revision ${VERSION_REVISION} ${VERSION_OFFICIAL}")
include (configure_config.cmake)
configure_file (Common/config.h.in ${CONFIG_INCLUDE_PATH}/config.h)
configure_file (Common/config_version.cpp.in ${CONFIG_INCLUDE_PATH}/config_version.cpp)

# proton: starts
configure_file (Common/testing_config.h.in ${CONFIG_INCLUDE_PATH}/testing_config.h)
include(${proton_SOURCE_DIR}/cmake/split_debug_symbols.cmake)
# proton: ends

if (USE_DEBUG_HELPERS)
    get_target_property(MAGIC_ENUM_INCLUDE_DIR ch_contrib::magic_enum INTERFACE_INCLUDE_DIRECTORIES)
    # CMake generator expression will do insane quoting when it encounters special character like quotes, spaces, etc.
    # Prefixing "SHELL:" will force it to use the original text.
    set (INCLUDE_DEBUG_HELPERS "SHELL:-I\"${proton_SOURCE_DIR}/base\" -I\"${MAGIC_ENUM_INCLUDE_DIR}\" -include \"${proton_SOURCE_DIR}/src/Core/iostream_debug_helpers.h\"")
    # Use generator expression as we don't want to pollute CMAKE_CXX_FLAGS, which will interfere with CMake check system.
    add_compile_options($<$<COMPILE_LANGUAGE:CXX>:${INCLUDE_DEBUG_HELPERS}>)
endif ()

# proton developers may use platform-dependent code under some macro (e.g. `#ifdef ENABLE_MULTITARGET`).
# If turned ON, this option defines such macro.
# See `src/Common/TargetSpecific.h`
option(ENABLE_MULTITARGET_CODE "Enable platform-dependent code" ON)

if (ENABLE_MULTITARGET_CODE)
    add_definitions(-DENABLE_MULTITARGET_CODE=1)
else()
    add_definitions(-DENABLE_MULTITARGET_CODE=0)
endif()

set(dbms_headers)
set(dbms_sources)

add_subdirectory (Access)
add_subdirectory (Backups)
add_subdirectory (Columns)
add_subdirectory (Common)
add_subdirectory (Core)
add_subdirectory (QueryPipeline)
add_subdirectory (DataTypes)
add_subdirectory (Dictionaries)
add_subdirectory (Disks)
add_subdirectory (Storages)
add_subdirectory (Parsers)
add_subdirectory (IO)
add_subdirectory (Functions)
add_subdirectory (Planner)
add_subdirectory (Interpreters)
add_subdirectory (AggregateFunctions)
add_subdirectory (TableFunctions)
add_subdirectory (Processors)
add_subdirectory (Compression)
add_subdirectory (Server)
add_subdirectory (Daemon)
add_subdirectory (Loggers)

add_headers_and_sources(clickhouse_common_io Common)
add_headers_and_sources(clickhouse_common_io Common/HashTable)
add_headers_and_sources(clickhouse_common_io Common/Scheduler)
add_headers_and_sources(clickhouse_common_io Common/Scheduler/Nodes)
add_headers_and_sources(clickhouse_common_io IO)
add_headers_and_sources(clickhouse_common_io IO/S3)
add_headers_and_sources(clickhouse_common_io IO/AzureBlobStorage)
list (REMOVE_ITEM clickhouse_common_io_sources Common/malloc.cpp Common/new_delete.cpp)

add_headers_and_sources(dbms Disks/IO)
add_headers_and_sources(dbms Disks/ObjectStorages)
if (USE_LIBPQXX)
    add_headers_and_sources(dbms Core/PostgreSQL)
    add_headers_and_sources(dbms Databases/PostgreSQL)
    add_headers_and_sources(dbms Storages/PostgreSQL)
endif()

if (TARGET ch_contrib::aws_sdk) # proton: added
    add_headers_and_sources(dbms Common/S3)
endif()
if (TARGET ch_contrib::aws_s3)
    add_headers_and_sources(dbms Disks/ObjectStorages/S3)
endif()

if (TARGET ch_contrib::azure_sdk)
    add_headers_and_sources(dbms Disks/ObjectStorages/AzureBlobStorage)
    add_headers_and_sources(dbms IO/AzureBlobStorage)
endif()

add_headers_and_sources(dbms Databases/ApacheIceberg)

add_headers_and_sources(dbms Disks/ObjectStorages/Cached)
add_headers_and_sources(dbms Disks/ObjectStorages/Local)
add_headers_and_sources(dbms Disks/ObjectStorages/Web)

add_headers_and_sources(dbms Storages/Cache)

add_headers_and_sources(dbms Common/NamedCollections)

list(APPEND clickhouse_common_io_sources ${CONFIG_INCLUDE_PATH}/config_version.cpp)

if (NOT ENABLE_SSL)
    list (REMOVE_ITEM clickhouse_common_io_sources Common/OpenSSLHelpers.cpp)
    list (REMOVE_ITEM clickhouse_common_io_headers Common/OpenSSLHelpers.h)
endif ()

add_library(clickhouse_common_io ${clickhouse_common_io_headers} ${clickhouse_common_io_sources})

add_library (clickhouse_malloc OBJECT Common/malloc.cpp)
set_source_files_properties(Common/malloc.cpp PROPERTIES COMPILE_FLAGS "-fno-builtin")

add_library (clickhouse_new_delete STATIC Common/new_delete.cpp)
target_link_libraries (clickhouse_new_delete PRIVATE clickhouse_common_io)
if (TARGET ch_contrib::jemalloc)
    target_link_libraries (clickhouse_new_delete PRIVATE ch_contrib::jemalloc)
endif()

if (TARGET ch_contrib::jemalloc)
    target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::jemalloc)
    target_link_libraries (clickhouse_storages_system PRIVATE ch_contrib::jemalloc)
endif()

add_subdirectory(Access/Common)
add_subdirectory(Common/ZooKeeper)
add_subdirectory(Common/Config)

# proton: starts
add_subdirectory(Cluster)

add_subdirectory(Checkpoint)

# proton: ends

set (all_modules)
macro(add_object_library name common_path)
    add_headers_and_sources(dbms ${common_path})
endmacro()

add_object_library(clickhouse_access Access)
add_object_library(clickhouse_backups Backups)
add_object_library(clickhouse_core Core)
add_object_library(clickhouse_core_streaming Core/Streaming)
add_object_library(clickhouse_core_mysql Core/MySQL)
add_object_library(clickhouse_compression Compression)
add_object_library(clickhouse_querypipeline QueryPipeline)
add_object_library(clickhouse_datatypes DataTypes)
add_object_library(clickhouse_datatypes_serializations DataTypes/Serializations)
add_object_library(clickhouse_databases Databases)
add_object_library(clickhouse_databases_mysql Databases/MySQL)
add_object_library(clickhouse_disks Disks)
add_object_library(clickhouse_planner Planner)
add_object_library(clickhouse_interpreters Interpreters)
add_object_library(clickhouse_interpreters_cache Interpreters/Cache)
add_object_library(clickhouse_interpreters_access Interpreters/Access)
add_object_library(clickhouse_interpreters_clusterproxy Interpreters/ClusterProxy)
add_object_library(clickhouse_interpreters_jit Interpreters/JIT)
add_object_library(clickhouse_columns Columns)
add_object_library(clickhouse_storages Storages)
add_object_library(clickhouse_storages_mysql Storages/MySQL)
add_object_library(clickhouse_storages_distributed Storages/Distributed)
add_object_library(clickhouse_storages_mergetree Storages/MergeTree)
add_object_library(clickhouse_storages_liveview Storages/LiveView)
add_object_library(clickhouse_storages_windowview Storages/WindowView)
add_object_library(clickhouse_client Client)
add_object_library(clickhouse_bridge BridgeHelper)
add_object_library(clickhouse_server Server)
add_object_library(clickhouse_server_http Server/HTTP)
add_object_library(clickhouse_formats Formats)
add_object_library(clickhouse_processors Processors)
# proton : starts
add_object_library(clickhouse_storages_mysql Storages/Iceberg)
add_object_library(clickhouse_processors_streaming Processors/Streaming)
add_object_library(clickhouse_task Task)
add_object_library(clickhouse_formats_avro Formats/Avro)
# proton : ends
add_object_library(clickhouse_processors_executors Processors/Executors)
add_object_library(clickhouse_processors_formats Processors/Formats)
add_object_library(clickhouse_processors_formats_impl Processors/Formats/Impl)
add_object_library(clickhouse_processors_transforms Processors/Transforms)

# proton: starts
add_object_library(clickhouse_server_rest_handlers Server/RestRouterHandlers)
add_object_library(clickhouse_interpreters_streaming Interpreters/Streaming)
add_object_library(clickhouse_interpreters_aggregator_streaming Interpreters/Streaming/Aggregator)
add_object_library(clickhouse_interpreters_aggregator_streaming Interpreters/Streaming/Aggregator/HybridAggregator)
add_object_library(clickhouse_interpreters_aggregator_streaming Interpreters/Streaming/Aggregator/MemoryAggregator)
add_object_library(clickhouse_interpreters_hash_join_streaming Interpreters/Streaming/HashJoin)
add_object_library(clickhouse_interpreters_hash_join_streaming Interpreters/Streaming/HashJoin/MemoryHashJoin)
add_object_library(clickhouse_interpreters_hash_join_streaming Interpreters/Streaming/HashJoin/HybridHashJoin)
add_object_library(clickhouse_interpreters_apply Interpreters/Apply)
add_object_library(clickhouse_interpreters_historical_storage_snapshot Interpreters/StorageSnapshot)
add_object_library(clickhouse_processors_transforms_streaming Processors/Transforms/Streaming)
add_object_library(clickhouse_storages_mv Storages/MatView)
add_object_library(clickhouse_storages_stream Storages/Stream)
add_object_library(clickhouse_storages_random Storages/Random)
add_object_library(clickhouse_storages_proxy Storages/Proxy)
add_object_library(clickhouse_storages_proxy Storages/Alert)
add_object_library(clickhouse_processors_queryplan_streaming Processors/QueryPlan/Streaming)
add_object_library(clickhouse_functions_streaming Functions/Streaming)
add_object_library(clickhouse_aggregate_functions_streaming AggregateFunctions/Streaming)
# proton: ends

add_object_library(clickhouse_processors_sources Processors/Sources)
add_object_library(clickhouse_processors_sinks Processors/Sinks)
add_object_library(clickhouse_processors_merges Processors/Merges)
add_object_library(clickhouse_processors_ttl Processors/TTL)
add_object_library(clickhouse_processors_merges_algorithms Processors/Merges/Algorithms)
add_object_library(clickhouse_processors_queryplan Processors/QueryPlan)
add_object_library(clickhouse_processors_queryplan_optimizations Processors/QueryPlan/Optimizations)
add_object_library(clickhouse_user_defined_functions Functions/UserDefined)

if (USE_PARQUET)
    add_object_library(clickhouse_processors_formats_impl_parquet Processors/Formats/Impl/Parquet)
endif()

# proton: starts
add_object_library(clickhouse_processors_queryplan_streaming Processors/QueryPlan/Streaming)
add_object_library(clickhouse_distributed_metadata_service DistributedMetadata)
add_object_library(clickhouse_bootstrap Bootstrap)

# lib external_stream (deps on) -> klog -> nlog -> clickhouse_common_io
add_subdirectory(ClickHouse)
add_object_library(clickhouse_clickhouse ClickHouse)

add_subdirectory(IO/Kafka)

add_subdirectory(Storages/ExternalStream)
add_subdirectory(Storages/ExternalTable)
# proton: end

set (DBMS_COMMON_LIBRARIES)

add_library (dbms STATIC ${dbms_headers} ${dbms_sources})
target_link_libraries (dbms PRIVATE ch_contrib::libdivide ${DBMS_COMMON_LIBRARIES})
if (TARGET ch_contrib::jemalloc)
    target_link_libraries (dbms PRIVATE ch_contrib::jemalloc)
endif()

target_link_libraries (dbms PUBLIC cluster)
target_link_libraries (dbms PRIVATE external_stream)
target_link_libraries (dbms PRIVATE external_table)
target_link_libraries (dbms PRIVATE checkpoint)

set (all_modules dbms)

macro (dbms_target_include_directories)
    foreach (module ${all_modules})
        target_include_directories (${module} ${ARGN})
    endforeach ()
endmacro ()

macro (dbms_target_link_libraries)
    foreach (module ${all_modules})
        target_link_libraries (${module} ${ARGN})
    endforeach ()
endmacro ()

dbms_target_include_directories (PUBLIC "${proton_SOURCE_DIR}/src" "${proton_BINARY_DIR}/src")
target_include_directories (clickhouse_common_io PUBLIC "${proton_SOURCE_DIR}/src" "${proton_BINARY_DIR}/src")

if (TARGET ch_contrib::llvm)
    dbms_target_link_libraries (PUBLIC ch_contrib::llvm)
endif ()

if (TARGET ch_contrib::gwp_asan)
    target_link_libraries (clickhouse_common_io PRIVATE ch_contrib::gwp_asan)
    target_link_libraries (clickhouse_new_delete PRIVATE ch_contrib::gwp_asan)
endif()

# Otherwise it will slow down stack traces printing too much.
set_source_files_properties(
        Common/Elf.cpp
        Common/Dwarf.cpp
        Common/SymbolIndex.cpp
        PROPERTIES COMPILE_FLAGS "-O2 ${WITHOUT_COVERAGE}")

target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::rocksdb)

target_link_libraries (clickhouse_common_io
        PRIVATE
            string_utils
            widechar_width
            ${LINK_LIBRARIES_ONLY_ON_X86_64}
        PUBLIC
            common
            ch_contrib::double_conversion
            ch_contrib::dragonbox_to_chars
)

# Use X86 AVX2/AVX512 instructions to accelerate filter operations
set_source_files_properties(
        Columns/ColumnFixedString.cpp
        Columns/ColumnsCommon.cpp
        Columns/ColumnVector.cpp
        Columns/ColumnDecimal.cpp
        Columns/ColumnString.cpp
        PROPERTIES COMPILE_FLAGS "${X86_INTRINSICS_FLAGS}")

target_link_libraries(clickhouse_common_io
        PUBLIC
            boost::program_options
            boost::system
            ch_contrib::cityhash
            ch_contrib::re2
            ch_contrib::zlib
            pcg_random
            Poco::Foundation
)

if (TARGET ch_contrib::cpuid)
    target_link_libraries(clickhouse_common_io PRIVATE ch_contrib::cpuid)
endif()

dbms_target_link_libraries(PUBLIC ch_contrib::abseil_swiss_tables)

# Make dbms depend on roaring instead of clickhouse_common_io so that roaring itself can depend on clickhouse_common_io
# That way we we can redirect malloc/free functions avoiding circular dependencies
dbms_target_link_libraries(PUBLIC ch_contrib::roaring)

# proton: starts
# rdkafka needed for external Kafka streams (not internal storage)
if (TARGET ch_contrib::rdkafka)
    dbms_target_link_libraries(PRIVATE ch_contrib::rdkafka)
endif()
# proton: ends

if (TARGET ch_contrib::nats_io)
    dbms_target_link_libraries(PRIVATE ch_contrib::nats_io ch_contrib::uv)
endif()

if (TARGET ch_contrib::sasl2)
    dbms_target_link_libraries(PRIVATE ch_contrib::sasl2)
endif()

if (TARGET ch_contrib::krb5)
    dbms_target_link_libraries(PRIVATE ch_contrib::krb5)
endif()

# Build and link V8 glue only when V8 is available and enabled
if (USE_V8)
    add_subdirectory(V8)
    dbms_target_link_libraries(PUBLIC V8)
    dbms_target_link_libraries(PUBLIC ${CMAKE_DL_LIBS})
endif()

if (TARGET ch_contrib::pulsar)
    dbms_target_link_libraries(PUBLIC ch_contrib::pulsar)
endif()

# proton: ends

dbms_target_link_libraries (
    PRIVATE
        boost::filesystem
        boost::program_options
        clickhouse_common_config
        clickhouse_common_zookeeper
        clickhouse_dictionaries_embedded
        clickhouse_parsers
        ch_contrib::lz4
        Poco::JSON
        string_utils
        rocks
    PUBLIC
        boost::system
        clickhouse_common_io
)

if (TARGET ch_contrib::mongocxx)
    dbms_target_link_libraries(
        PUBLIC
            ch_contrib::libbson
            ch_contrib::libmongoc
            ch_contrib::bsoncxx
            ch_contrib::mongocxx
    )
endif ()

if (TARGET ch::mysqlxx)
    dbms_target_link_libraries (PUBLIC ch::mysqlxx)
endif()

dbms_target_link_libraries (
    PUBLIC
        boost::circular_buffer
        boost::heap)

#target_include_directories(clickhouse_common_io PUBLIC "${CMAKE_CURRENT_BINARY_DIR}/Core/include") # uses some includes from core
#dbms_target_include_directories(PUBLIC "${CMAKE_CURRENT_BINARY_DIR}/Core/include")

target_link_libraries(clickhouse_common_io PUBLIC
    ch_contrib::miniselect
    ch_contrib::pdqsort)
dbms_target_link_libraries(PUBLIC
    ch_contrib::miniselect
    ch_contrib::pdqsort)

dbms_target_link_libraries(PRIVATE ch_contrib::zstd)
target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::zstd)
target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::xz)

if (TARGET ch_contrib::icu)
    dbms_target_link_libraries (PRIVATE ch_contrib::icu)
endif ()

if (TARGET ch_contrib::capnp)
    dbms_target_link_libraries (PRIVATE ch_contrib::capnp)
endif ()

if (TARGET ch_contrib::parquet)
    dbms_target_link_libraries (PRIVATE ch_contrib::parquet)
endif ()

if (TARGET ch_contrib::avrocpp)
    dbms_target_link_libraries(PRIVATE ch_contrib::avrocpp)
endif ()

set_source_files_properties(Common/CaresPTRResolver.cpp PROPERTIES COMPILE_FLAGS -Wno-reserved-identifier)
target_link_libraries (clickhouse_common_io PRIVATE ch_contrib::c-ares)

# Sources that use C23 `#embed` to inline resources into `.rodata`.
# (ConfigProcessor.cpp and ExternalGrokPatterns.cpp get the same flag in Common/Config/CMakeLists.txt;
#  attachInformationSchemaTables.cpp gets it in Storages/System/CMakeLists.txt.)
set_source_files_properties(Server/WebUIRequestHandler.cpp PROPERTIES COMPILE_FLAGS -Wno-c23-extensions)
# proton: starts
# BuiltinSchemasProvisioner is Proton-specific.
set_source_files_properties(Interpreters/BuiltinSchemasProvisioner.cpp PROPERTIES COMPILE_FLAGS -Wno-c23-extensions)
# proton: ends

if (TARGET OpenSSL::Crypto)
    dbms_target_link_libraries (PRIVATE OpenSSL::Crypto)
    target_link_libraries (clickhouse_common_io PRIVATE OpenSSL::Crypto)
endif ()

if (TARGET ch_contrib::ldap)
    dbms_target_link_libraries (PRIVATE ch_contrib::ldap ch_contrib::lber)
endif ()
dbms_target_link_libraries (PRIVATE ch_contrib::sparsehash)

if (TARGET ch_contrib::protobuf)
    dbms_target_link_libraries (PRIVATE ch_contrib::protobuf)
endif ()

if (TARGET clickhouse_grpc_protos)
    dbms_target_link_libraries (PUBLIC clickhouse_grpc_protos)
endif()

# proton: starts
if (TARGET ch_contrib::aws_sdk)
    target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::aws_sdk)
endif()
# proton: ends

if (TARGET ch_contrib::azure_sdk)
    target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::azure_sdk)
    dbms_target_link_libraries (PRIVATE ch_contrib::azure_sdk)
endif()

if (TARGET ch_contrib::s2)
    dbms_target_link_libraries (PUBLIC ch_contrib::s2)
endif()

if (TARGET ch_contrib::brotli)
    target_link_libraries (clickhouse_common_io PRIVATE ch_contrib::brotli)
endif()

if (TARGET ch_contrib::snappy)
    target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::snappy)
endif()

if (TARGET ch_contrib::msgpack)
    target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::msgpack)
endif()

if (TARGET ch_contrib::liburing)
    target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::liburing)
    target_include_directories (clickhouse_common_io SYSTEM BEFORE PUBLIC ${LIBURING_COMPAT_INCLUDE_DIR} ${LIBURING_INCLUDE_DIR})
endif()

target_link_libraries (clickhouse_common_io PUBLIC ch_contrib::fast_float)

if (USE_ORC)
    dbms_target_link_libraries(PUBLIC ${ORC_LIBRARIES})
    dbms_target_include_directories(SYSTEM BEFORE PUBLIC ${ORC_INCLUDE_DIR} "${PROJECT_BINARY_DIR}/contrib/orc/c++/include")
endif ()

if (TARGET ch_contrib::rocksdb)
    dbms_target_link_libraries(PUBLIC ch_contrib::rocksdb)
endif()

if (TARGET ch_contrib::libpqxx)
    dbms_target_link_libraries(PUBLIC ch_contrib::libpqxx)
endif()

if (TARGET ch_contrib::datasketches)
    target_link_libraries (clickhouse_aggregate_functions PRIVATE ch_contrib::datasketches)
endif ()

target_link_libraries (clickhouse_common_io PRIVATE ch_contrib::lz4)

dbms_target_link_libraries(PRIVATE _boost_context)

if (ENABLE_NLP)
    dbms_target_link_libraries (PUBLIC ch_contrib::stemmer)
    dbms_target_link_libraries (PUBLIC ch_contrib::wnb)
    dbms_target_link_libraries (PUBLIC ch_contrib::lemmagen)
    # FrequencyHolder.cpp embeds the .zst dictionaries via C23 `#embed`.
    set_source_files_properties(Common/FrequencyHolder.cpp PROPERTIES COMPILE_FLAGS -Wno-c23-extensions)
endif()

if (TARGET ch_contrib::ulid)
    dbms_target_link_libraries (PUBLIC ch_contrib::ulid)
endif()

if (TARGET ch_contrib::bzip2)
    target_link_libraries (clickhouse_common_io PRIVATE ch_contrib::bzip2)
endif()

if (TARGET ch_contrib::simdjson)
    dbms_target_link_libraries(PRIVATE ch_contrib::simdjson)
endif()

if (TARGET ch_contrib::rapidjson)
    dbms_target_link_libraries(PRIVATE ch_contrib::rapidjson)
endif()

if (TARGET ch_contrib::python)
    message(STATUS "Python enabled")
    add_subdirectory(CPython)

    target_link_libraries(clickhouse_common_io PUBLIC ch_contrib::python)
    # target_link_libraries(dbms PRIVATE
    # -Wl,--whole-archive
    # ch_contrib::python
    # -Wl,--no-whole-archive
    # -Wl,--export-dynamic
    # )
    dbms_target_link_libraries(PUBLIC cpython_util)
    # if (ENABLE_NUMPY)
    #     find_library(LIBNUMPY libnumpy.so PATHS ${proton_SOURCE_DIR}/contrib/numpy-cmake/numpy/build/numpy/_core)
    #     if (LIBNUMPY)
    #         message(STATUS "numpy found ${LIBNUMPY}")
    #         dbms_target_link_libraries(PUBLIC ${LIBNUMPY})
    #     else()
    #         message(FATAL_ERROR "numpy not found")
    #     endif()
    # else()
    #     message(STATUS "numpy disabled")
    # endif()
endif()

dbms_target_link_libraries(PUBLIC ch_contrib::consistent_hashing)

include ("${proton_SOURCE_DIR}/cmake/add_check.cmake")

if (ENABLE_TESTS)
    macro (grep_gtest_sources BASE_DIR DST_VAR)
        # Cold match files that are not in tests/ directories
        file(GLOB_RECURSE "${DST_VAR}" CONFIGURE_DEPENDS "${BASE_DIR}" "gtest*.cpp")
    endmacro()

    # attach all dbms gtest sources
    grep_gtest_sources("${proton_SOURCE_DIR}/src" dbms_gtest_sources)

    # Exclude V8-dependent tests when JavaScript UDF is disabled or V8 is unavailable
    if (NOT USE_V8)
        list(REMOVE_ITEM dbms_gtest_sources
            ${proton_SOURCE_DIR}/src/AggregateFunctions/tests/gtest_uda.cpp)
    endif()

    proton_add_executable(unit_tests_dbms ${dbms_gtest_sources})

    # proton : starts
    if (SPLIT_DEBUG_SYMBOLS)
        proton_split_debug_symbols_without_install(TARGET unit_tests_dbms DESTINATION_DIR ${CMAKE_CURRENT_BINARY_DIR}/${SPLITTED_DEBUG_SYMBOLS_DIR} BINARY_PATH unit_tests_dbms)
    endif()
    # proton : ends

    # gtest framework has substandard code
    target_compile_options(unit_tests_dbms PRIVATE
        -Wno-sign-compare
    )

    target_link_libraries(unit_tests_dbms PRIVATE
        ch_contrib::gtest_all
        ch_contrib::avrocpp
        ch_contrib::gmock_all
        clickhouse_functions
        clickhouse_table_functions
        clickhouse_aggregate_functions
        clickhouse_parsers
        clickhouse_storages_system
        dbms
        daemon
        clickhouse_common_config
        clickhouse_common_zookeeper
        string_utils
        rocks
        loggers)

    if (TARGET ch_contrib::simdjson)
        target_link_libraries(unit_tests_dbms PRIVATE ch_contrib::simdjson)
    endif()

    if(TARGET ch_contrib::rapidjson)
        target_link_libraries(unit_tests_dbms PRIVATE ch_contrib::rapidjson)
    endif()

    if (TARGET ch_contrib::yaml_cpp)
        target_link_libraries(unit_tests_dbms PRIVATE ch_contrib::yaml_cpp)
    endif()

    add_check(unit_tests_dbms)
endif ()
