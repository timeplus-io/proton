# Copyright 2016 MongoDB Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if(NOT BUILD_TESTING)
    set_property(DIRECTORY PROPERTY EXCLUDE_FROM_ALL TRUE)
endif()

if(MONGOCXX_BUILD_SHARED)
    mongocxx_add_library(mongocxx_mocked "mongocxx-mocked" SHARED)
else()
    mongocxx_add_library(mongocxx_mocked "mongocxx-mocked" STATIC)
endif()

target_link_libraries(mongocxx_mocked PUBLIC bsoncxx_testing)
target_compile_definitions(mongocxx_mocked PUBLIC MONGOCXX_TESTING)

if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    target_compile_options(mongocxx_mocked PRIVATE /bigobj)
endif()

set(spec_test_common
    spec/operation.cpp
    spec/unified_tests/entity.cpp
    spec/unified_tests/assert.cpp
    spec/monitoring.cpp
    spec/util.cpp
)

add_executable(test_driver
    bulk_write.cpp
    change_streams.cpp
    client_helpers.cpp
    client_session.cpp
    client_side_encryption.cpp
    client.cpp
    collection_mocked.cpp
    collection.cpp
    conversions.cpp
    database.cpp
    gridfs/bucket.cpp
    gridfs/downloader.cpp
    gridfs/uploader.cpp
    hint.cpp
    index_view.cpp
    model/delete_many.cpp
    model/delete_one.cpp
    model/insert_one.cpp
    model/replace_one.cpp
    model/update_many.cpp
    model/update_one.cpp
    options/aggregate.cpp
    options/bulk_write.cpp
    options/client_session.cpp
    options/count.cpp
    options/create_collection.cpp
    options/delete.cpp
    options/distinct.cpp
    options/find_one_and_delete.cpp
    options/find_one_and_replace.cpp
    options/find_one_and_update.cpp
    options/find.cpp
    options/gridfs/bucket.cpp
    options/gridfs/upload.cpp
    options/index.cpp
    options/insert.cpp
    options/pool.cpp
    options/replace.cpp
    options/update.cpp
    pool.cpp
    private/numeric_casting.cpp
    private/scoped_bson_t.cpp
    private/write_concern.cpp
    read_concern.cpp
    read_preference.cpp
    result/bulk_write.cpp
    result/delete.cpp
    result/gridfs/upload.cpp
    result/insert_one.cpp
    result/replace_one.cpp
    result/update.cpp
    sdam-monitoring.cpp
    search_index_view.cpp
    spec/initial_dns_seedlist_discovery.cpp
    spec/monitoring.cpp
    spec/monitoring.hh
    spec/unified_tests/assert.cpp
    spec/unified_tests/entity.cpp
    spec/uri_options.cpp
    transactions.cpp
    uri.cpp
    validation_criteria.cpp
    write_concern.cpp
)

add_executable(test_logging
    logging.cpp
)

add_executable(test_instance
    instance.cpp
)

add_executable(test_crud_specs
    spec/crud.cpp
    ${spec_test_common}
    client_helpers.cpp
)

add_executable(test_gridfs_specs
    spec/gridfs.cpp
    ${spec_test_common}
    client_helpers.cpp
)

add_executable(test_client_side_encryption_specs
    spec/client_side_encryption.cpp
    ${spec_test_common}
    client_helpers.cpp
)

add_executable(test_command_monitoring_specs
    spec/command_monitoring.cpp
    ${spec_test_common}
    client_helpers.cpp
)

add_executable(test_transactions_specs
    spec/transactions.cpp
    ${spec_test_common}
    client_helpers.cpp
)

add_executable(test_retryable_reads_specs
    spec/retryable-reads.cpp
    ${spec_test_common}
    client_helpers.cpp
)

add_executable(test_read_write_concern_specs
    spec/read_write_concern.cpp
    ${spec_test_common}
    client_helpers.cpp
)

add_executable(test_mongohouse_specs
    spec/mongohouse.cpp
    ${spec_test_common}
    client_helpers.cpp
)

add_executable(test_unified_format_spec
    spec/unified_tests/operations.cpp
    spec/unified_tests/runner.cpp
    ${spec_test_common}
    client_helpers.cpp
)

add_executable(test_versioned_api
    versioned_api.cpp
    ${spec_test_common}
    client_helpers.cpp
)

# Common target properties for test executables.
add_library(mongocxx_test_properties INTERFACE)
target_link_libraries(mongocxx_test_properties INTERFACE mongocxx_mocked ${libmongoc_target})
target_include_directories(mongocxx_test_properties INTERFACE

    # Allow `#include <mongoc/mongoc.h>`
    ${libmongoc_include_directories}

    # Allow `#include <mongocxx/test/...>` and `#include <third_party/...>`.
    ${CMAKE_CURRENT_SOURCE_DIR}/../..
)
add_library(mongocxx::test_properties ALIAS mongocxx_test_properties)

# Avoid redundant recompilation of catch/main.cpp.
add_library(mongocxx_test_properties_with_main STATIC EXCLUDE_FROM_ALL ${THIRD_PARTY_SOURCE_DIR}/catch/main.cpp)
target_link_libraries(mongocxx_test_properties_with_main PUBLIC mongocxx::test_properties)
add_library(mongocxx::test_properties_with_main ALIAS mongocxx_test_properties_with_main)

set_property(
    TARGET
        test_driver
        test_logging
        test_instance
        test_client_side_encryption_specs
        test_crud_specs
        test_gridfs_specs
        test_command_monitoring_specs
        test_transactions_specs
        test_retryable_reads_specs
        test_read_write_concern_specs
        test_mongohouse_specs
        test_unified_format_spec
        test_versioned_api
    APPEND PROPERTY LINK_LIBRARIES
        mongocxx::test_properties_with_main
)

target_compile_definitions(test_driver PRIVATE ${libmongoc_definitions})
target_compile_definitions(test_logging PRIVATE ${libmongoc_definitions})
target_compile_definitions(test_instance PRIVATE ${libmongoc_definitions})

if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    target_compile_options(test_driver PRIVATE /bigobj)
endif()

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    target_compile_options(test_driver PRIVATE -Wno-maybe-uninitialized)
    target_compile_options(test_retryable_reads_specs PRIVATE -Wno-maybe-uninitialized)
endif()

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(test_driver PRIVATE Threads::Threads)

add_test(NAME driver COMMAND test_driver)
add_test(NAME logging COMMAND test_logging)
add_test(NAME instance COMMAND test_instance)
add_test(NAME crud_specs COMMAND test_crud_specs)
add_test(NAME gridfs_specs COMMAND test_gridfs_specs)
add_test(NAME client_side_encryption_specs COMMAND test_client_side_encryption_specs)
add_test(NAME command_monitoring_specs COMMAND test_command_monitoring_specs)
add_test(NAME transactions_specs COMMAND test_transactions_specs)
add_test(NAME retryable_reads_spec COMMAND test_retryable_reads_specs)
add_test(NAME read_write_concern_specs COMMAND test_read_write_concern_specs)
add_test(NAME unified_format_spec COMMAND test_unified_format_spec)
add_test(NAME versioned_api COMMAND test_versioned_api)

# Adding this as a test will run it as part of the RUN_TESTS command in MSVC.
# Do not add, since we only test mongohouse on Linux.
if(0)
    add_test(mongohouse_specs NAME test_mongohouse_specs COMMAND)
endif()

set_property(TEST crud_specs APPEND PROPERTY ENVIRONMENT
    "CRUD_LEGACY_TESTS_PATH=${DATA_SOURCE_DIR}/crud/legacy"
)

set_property(TEST gridfs_specs APPEND PROPERTY ENVIRONMENT
    "GRIDFS_TESTS_PATH=${DATA_SOURCE_DIR}/gridfs"
)

set_property(TEST client_side_encryption_specs APPEND PROPERTY ENVIRONMENT
    "CLIENT_SIDE_ENCRYPTION_LEGACY_TESTS_PATH=${DATA_SOURCE_DIR}/client_side_encryption/legacy"
)

set_property(TEST driver APPEND PROPERTY ENVIRONMENT
    "CLIENT_SIDE_ENCRYPTION_TESTS_PATH=${DATA_SOURCE_DIR}/client_side_encryption"
    "URI_OPTIONS_TESTS_PATH=${DATA_SOURCE_DIR}/uri-options"
)

set_property(TEST command_monitoring_specs APPEND PROPERTY ENVIRONMENT
    "COMMAND_MONITORING_TESTS_PATH=${DATA_SOURCE_DIR}/command-monitoring"
)

set_property(TEST transactions_specs APPEND PROPERTY ENVIRONMENT
    "TRANSACTIONS_LEGACY_TESTS_PATH=${DATA_SOURCE_DIR}/transactions/legacy"
    "WITH_TRANSACTION_TESTS_PATH=${DATA_SOURCE_DIR}/with_transaction"
)

set_property(TEST retryable_reads_spec APPEND PROPERTY ENVIRONMENT
    "RETRYABLE_READS_LEGACY_TESTS_PATH=${DATA_SOURCE_DIR}/retryable-reads/legacy"
)

set_property(TEST read_write_concern_specs APPEND PROPERTY ENVIRONMENT
    "READ_WRITE_CONCERN_OPERATION_TESTS_PATH=${DATA_SOURCE_DIR}/read-write-concern/operation"
)

set_property(TEST unified_format_spec APPEND PROPERTY ENVIRONMENT
    "CHANGE_STREAMS_UNIFIED_TESTS_PATH=${DATA_SOURCE_DIR}/change-streams/unified"
    "CLIENT_SIDE_ENCRYPTION_UNIFIED_TESTS_PATH=${DATA_SOURCE_DIR}/client_side_encryption/unified"
    "COLLECTION_MANAGEMENT_TESTS_PATH=${DATA_SOURCE_DIR}/collection-management"
    "CRUD_UNIFIED_TESTS_PATH=${DATA_SOURCE_DIR}/crud/unified"
    "INDEX_MANAGEMENT_TESTS_PATH=${DATA_SOURCE_DIR}/index-management"
    "RETRYABLE_READS_UNIFIED_TESTS_PATH=${DATA_SOURCE_DIR}/retryable-reads/unified/"
    "RETRYABLE_WRITES_UNIFIED_TESTS_PATH=${DATA_SOURCE_DIR}/retryable-writes/unified/"
    "SESSION_UNIFIED_TESTS_PATH=${DATA_SOURCE_DIR}/sessions/unified/"
    "TRANSACTIONS_UNIFIED_TESTS_PATH=${DATA_SOURCE_DIR}/transactions/unified"
    "UNIFIED_FORMAT_TESTS_PATH=${DATA_SOURCE_DIR}/unified-format"
    "VERSIONED_API_TESTS_PATH=${DATA_SOURCE_DIR}/versioned-api"
)

if(MONGOCXX_ENABLE_SLOW_TESTS)
    set_property(TEST driver APPEND PROPERTY ENVIRONMENT "MONGOCXX_ENABLE_SLOW_TESTS=1")
endif()

# Generate test to ensure macro guards behave properly.
if(ENABLE_MACRO_GUARD_TESTS)
    include(MacroGuardTest)

    add_macro_guard_test(
        PROJECT_NAME mongocxx
        PROJECT_TEST_PROPERTIES_TARGET mongocxx::test_properties
        GUARDED_MACROS
            MONGOCXX_INLINE # compiler.hpp
            MONGOCXX_CALL
            MONGOCXX_INLINE_NAMESPACE_BEGIN # config.hpp (generated by CMake)
            MONGOCXX_INLINE_NAMESPACE_END
            MONGOCXX_VERSION_STRING # version.hpp (generated by CMake)
            MONGOCXX_VERSION_EXTRA
            MONGOCXX_VERSION_MAJOR
            MONGOCXX_VERSION_MINOR
            MONGOCXX_VERSION_PATCH
            MONGOCXX_API_H # export.hpp (generated by CMake)
            MONGOCXX_API
            MONGOCXX_PRIVATE
            MONGOCXX_DEPRECATED
            MONGOCXX_DEPRECATED_EXPORT
            MONGOCXX_DEPRECATED_NO_EXPORT
            DEFINE_NO_DEPRECATED
            MONGOCXX_NO_DEPRECATED
            MONGOCXX_UNREACHABLE # prelude.hpp
            MONGOCXX_ENABLE_SSL # config.hh (generated by CMake)
            MONGOCXX_COMPILER_ID
            MONGOCXX_COMPILER_VERSION
        INCLUDE_PATTERNS
            "include/*.hpp" # Public headers.
            "lib/*.hh" # Private headers.
            "test/*.hh" # Test headers.
        EXCLUDE_REGEXES
            "(include|lib)/mongocxx/v.*/config/.*" # Config headers.
            ".*_symbols.hh" # Symbol headers.
    )
endif()

set_dist_list(src_mongocxx_test_DIST
    CMakeLists.txt
    bulk_write.cpp
    change_streams.cpp
    client_helpers.cpp
    client_helpers.hh
    client_session.cpp
    client_side_encryption.cpp
    client.cpp
    collection_mocked.cpp
    collection.cpp
    conversions.cpp
    database.cpp
    gridfs/bucket.cpp
    gridfs/downloader.cpp
    gridfs/uploader.cpp
    hint.cpp
    index_view.cpp
    instance.cpp
    logging.cpp
    model/delete_many.cpp
    model/delete_one.cpp
    model/insert_one.cpp
    model/replace_one.cpp
    model/update_many.cpp
    model/update_one.cpp
    options/aggregate.cpp
    options/bulk_write.cpp
    options/client_session.cpp
    options/count.cpp
    options/create_collection.cpp
    options/delete.cpp
    options/distinct.cpp
    options/find_one_and_delete.cpp
    options/find_one_and_replace.cpp
    options/find_one_and_update.cpp
    options/find.cpp
    options/gridfs/bucket.cpp
    options/gridfs/upload.cpp
    options/index.cpp
    options/insert.cpp
    options/pool.cpp
    options/replace.cpp
    options/update.cpp
    pool.cpp
    private/numeric_casting.cpp
    private/scoped_bson_t.cpp
    private/write_concern.cpp
    read_concern.cpp
    read_preference.cpp
    result/bulk_write.cpp
    result/delete.cpp
    result/gridfs/upload.cpp
    result/insert_one.cpp
    result/replace_one.cpp
    result/update.cpp
    sdam-monitoring.cpp
    search_index_view.cpp
    spec/client_side_encryption.cpp
    spec/command_monitoring.cpp
    spec/crud.cpp
    spec/gridfs.cpp
    spec/initial_dns_seedlist_discovery.cpp
    spec/mongohouse.cpp
    spec/monitoring.cpp
    spec/monitoring.hh
    spec/operation.cpp
    spec/operation.hh
    spec/read_write_concern.cpp
    spec/retryable-reads.cpp
    spec/transactions.cpp
    spec/unified_tests/assert.cpp
    spec/unified_tests/assert.hh
    spec/unified_tests/entity.cpp
    spec/unified_tests/entity.hh
    spec/unified_tests/operations.cpp
    spec/unified_tests/operations.hh
    spec/unified_tests/runner.cpp
    spec/uri_options.cpp
    spec/util.cpp
    spec/util.hh
    test_macro_guards.cpp.in
    transactions.cpp
    uri.cpp
    validation_criteria.cpp
    versioned_api.cpp
    write_concern.cpp
)
