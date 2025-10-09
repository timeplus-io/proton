# Copyright 2023 MongoDB Inc.
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

list(APPEND mongocxx_sources
    mongocxx/v_noabi/mongocxx/bulk_write.cpp
    mongocxx/v_noabi/mongocxx/change_stream.cpp
    mongocxx/v_noabi/mongocxx/client.cpp
    mongocxx/v_noabi/mongocxx/client_encryption.cpp
    mongocxx/v_noabi/mongocxx/client_session.cpp
    mongocxx/v_noabi/mongocxx/collection.cpp
    mongocxx/v_noabi/mongocxx/cursor.cpp
    mongocxx/v_noabi/mongocxx/database.cpp
    mongocxx/v_noabi/mongocxx/events/command_failed_event.cpp
    mongocxx/v_noabi/mongocxx/events/command_started_event.cpp
    mongocxx/v_noabi/mongocxx/events/command_succeeded_event.cpp
    mongocxx/v_noabi/mongocxx/events/heartbeat_failed_event.cpp
    mongocxx/v_noabi/mongocxx/events/heartbeat_started_event.cpp
    mongocxx/v_noabi/mongocxx/events/heartbeat_succeeded_event.cpp
    mongocxx/v_noabi/mongocxx/events/server_changed_event.cpp
    mongocxx/v_noabi/mongocxx/events/server_closed_event.cpp
    mongocxx/v_noabi/mongocxx/events/server_description.cpp
    mongocxx/v_noabi/mongocxx/events/server_opening_event.cpp
    mongocxx/v_noabi/mongocxx/events/topology_changed_event.cpp
    mongocxx/v_noabi/mongocxx/events/topology_closed_event.cpp
    mongocxx/v_noabi/mongocxx/events/topology_description.cpp
    mongocxx/v_noabi/mongocxx/events/topology_opening_event.cpp
    mongocxx/v_noabi/mongocxx/exception/error_code.cpp
    mongocxx/v_noabi/mongocxx/exception/operation_exception.cpp
    mongocxx/v_noabi/mongocxx/exception/server_error_code.cpp
    mongocxx/v_noabi/mongocxx/gridfs/bucket.cpp
    mongocxx/v_noabi/mongocxx/gridfs/downloader.cpp
    mongocxx/v_noabi/mongocxx/gridfs/uploader.cpp
    mongocxx/v_noabi/mongocxx/hint.cpp
    mongocxx/v_noabi/mongocxx/index_model.cpp
    mongocxx/v_noabi/mongocxx/index_view.cpp
    mongocxx/v_noabi/mongocxx/instance.cpp
    mongocxx/v_noabi/mongocxx/logger.cpp
    mongocxx/v_noabi/mongocxx/model/delete_many.cpp
    mongocxx/v_noabi/mongocxx/model/delete_one.cpp
    mongocxx/v_noabi/mongocxx/model/insert_one.cpp
    mongocxx/v_noabi/mongocxx/model/replace_one.cpp
    mongocxx/v_noabi/mongocxx/model/update_many.cpp
    mongocxx/v_noabi/mongocxx/model/update_one.cpp
    mongocxx/v_noabi/mongocxx/model/write.cpp
    mongocxx/v_noabi/mongocxx/options/aggregate.cpp
    mongocxx/v_noabi/mongocxx/options/apm.cpp
    mongocxx/v_noabi/mongocxx/options/auto_encryption.cpp
    mongocxx/v_noabi/mongocxx/options/bulk_write.cpp
    mongocxx/v_noabi/mongocxx/options/change_stream.cpp
    mongocxx/v_noabi/mongocxx/options/client.cpp
    mongocxx/v_noabi/mongocxx/options/client_encryption.cpp
    mongocxx/v_noabi/mongocxx/options/client_session.cpp
    mongocxx/v_noabi/mongocxx/options/count.cpp
    mongocxx/v_noabi/mongocxx/options/create_collection.cpp
    mongocxx/v_noabi/mongocxx/options/data_key.cpp
    mongocxx/v_noabi/mongocxx/options/delete.cpp
    mongocxx/v_noabi/mongocxx/options/distinct.cpp
    mongocxx/v_noabi/mongocxx/options/encrypt.cpp
    mongocxx/v_noabi/mongocxx/options/estimated_document_count.cpp
    mongocxx/v_noabi/mongocxx/options/find.cpp
    mongocxx/v_noabi/mongocxx/options/find_one_and_delete.cpp
    mongocxx/v_noabi/mongocxx/options/find_one_and_replace.cpp
    mongocxx/v_noabi/mongocxx/options/find_one_and_update.cpp
    mongocxx/v_noabi/mongocxx/options/gridfs/bucket.cpp
    mongocxx/v_noabi/mongocxx/options/gridfs/upload.cpp
    mongocxx/v_noabi/mongocxx/options/index.cpp
    mongocxx/v_noabi/mongocxx/options/index_view.cpp
    mongocxx/v_noabi/mongocxx/options/insert.cpp
    mongocxx/v_noabi/mongocxx/options/pool.cpp
    mongocxx/v_noabi/mongocxx/options/range.cpp
    mongocxx/v_noabi/mongocxx/options/replace.cpp
    mongocxx/v_noabi/mongocxx/options/rewrap_many_datakey.cpp
    mongocxx/v_noabi/mongocxx/options/server_api.cpp
    mongocxx/v_noabi/mongocxx/options/tls.cpp
    mongocxx/v_noabi/mongocxx/options/transaction.cpp
    mongocxx/v_noabi/mongocxx/options/update.cpp
    mongocxx/v_noabi/mongocxx/pipeline.cpp
    mongocxx/v_noabi/mongocxx/pool.cpp
    mongocxx/v_noabi/mongocxx/private/conversions.cpp
    mongocxx/v_noabi/mongocxx/private/libbson.cpp
    mongocxx/v_noabi/mongocxx/private/libmongoc.cpp
    mongocxx/v_noabi/mongocxx/private/numeric_casting.cpp
    mongocxx/v_noabi/mongocxx/read_concern.cpp
    mongocxx/v_noabi/mongocxx/read_preference.cpp
    mongocxx/v_noabi/mongocxx/result/bulk_write.cpp
    mongocxx/v_noabi/mongocxx/result/delete.cpp
    mongocxx/v_noabi/mongocxx/result/gridfs/upload.cpp
    mongocxx/v_noabi/mongocxx/result/insert_many.cpp
    mongocxx/v_noabi/mongocxx/result/insert_one.cpp
    mongocxx/v_noabi/mongocxx/result/replace_one.cpp
    mongocxx/v_noabi/mongocxx/result/rewrap_many_datakey.cpp
    mongocxx/v_noabi/mongocxx/result/update.cpp
    mongocxx/v_noabi/mongocxx/search_index_model.cpp
    mongocxx/v_noabi/mongocxx/search_index_view.cpp
    mongocxx/v_noabi/mongocxx/uri.cpp
    mongocxx/v_noabi/mongocxx/validation_criteria.cpp
    mongocxx/v_noabi/mongocxx/write_concern.cpp
)
list(TRANSFORM mongocxx_sources PREPEND "${CMAKE_CURRENT_SOURCE_DIR}/")
set(mongocxx_sources "${mongocxx_sources}" PARENT_SCOPE)

# Generate private headers.
if(1)
    if(DEFINED CMAKE_CXX_COMPILER_ID)
        set(MONGOCXX_COMPILER_ID "${CMAKE_CXX_COMPILER_ID}")
    else()
        set(MONGOCXX_COMPILER_ID "Unknown")
    endif()

    if(DEFINED CMAKE_CXX_COMPILER_VERSION)
        set(MONGOCXX_COMPILER_VERSION "${CMAKE_CXX_COMPILER_VERSION}")
    else()
        set(MONGOCXX_COMPILER_VERSION "Unknown")
    endif()

    configure_file(
        mongocxx/v_noabi/mongocxx/config/private/config.hh.in
        mongocxx/v_noabi/mongocxx/config/private/config.hh
    )
endif()

# Generate and install public headers.
if(1)
    configure_file(
        mongocxx/v_noabi/mongocxx/config/version.hpp.in
        mongocxx/v_noabi/mongocxx/config/version.hpp
    )

    configure_file(
        mongocxx/v_noabi/mongocxx/config/config.hpp.in
        mongocxx/v_noabi/mongocxx/config/config.hpp
    )

    install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/mongocxx/v_noabi/mongocxx/config/config.hpp
        ${CMAKE_CURRENT_BINARY_DIR}/mongocxx/v_noabi/mongocxx/config/version.hpp
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/mongocxx/v_noabi/mongocxx/config
        COMPONENT dev
    )
endif()

set_dist_list(src_mongocxx_lib_DIST
    CMakeLists.txt
    mongocxx/v_noabi/mongocxx/bulk_write.cpp
    mongocxx/v_noabi/mongocxx/change_stream.cpp
    mongocxx/v_noabi/mongocxx/client_encryption.cpp
    mongocxx/v_noabi/mongocxx/client_session.cpp
    mongocxx/v_noabi/mongocxx/client.cpp
    mongocxx/v_noabi/mongocxx/collection.cpp
    mongocxx/v_noabi/mongocxx/config/config.hpp.in
    mongocxx/v_noabi/mongocxx/config/private/config.hh.in
    mongocxx/v_noabi/mongocxx/config/private/postlude.hh
    mongocxx/v_noabi/mongocxx/config/private/prelude.hh
    mongocxx/v_noabi/mongocxx/config/version.hpp.in
    mongocxx/v_noabi/mongocxx/cursor.cpp
    mongocxx/v_noabi/mongocxx/database.cpp
    mongocxx/v_noabi/mongocxx/events/command_failed_event.cpp
    mongocxx/v_noabi/mongocxx/events/command_started_event.cpp
    mongocxx/v_noabi/mongocxx/events/command_succeeded_event.cpp
    mongocxx/v_noabi/mongocxx/events/heartbeat_failed_event.cpp
    mongocxx/v_noabi/mongocxx/events/heartbeat_started_event.cpp
    mongocxx/v_noabi/mongocxx/events/heartbeat_succeeded_event.cpp
    mongocxx/v_noabi/mongocxx/events/server_changed_event.cpp
    mongocxx/v_noabi/mongocxx/events/server_closed_event.cpp
    mongocxx/v_noabi/mongocxx/events/server_description.cpp
    mongocxx/v_noabi/mongocxx/events/server_opening_event.cpp
    mongocxx/v_noabi/mongocxx/events/topology_changed_event.cpp
    mongocxx/v_noabi/mongocxx/events/topology_closed_event.cpp
    mongocxx/v_noabi/mongocxx/events/topology_description.cpp
    mongocxx/v_noabi/mongocxx/events/topology_opening_event.cpp
    mongocxx/v_noabi/mongocxx/exception/error_code.cpp
    mongocxx/v_noabi/mongocxx/exception/operation_exception.cpp
    mongocxx/v_noabi/mongocxx/exception/private/mongoc_error.hh
    mongocxx/v_noabi/mongocxx/exception/server_error_code.cpp
    mongocxx/v_noabi/mongocxx/gridfs/bucket.cpp
    mongocxx/v_noabi/mongocxx/gridfs/downloader.cpp
    mongocxx/v_noabi/mongocxx/gridfs/private/bucket.hh
    mongocxx/v_noabi/mongocxx/gridfs/private/downloader.hh
    mongocxx/v_noabi/mongocxx/gridfs/private/uploader.hh
    mongocxx/v_noabi/mongocxx/gridfs/uploader.cpp
    mongocxx/v_noabi/mongocxx/hint.cpp
    mongocxx/v_noabi/mongocxx/index_model.cpp
    mongocxx/v_noabi/mongocxx/index_view.cpp
    mongocxx/v_noabi/mongocxx/instance.cpp
    mongocxx/v_noabi/mongocxx/logger.cpp
    mongocxx/v_noabi/mongocxx/model/delete_many.cpp
    mongocxx/v_noabi/mongocxx/model/delete_one.cpp
    mongocxx/v_noabi/mongocxx/model/insert_one.cpp
    mongocxx/v_noabi/mongocxx/model/replace_one.cpp
    mongocxx/v_noabi/mongocxx/model/update_many.cpp
    mongocxx/v_noabi/mongocxx/model/update_one.cpp
    mongocxx/v_noabi/mongocxx/model/write.cpp
    mongocxx/v_noabi/mongocxx/options/aggregate.cpp
    mongocxx/v_noabi/mongocxx/options/apm.cpp
    mongocxx/v_noabi/mongocxx/options/auto_encryption.cpp
    mongocxx/v_noabi/mongocxx/options/bulk_write.cpp
    mongocxx/v_noabi/mongocxx/options/change_stream.cpp
    mongocxx/v_noabi/mongocxx/options/client_encryption.cpp
    mongocxx/v_noabi/mongocxx/options/client_session.cpp
    mongocxx/v_noabi/mongocxx/options/client.cpp
    mongocxx/v_noabi/mongocxx/options/count.cpp
    mongocxx/v_noabi/mongocxx/options/create_collection.cpp
    mongocxx/v_noabi/mongocxx/options/data_key.cpp
    mongocxx/v_noabi/mongocxx/options/delete.cpp
    mongocxx/v_noabi/mongocxx/options/distinct.cpp
    mongocxx/v_noabi/mongocxx/options/encrypt.cpp
    mongocxx/v_noabi/mongocxx/options/estimated_document_count.cpp
    mongocxx/v_noabi/mongocxx/options/find_one_and_delete.cpp
    mongocxx/v_noabi/mongocxx/options/find_one_and_replace.cpp
    mongocxx/v_noabi/mongocxx/options/find_one_and_update.cpp
    mongocxx/v_noabi/mongocxx/options/find.cpp
    mongocxx/v_noabi/mongocxx/options/gridfs/bucket.cpp
    mongocxx/v_noabi/mongocxx/options/gridfs/upload.cpp
    mongocxx/v_noabi/mongocxx/options/index_view.cpp
    mongocxx/v_noabi/mongocxx/options/index.cpp
    mongocxx/v_noabi/mongocxx/options/insert.cpp
    mongocxx/v_noabi/mongocxx/options/pool.cpp
    mongocxx/v_noabi/mongocxx/options/private/apm.hh
    mongocxx/v_noabi/mongocxx/options/private/server_api.hh
    mongocxx/v_noabi/mongocxx/options/private/ssl.hh
    mongocxx/v_noabi/mongocxx/options/private/transaction.hh
    mongocxx/v_noabi/mongocxx/options/range.cpp
    mongocxx/v_noabi/mongocxx/options/replace.cpp
    mongocxx/v_noabi/mongocxx/options/rewrap_many_datakey.cpp
    mongocxx/v_noabi/mongocxx/options/server_api.cpp
    mongocxx/v_noabi/mongocxx/options/tls.cpp
    mongocxx/v_noabi/mongocxx/options/transaction.cpp
    mongocxx/v_noabi/mongocxx/options/update.cpp
    mongocxx/v_noabi/mongocxx/pipeline.cpp
    mongocxx/v_noabi/mongocxx/pool.cpp
    mongocxx/v_noabi/mongocxx/private/append_aggregate_options.hh
    mongocxx/v_noabi/mongocxx/private/bulk_write.hh
    mongocxx/v_noabi/mongocxx/private/change_stream.hh
    mongocxx/v_noabi/mongocxx/private/client_encryption.hh
    mongocxx/v_noabi/mongocxx/private/client_session.hh
    mongocxx/v_noabi/mongocxx/private/client.hh
    mongocxx/v_noabi/mongocxx/private/collection.hh
    mongocxx/v_noabi/mongocxx/private/conversions.cpp
    mongocxx/v_noabi/mongocxx/private/conversions.hh
    mongocxx/v_noabi/mongocxx/private/cursor.hh
    mongocxx/v_noabi/mongocxx/private/database.hh
    mongocxx/v_noabi/mongocxx/private/index_view.hh
    mongocxx/v_noabi/mongocxx/private/libbson.cpp
    mongocxx/v_noabi/mongocxx/private/libbson.hh
    mongocxx/v_noabi/mongocxx/private/libmongoc_symbols.hh
    mongocxx/v_noabi/mongocxx/private/libmongoc.cpp
    mongocxx/v_noabi/mongocxx/private/libmongoc.hh
    mongocxx/v_noabi/mongocxx/private/numeric_casting.cpp
    mongocxx/v_noabi/mongocxx/private/numeric_casting.hh
    mongocxx/v_noabi/mongocxx/private/pipeline.hh
    mongocxx/v_noabi/mongocxx/private/pool.hh
    mongocxx/v_noabi/mongocxx/private/read_concern.hh
    mongocxx/v_noabi/mongocxx/private/read_preference.hh
    mongocxx/v_noabi/mongocxx/private/search_index_model.hh
    mongocxx/v_noabi/mongocxx/private/search_index_view.hh
    mongocxx/v_noabi/mongocxx/private/uri.hh
    mongocxx/v_noabi/mongocxx/private/write_concern.hh
    mongocxx/v_noabi/mongocxx/read_concern.cpp
    mongocxx/v_noabi/mongocxx/read_preference.cpp
    mongocxx/v_noabi/mongocxx/result/bulk_write.cpp
    mongocxx/v_noabi/mongocxx/result/delete.cpp
    mongocxx/v_noabi/mongocxx/result/gridfs/upload.cpp
    mongocxx/v_noabi/mongocxx/result/insert_many.cpp
    mongocxx/v_noabi/mongocxx/result/insert_one.cpp
    mongocxx/v_noabi/mongocxx/result/replace_one.cpp
    mongocxx/v_noabi/mongocxx/result/rewrap_many_datakey.cpp
    mongocxx/v_noabi/mongocxx/result/update.cpp
    mongocxx/v_noabi/mongocxx/search_index_model.cpp
    mongocxx/v_noabi/mongocxx/search_index_view.cpp
    mongocxx/v_noabi/mongocxx/test_util/export_for_testing.hh
    mongocxx/v_noabi/mongocxx/test_util/mock.hh
    mongocxx/v_noabi/mongocxx/uri.cpp
    mongocxx/v_noabi/mongocxx/validation_criteria.cpp
    mongocxx/v_noabi/mongocxx/write_concern.cpp
)
