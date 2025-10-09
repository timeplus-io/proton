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

include_directories(
    ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_INCLUDEDIR}/bsoncxx/v_noabi
    ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_INCLUDEDIR}/mongocxx/v_noabi
)

set(MONGOCXX_EXAMPLES
    aggregate.cpp
    automatic_client_side_field_level_encryption.cpp
    bulk_write.cpp
    causal_consistency.cpp
    change_streams.cpp
    client_session.cpp
    connect.cpp
    create.cpp
    document_validation.cpp
    exception.cpp
    explicit_encryption.cpp
    explicit_encryption_auto_decryption.cpp
    get_values_from_documents.cpp
    gridfs.cpp
    index.cpp
    inserted_id.cpp
    instance_management.cpp
    mongodb.com/aggregation_examples.cpp
    mongodb.com/runcommand_examples.cpp
    mongodb.com/index_examples.cpp
    mongodb.com/documentation_examples.cpp
    mongodb.com/usage_overview.cpp
    query_projection.cpp
    query.cpp
    remove.cpp
    server_side_field_level_encryption_enforcement.cpp
    tailable_cursor.cpp
    tutorial.cpp
    update.cpp
    view_or_value_variant.cpp
    with_transaction.cpp
)

if(MONGOCXX_BUILD_SHARED)
    set(MONGOCXX_LIBRARY_FOR_EXAMPLES mongocxx_shared)
else()
    set(MONGOCXX_LIBRARY_FOR_EXAMPLES mongocxx_static)
endif()

foreach(EXAMPLE_SRC ${MONGOCXX_EXAMPLES})
    get_filename_component(EXAMPLE_TARGET ${EXAMPLE_SRC} NAME_WE)
    add_executable(${EXAMPLE_TARGET} ${EXAMPLE_SRC})
    target_include_directories(${EXAMPLE_TARGET} PUBLIC ${CMAKE_CURRENT_LIST_DIR})
    target_link_libraries(${EXAMPLE_TARGET} ${MONGOCXX_LIBRARY_FOR_EXAMPLES})
    set(MONGOCXX_EXAMPLE_EXECUTABLES ${MONGOCXX_EXAMPLE_EXECUTABLES} ${EXAMPLE_TARGET})
endforeach(EXAMPLE_SRC)

add_executable(pool pool.cpp)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(pool ${MONGOCXX_LIBRARY_FOR_EXAMPLES} Threads::Threads)
set (MONGOCXX_EXAMPLE_EXECUTABLES ${MONGOCXX_EXAMPLE_EXECUTABLES} pool)

set(MONGOCXX_EXAMPLE_EXECUTABLES ${MONGOCXX_EXAMPLE_EXECUTABLES} PARENT_SCOPE)

set_dist_list (examples_mongocxx_DIST
   CMakeLists.txt
   ${MONGOCXX_EXAMPLES}
   pool.cpp
)
