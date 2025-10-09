// Copyright 2018-present MongoDB Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <mongocxx/events/server_changed_event.hpp>
#include <mongocxx/private/libmongoc.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace events {

server_changed_event::server_changed_event(const void* event) : _event(event) {}

server_changed_event::~server_changed_event() = default;

bsoncxx::v_noabi::stdx::string_view server_changed_event::host() const {
    return libmongoc::apm_server_changed_get_host(
               static_cast<const mongoc_apm_server_changed_t*>(_event))
        ->host;
}

std::uint16_t server_changed_event::port() const {
    return libmongoc::apm_server_changed_get_host(
               static_cast<const mongoc_apm_server_changed_t*>(_event))
        ->port;
}

const bsoncxx::v_noabi::oid server_changed_event::topology_id() const {
    bson_oid_t boid;
    libmongoc::apm_server_changed_get_topology_id(
        static_cast<const mongoc_apm_server_changed_t*>(_event), &boid);

    return bsoncxx::v_noabi::oid{reinterpret_cast<const char*>(boid.bytes), sizeof(boid.bytes)};
}

const server_description server_changed_event::previous_description() const {
    return server_description{libmongoc::apm_server_changed_get_previous_description(
        static_cast<const mongoc_apm_server_changed_t*>(_event))};
}

const server_description server_changed_event::new_description() const {
    return server_description{libmongoc::apm_server_changed_get_new_description(
        static_cast<const mongoc_apm_server_changed_t*>(_event))};
}

}  // namespace events
}  // namespace v_noabi
}  // namespace mongocxx
