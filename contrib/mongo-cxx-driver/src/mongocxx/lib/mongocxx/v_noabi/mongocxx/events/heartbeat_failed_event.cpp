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

#include <mongocxx/events/heartbeat_failed_event.hpp>
#include <mongocxx/private/libmongoc.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace events {

heartbeat_failed_event::heartbeat_failed_event(const void* event) : _failed_event(event) {}

heartbeat_failed_event::~heartbeat_failed_event() = default;

std::string heartbeat_failed_event::message() const {
    bson_error_t error;
    auto casted = static_cast<const mongoc_apm_server_heartbeat_failed_t*>(_failed_event);
    libmongoc::apm_server_heartbeat_failed_get_error(casted, &error);
    return error.message;
}

std::int64_t heartbeat_failed_event::duration() const {
    auto casted = static_cast<const mongoc_apm_server_heartbeat_failed_t*>(_failed_event);
    return libmongoc::apm_server_heartbeat_failed_get_duration(casted);
}

bsoncxx::v_noabi::stdx::string_view heartbeat_failed_event::host() const {
    auto casted = static_cast<const mongoc_apm_server_heartbeat_failed_t*>(_failed_event);
    return libmongoc::apm_server_heartbeat_failed_get_host(casted)->host;
}

std::uint16_t heartbeat_failed_event::port() const {
    auto casted = static_cast<const mongoc_apm_server_heartbeat_failed_t*>(_failed_event);
    return libmongoc::apm_server_heartbeat_failed_get_host(casted)->port;
}

bool heartbeat_failed_event::awaited() const {
    auto casted = static_cast<const mongoc_apm_server_heartbeat_failed_t*>(_failed_event);
    return libmongoc::apm_server_heartbeat_failed_get_awaited(casted);
}

}  // namespace events
}  // namespace v_noabi
}  // namespace mongocxx
