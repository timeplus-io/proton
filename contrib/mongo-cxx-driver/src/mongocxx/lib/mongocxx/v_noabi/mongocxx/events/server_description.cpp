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

#include <mongocxx/events/server_description.hpp>
#include <mongocxx/private/libmongoc.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace events {

server_description::server_description(const void* sd) : _sd(sd) {}

server_description::~server_description() = default;

std::uint32_t server_description::id() const {
    return libmongoc::server_description_id(static_cast<const mongoc_server_description_t*>(_sd));
}

std::int64_t server_description::round_trip_time() const {
    return libmongoc::server_description_round_trip_time(
        static_cast<const mongoc_server_description_t*>(_sd));
}

bsoncxx::v_noabi::stdx::string_view server_description::type() const {
    return libmongoc::server_description_type(static_cast<const mongoc_server_description_t*>(_sd));
}

bsoncxx::v_noabi::document::view server_description::is_master() const {
    return hello();
}

bsoncxx::v_noabi::document::view server_description::hello() const {
    auto reply = libmongoc::server_description_hello_response(
        static_cast<const mongoc_server_description_t*>(_sd));
    return {bson_get_data(reply), reply->len};
}

bsoncxx::v_noabi::stdx::string_view server_description::host() const {
    return libmongoc::server_description_host(static_cast<const mongoc_server_description_t*>(_sd))
        ->host;
}

std::uint16_t server_description::port() const {
    return libmongoc::server_description_host(static_cast<const mongoc_server_description_t*>(_sd))
        ->port;
}

}  // namespace events
}  // namespace v_noabi
}  // namespace mongocxx
