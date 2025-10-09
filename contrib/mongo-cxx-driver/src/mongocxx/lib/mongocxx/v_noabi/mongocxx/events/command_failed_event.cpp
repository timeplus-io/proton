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

#include <bsoncxx/oid.hpp>
#include <bsoncxx/private/helpers.hh>
#include <bsoncxx/private/libbson.hh>
#include <bsoncxx/stdx/make_unique.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/events/command_failed_event.hpp>
#include <mongocxx/private/libmongoc.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace events {

command_failed_event::command_failed_event(const void* event) : _failed_event(event) {}

command_failed_event::~command_failed_event() = default;

bsoncxx::v_noabi::document::view command_failed_event::failure() const {
    auto failure = libmongoc::apm_command_failed_get_reply(
        static_cast<const mongoc_apm_command_failed_t*>(_failed_event));
    return {bson_get_data(failure), failure->len};
}

bsoncxx::v_noabi::stdx::string_view command_failed_event::command_name() const {
    return libmongoc::apm_command_failed_get_command_name(
        static_cast<const mongoc_apm_command_failed_t*>(_failed_event));
}

std::int64_t command_failed_event::duration() const {
    return libmongoc::apm_command_failed_get_duration(
        static_cast<const mongoc_apm_command_failed_t*>(_failed_event));
}

std::int64_t command_failed_event::request_id() const {
    return libmongoc::apm_command_failed_get_request_id(
        static_cast<const mongoc_apm_command_failed_t*>(_failed_event));
}

std::int64_t command_failed_event::operation_id() const {
    return libmongoc::apm_command_failed_get_operation_id(
        static_cast<const mongoc_apm_command_failed_t*>(_failed_event));
}

bsoncxx::v_noabi::stdx::optional<bsoncxx::v_noabi::oid> command_failed_event::service_id() const {
    const bson_oid_t* bson_oid = libmongoc::apm_command_failed_get_service_id(
        static_cast<const mongoc_apm_command_failed_t*>(_failed_event));

    if (nullptr == bson_oid)
        return {bsoncxx::v_noabi::stdx::nullopt};

    return {bsoncxx::helpers::make_oid(bson_oid)};
}

bsoncxx::v_noabi::stdx::string_view command_failed_event::host() const {
    return libmongoc::apm_command_failed_get_host(
               static_cast<const mongoc_apm_command_failed_t*>(_failed_event))
        ->host;
}

std::uint16_t command_failed_event::port() const {
    return libmongoc::apm_command_failed_get_host(
               static_cast<const mongoc_apm_command_failed_t*>(_failed_event))
        ->port;
}

}  // namespace events
}  // namespace v_noabi
}  // namespace mongocxx
