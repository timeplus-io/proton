// Copyright 2021 MongoDB Inc.
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

#pragma once

#include <mongocxx/exception/error_code.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/options/apm.hpp>
#include <mongocxx/options/server_api.hpp>
#include <mongocxx/private/libmongoc.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace options {

using unique_server_api =
    std::unique_ptr<mongoc_server_api_t, decltype(libmongoc::server_api_destroy)>;

BSON_MAYBE_UNUSED
static unique_server_api make_server_api(const server_api& opts) {
    mongoc_server_api_version_t mongoc_api_version;

    // Convert version enum value to std::string then to c_str to create mongoc api version.
    auto result = libmongoc::server_api_version_from_string(
        server_api::version_to_string(opts.get_version()).c_str(), &mongoc_api_version);
    if (!result) {
        throw mongocxx::v_noabi::logic_error{
            mongocxx::v_noabi::error_code::k_invalid_parameter,
            "invalid server API version" + server_api::version_to_string(opts.get_version())};
    }

    auto mongoc_server_api_opts = libmongoc::server_api_new(mongoc_api_version);
    if (!mongoc_server_api_opts) {
        throw mongocxx::v_noabi::logic_error{mongocxx::v_noabi::error_code::k_create_resource_fail,
                                             "could not create server API"};
    }

    if (opts.strict().value_or(false)) {
        libmongoc::server_api_strict(mongoc_server_api_opts, opts.strict().value_or(false));
    }
    if (opts.deprecation_errors().value_or(false)) {
        libmongoc::server_api_deprecation_errors(mongoc_server_api_opts,
                                                 opts.deprecation_errors().value_or(false));
    }

    return {mongoc_server_api_opts, libmongoc::server_api_destroy};
}

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
