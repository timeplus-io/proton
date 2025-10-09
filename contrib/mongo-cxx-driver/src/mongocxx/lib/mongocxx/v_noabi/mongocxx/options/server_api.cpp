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

#include <system_error>

#include <bsoncxx/stdx/string_view.hpp>
#include <mongocxx/exception/error_code.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/options/server_api.hpp>
#include <mongocxx/private/libmongoc.hh>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace options {

std::string server_api::version_to_string(server_api::version version) {
    switch (version) {
        case server_api::version::k_version_1:
            return "1";
        default:
            throw mongocxx::v_noabi::logic_error{mongocxx::v_noabi::error_code::k_invalid_parameter,
                                                 "invalid server API version"};
    }
}

server_api::version server_api::version_from_string(stdx::string_view version) {
    if (!version.compare("1")) {
        return server_api::version::k_version_1;
    }
    throw mongocxx::v_noabi::logic_error{mongocxx::v_noabi::error_code::k_invalid_parameter,
                                         "invalid server API version"};
}

server_api::server_api(server_api::version version) : _version(std::move(version)) {}

server_api& server_api::strict(bool strict) {
    _strict = strict;
    return *this;
}

const stdx::optional<bool>& server_api::strict() const {
    return _strict;
}

server_api& server_api::deprecation_errors(bool deprecation_errors) {
    _deprecation_errors = deprecation_errors;
    return *this;
}

const stdx::optional<bool>& server_api::deprecation_errors() const {
    return _deprecation_errors;
}

server_api::version server_api::get_version() const {
    return _version;
}

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx
