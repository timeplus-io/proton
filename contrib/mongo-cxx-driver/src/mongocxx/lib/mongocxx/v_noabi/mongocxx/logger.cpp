// Copyright 2015 MongoDB Inc.
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

#include <mongocxx/logger.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

stdx::string_view MONGOCXX_CALL to_string(log_level level) {
    switch (level) {
        case log_level::k_error:
            return "error";
        case log_level::k_critical:
            return "critical";
        case log_level::k_warning:
            return "warning";
        case log_level::k_message:
            return "message";
        case log_level::k_info:
            return "info";
        case log_level::k_debug:
            return "debug";
        case log_level::k_trace:
            return "trace";
        default:
            return "unknown";
    }
}

logger::logger() = default;
logger::~logger() = default;

}  // namespace v_noabi
}  // namespace mongocxx
