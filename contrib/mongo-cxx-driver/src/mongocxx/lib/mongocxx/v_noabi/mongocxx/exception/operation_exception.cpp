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

#include <string>
#include <utility>

#include <bsoncxx/string/to_string.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/private/libbson.hh>
#include <mongocxx/private/libmongoc.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

operation_exception::operation_exception(std::error_code ec,
                                         bsoncxx::v_noabi::document::value&& raw_server_error,
                                         std::string what_arg)
    : exception(ec, what_arg), _raw_server_error{std::move(raw_server_error)} {}

const stdx::optional<bsoncxx::v_noabi::document::value>& operation_exception::raw_server_error()
    const {
    return _raw_server_error;
}

stdx::optional<bsoncxx::v_noabi::document::value>& operation_exception::raw_server_error() {
    return _raw_server_error;
}

bool operation_exception::has_error_label(stdx::string_view label) const {
    if (!_raw_server_error) {
        return false;
    }

    libbson::scoped_bson_t error(_raw_server_error->view());
    std::string label_str{label.data(), label.size()};
    return libmongoc::error_has_label(error.bson(), label_str.c_str());
}

}  // namespace v_noabi
}  // namespace mongocxx
