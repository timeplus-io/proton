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

#pragma once

#include <bsoncxx/document/value.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/exception/server_error_code.hpp>
#include <mongocxx/private/libmongoc.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

inline std::error_code make_error_code(int code, int) {
    // Domain is ignored. We simply issue the code.
    return {code, server_error_category()};
}

inline std::error_code make_error_code(const ::bson_error_t& error) {
    return make_error_code(static_cast<int>(error.code), static_cast<int>(error.domain));
}

inline void set_bson_error_message(bson_error_t* error, const char* msg) {
    bson_strncpy(error->message,
                 msg,
                 std::min(strlen(msg) + 1, static_cast<size_t>(BSON_ERROR_BUFFER_SIZE)));
}

inline void make_bson_error(bson_error_t* error, const operation_exception& e) {
    // No way to get the domain back out of the exception, so zero out.
    error->code = static_cast<uint32_t>(e.code().value());
    error->domain = 0;
    set_bson_error_message(error, e.what());
}

inline void make_generic_bson_error(bson_error_t* error) {
    // CDRIVER-3524 Zero these out since we don't have them.
    error->code = 0;
    error->domain = 0;
    set_bson_error_message(error, "unknown error");
}

template <typename exception_type>
void throw_exception(const ::bson_error_t& error) {
    throw exception_type{make_error_code(error), error.message};
}

template <typename exception_type>
void throw_exception(bsoncxx::v_noabi::document::value raw_server_error,
                     const ::bson_error_t& error) {
    throw exception_type{make_error_code(error), std::move(raw_server_error), error.message};
}

}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
