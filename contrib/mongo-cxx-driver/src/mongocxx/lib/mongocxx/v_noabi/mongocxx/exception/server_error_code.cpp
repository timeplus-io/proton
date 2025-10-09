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

#include <mongocxx/exception/server_error_code.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

namespace {

//
// An error_category for codes returned by the server.
//
class server_error_category final : public std::error_category {
   public:
    const char* name() const noexcept override {
        return "mongodb";
    }

    std::string message(int) const noexcept override {
        return "generic server error";
    }
};

}  // namespace

const std::error_category& MONGOCXX_CALL server_error_category() {
    static const class server_error_category category {};
    return category;
}

}  // namespace v_noabi
}  // namespace mongocxx
