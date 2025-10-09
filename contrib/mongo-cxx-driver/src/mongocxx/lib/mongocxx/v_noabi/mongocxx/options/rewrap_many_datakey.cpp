// Copyright 2023 MongoDB Inc.
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

#include <bsoncxx/types/private/convert.hh>
#include <mongocxx/exception/error_code.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/options/rewrap_many_datakey.hpp>
#include <mongocxx/private/libbson.hh>
#include <mongocxx/private/libmongoc.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace options {

rewrap_many_datakey& rewrap_many_datakey::provider(
    bsoncxx::v_noabi::string::view_or_value provider) {
    _provider = std::move(provider);
    return *this;
}

bsoncxx::v_noabi::string::view_or_value rewrap_many_datakey::provider() const {
    return _provider;
}

rewrap_many_datakey& rewrap_many_datakey::master_key(
    bsoncxx::v_noabi::document::view_or_value master_key) {
    _master_key = std::move(master_key);
    return *this;
}

const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& rewrap_many_datakey::master_key()
    const {
    return _master_key;
}

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx
