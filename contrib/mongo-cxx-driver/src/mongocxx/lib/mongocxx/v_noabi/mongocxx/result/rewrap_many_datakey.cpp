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

#include <mongocxx/bulk_write.hpp>
#include <mongocxx/private/libbson.hh>
#include <mongocxx/result/rewrap_many_datakey.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace result {

rewrap_many_datakey::rewrap_many_datakey(
    mongocxx::v_noabi::result::bulk_write bulk_write_result_doc) {
    _result = std::move(bulk_write_result_doc);
}

const bsoncxx::v_noabi::stdx::optional<mongocxx::v_noabi::result::bulk_write>&
rewrap_many_datakey::result() {
    return _result;
}

}  // namespace result
}  // namespace v_noabi
}  // namespace mongocxx
