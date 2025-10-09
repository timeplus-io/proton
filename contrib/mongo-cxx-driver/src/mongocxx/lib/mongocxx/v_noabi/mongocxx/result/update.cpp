// Copyright 2014 MongoDB Inc.
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

#include <mongocxx/result/update.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace result {

update::update(result::bulk_write result) : _result(std::move(result)) {}

const result::bulk_write& update::result() const {
    return _result;
}
std::int32_t update::matched_count() const {
    return _result.matched_count();
}

std::int32_t update::modified_count() const {
    return _result.modified_count();
}

std::int32_t update::upserted_count() const {
    return _result.upserted_count();
}

stdx::optional<bsoncxx::v_noabi::document::element> update::upserted_id() const {
    if (_result.upserted_ids().size() == 0) {
        return stdx::nullopt;
    }
    return _result.upserted_ids()[0];
}

bool MONGOCXX_CALL operator==(const update& lhs, const update& rhs) {
    return lhs.result() == rhs.result();
}
bool MONGOCXX_CALL operator!=(const update& lhs, const update& rhs) {
    return !(lhs == rhs);
}

}  // namespace result
}  // namespace v_noabi
}  // namespace mongocxx
