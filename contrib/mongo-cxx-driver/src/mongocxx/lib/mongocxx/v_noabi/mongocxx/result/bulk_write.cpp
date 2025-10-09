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

#include <mongocxx/result/bulk_write.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace result {

bulk_write::bulk_write(bsoncxx::v_noabi::document::value raw_response)
    : _response(std::move(raw_response)) {}

std::int32_t bulk_write::inserted_count() const {
    return view()["nInserted"].get_int32();
}

std::int32_t bulk_write::matched_count() const {
    return view()["nMatched"].get_int32();
}

std::int32_t bulk_write::modified_count() const {
    return view()["nModified"].get_int32();
}

std::int32_t bulk_write::deleted_count() const {
    return view()["nRemoved"].get_int32();
}

std::int32_t bulk_write::upserted_count() const {
    return view()["nUpserted"].get_int32();
}

bulk_write::id_map bulk_write::upserted_ids() const {
    id_map upserted_ids;

    if (!view()["upserted"]) {
        return upserted_ids;
    }

    for (auto&& id : view()["upserted"].get_array().value) {
        upserted_ids.emplace(id["index"].get_int32(), id["_id"]);
    }
    return upserted_ids;
}

bsoncxx::v_noabi::document::view bulk_write::view() const {
    return _response.view();
}

bool MONGOCXX_CALL operator==(const bulk_write& lhs, const bulk_write& rhs) {
    return lhs.view() == rhs.view();
}
bool MONGOCXX_CALL operator!=(const bulk_write& lhs, const bulk_write& rhs) {
    return !(lhs == rhs);
}

}  // namespace result
}  // namespace v_noabi
}  // namespace mongocxx
