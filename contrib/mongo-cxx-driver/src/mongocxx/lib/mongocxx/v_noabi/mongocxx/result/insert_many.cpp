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

#include <bsoncxx/types/bson_value/view.hpp>
#include <mongocxx/result/insert_many.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace result {

insert_many::insert_many(result::bulk_write result, bsoncxx::v_noabi::array::value inserted_ids)
    : _result(std::move(result)), _inserted_ids_owned(std::move(inserted_ids)) {
    _buildInsertedIds();
}

insert_many::insert_many(const insert_many& src)
    : _result(src._result), _inserted_ids_owned(src._inserted_ids_owned) {
    _buildInsertedIds();
}

insert_many& insert_many::operator=(const insert_many& src) {
    insert_many tmp(src);
    *this = std::move(tmp);
    return *this;
}

void insert_many::_buildInsertedIds() {
    _inserted_ids.clear();
    std::size_t index = 0;
    for (auto&& ele : _inserted_ids_owned.view()) {
        _inserted_ids.emplace(index++, ele.get_document().value["_id"]);
    }
}

const result::bulk_write& insert_many::result() const {
    return _result;
}

std::int32_t insert_many::inserted_count() const {
    return _result.inserted_count();
}

insert_many::id_map insert_many::inserted_ids() const {
    return _inserted_ids;
}

bool MONGOCXX_CALL operator==(const insert_many& lhs, const insert_many& rhs) {
    if (lhs.result() != rhs.result()) {
        return false;
    } else if (lhs.inserted_ids().size() != rhs.inserted_ids().size()) {
        return false;
    }
    insert_many::id_map::const_iterator litr = lhs._inserted_ids.begin();
    insert_many::id_map::const_iterator ritr = rhs._inserted_ids.begin();
    for (; litr != lhs._inserted_ids.end(); litr++, ritr++) {
        if (litr->first != ritr->first || litr->second.get_oid() != ritr->second.get_oid()) {
            return false;
        }
    }
    return true;
}
bool MONGOCXX_CALL operator!=(const insert_many& lhs, const insert_many& rhs) {
    return !(lhs == rhs);
}

}  // namespace result
}  // namespace v_noabi
}  // namespace mongocxx
