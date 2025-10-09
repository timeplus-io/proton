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

#include <bsoncxx/array/view_or_value.hpp>
#include <mongocxx/model/update_one.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace model {

update_one::update_one(bsoncxx::v_noabi::document::view_or_value filter,
                       bsoncxx::v_noabi::document::view_or_value update)
    : _filter(std::move(filter)), _update(std::move(update)) {}

update_one::update_one(bsoncxx::v_noabi::document::view_or_value filter, const pipeline& update)
    : _filter(std::move(filter)), _update(bsoncxx::v_noabi::document::value(update.view_array())) {}

update_one::update_one(bsoncxx::v_noabi::document::view_or_value filter,
                       std::initializer_list<_empty_doc_tag>)
    : _filter(std::move(filter)), _update() {}

const bsoncxx::v_noabi::document::view_or_value& update_one::filter() const {
    return _filter;
}

const bsoncxx::v_noabi::document::view_or_value& update_one::update() const {
    return _update;
}

update_one& update_one::collation(bsoncxx::v_noabi::document::view_or_value collation) {
    _collation = collation;
    return *this;
}

const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& update_one::collation() const {
    return _collation;
}

update_one& update_one::hint(mongocxx::v_noabi::hint index_hint) {
    _hint = std::move(index_hint);
    return *this;
}

const stdx::optional<mongocxx::v_noabi::hint>& update_one::hint() const {
    return _hint;
}

update_one& update_one::upsert(bool upsert) {
    _upsert = upsert;
    return *this;
}

const stdx::optional<bool>& update_one::upsert() const {
    return _upsert;
}

update_one& update_one::array_filters(bsoncxx::v_noabi::array::view_or_value array_filters) {
    _array_filters = std::move(array_filters);
    return *this;
}

const stdx::optional<bsoncxx::v_noabi::array::view_or_value>& update_one::array_filters() const {
    return _array_filters;
}

}  // namespace model
}  // namespace v_noabi
}  // namespace mongocxx
