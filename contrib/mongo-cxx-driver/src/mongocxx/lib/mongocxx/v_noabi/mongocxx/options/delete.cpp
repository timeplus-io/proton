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

#include <mongocxx/hint.hpp>
#include <mongocxx/options/delete.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace options {

delete_options& delete_options::collation(bsoncxx::v_noabi::document::view_or_value collation) {
    _collation = std::move(collation);
    return *this;
}

delete_options& delete_options::write_concern(mongocxx::v_noabi::write_concern wc) {
    _write_concern = std::move(wc);
    return *this;
}

delete_options& delete_options::hint(mongocxx::v_noabi::hint index_hint) {
    _hint = std::move(index_hint);
    return *this;
}

const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& delete_options::collation() const {
    return _collation;
}

const stdx::optional<mongocxx::v_noabi::write_concern>& delete_options::write_concern() const {
    return _write_concern;
}

const stdx::optional<mongocxx::v_noabi::hint>& delete_options::hint() const {
    return _hint;
}

delete_options& delete_options::let(bsoncxx::v_noabi::document::view_or_value let) {
    _let = let;
    return *this;
}

const stdx::optional<bsoncxx::v_noabi::document::view_or_value> delete_options::let() const {
    return _let;
}

delete_options& delete_options::comment(
    bsoncxx::v_noabi::types::bson_value::view_or_value comment) {
    _comment = std::move(comment);
    return *this;
}

const stdx::optional<bsoncxx::v_noabi::types::bson_value::view_or_value> delete_options::comment()
    const {
    return _comment;
}

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx
