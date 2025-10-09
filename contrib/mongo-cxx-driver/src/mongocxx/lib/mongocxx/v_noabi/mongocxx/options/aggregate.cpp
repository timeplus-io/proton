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

#include <bsoncxx/builder/basic/document.hpp>
#include <mongocxx/options/aggregate.hpp>
#include <mongocxx/private/append_aggregate_options.hh>
#include <mongocxx/private/read_preference.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace options {

using bsoncxx::v_noabi::builder::basic::kvp;

aggregate& aggregate::allow_disk_use(bool allow_disk_use) {
    _allow_disk_use = allow_disk_use;
    return *this;
}

void aggregate::append(bsoncxx::v_noabi::builder::basic::document& builder) const {
    append_aggregate_options(builder, *this);
}

aggregate& aggregate::collation(bsoncxx::v_noabi::document::view_or_value collation) {
    _collation = std::move(collation);
    return *this;
}

aggregate& aggregate::let(bsoncxx::v_noabi::document::view_or_value let) {
    _let = std::move(let);
    return *this;
}

aggregate& aggregate::batch_size(std::int32_t batch_size) {
    _batch_size = batch_size;
    return *this;
}

aggregate& aggregate::max_time(std::chrono::milliseconds max_time) {
    _max_time = std::move(max_time);
    return *this;
}

aggregate& aggregate::read_preference(mongocxx::v_noabi::read_preference rp) {
    _read_preference = std::move(rp);
    return *this;
}

aggregate& aggregate::bypass_document_validation(bool bypass_document_validation) {
    _bypass_document_validation = bypass_document_validation;
    return *this;
}

aggregate& aggregate::hint(mongocxx::v_noabi::hint index_hint) {
    _hint = std::move(index_hint);
    return *this;
}

aggregate& aggregate::read_concern(mongocxx::v_noabi::read_concern read_concern) {
    _read_concern = std::move(read_concern);
    return *this;
}

aggregate& aggregate::write_concern(mongocxx::v_noabi::write_concern write_concern) {
    _write_concern = std::move(write_concern);
    return *this;
}

aggregate& aggregate::comment(bsoncxx::v_noabi::types::bson_value::view_or_value comment) {
    _comment = std::move(comment);
    return *this;
}

const stdx::optional<bool>& aggregate::allow_disk_use() const {
    return _allow_disk_use;
}

const stdx::optional<std::int32_t>& aggregate::batch_size() const {
    return _batch_size;
}

const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& aggregate::collation() const {
    return _collation;
}

const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& aggregate::let() const {
    return _let;
}

const stdx::optional<std::chrono::milliseconds>& aggregate::max_time() const {
    return _max_time;
}

const stdx::optional<mongocxx::v_noabi::read_preference>& aggregate::read_preference() const {
    return _read_preference;
}

const stdx::optional<bool>& aggregate::bypass_document_validation() const {
    return _bypass_document_validation;
}

const stdx::optional<mongocxx::v_noabi::hint>& aggregate::hint() const {
    return _hint;
}

const stdx::optional<mongocxx::v_noabi::read_concern>& aggregate::read_concern() const {
    return _read_concern;
}

const stdx::optional<mongocxx::v_noabi::write_concern>& aggregate::write_concern() const {
    return _write_concern;
}

const stdx::optional<bsoncxx::v_noabi::types::bson_value::view_or_value>& aggregate::comment()
    const {
    return _comment;
}

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx
