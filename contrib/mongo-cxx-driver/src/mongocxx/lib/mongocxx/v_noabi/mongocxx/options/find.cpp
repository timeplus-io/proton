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

#include <mongocxx/options/find.hpp>
#include <mongocxx/private/read_preference.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace options {

find& find::allow_disk_use(bool allow_disk_use) {
    _allow_disk_use = allow_disk_use;
    return *this;
}

find& find::allow_partial_results(bool allow_partial) {
    _allow_partial_results = allow_partial;
    return *this;
}

find& find::batch_size(std::int32_t batch_size) {
    _batch_size = batch_size;
    return *this;
}

find& find::collation(bsoncxx::v_noabi::document::view_or_value collation) {
    _collation = std::move(collation);
    return *this;
}

find& find::comment(bsoncxx::v_noabi::string::view_or_value comment) {
    _comment = std::move(comment);
    return *this;
}

find& find::cursor_type(cursor::type cursor_type) {
    _cursor_type = cursor_type;
    return *this;
}

find& find::hint(mongocxx::v_noabi::hint index_hint) {
    _hint = std::move(index_hint);
    return *this;
}

find& find::limit(std::int64_t limit) {
    _limit = limit;
    return *this;
}

find& find::let(bsoncxx::v_noabi::document::view_or_value let) {
    _let = let;
    return *this;
}

find& find::comment_option(bsoncxx::v_noabi::types::bson_value::view_or_value comment) {
    _comment_option = std::move(comment);
    return *this;
}

find& find::max(bsoncxx::v_noabi::document::view_or_value max) {
    _max = std::move(max);
    return *this;
}

find& find::max_await_time(std::chrono::milliseconds max_await_time) {
    _max_await_time = std::move(max_await_time);
    return *this;
}

find& find::max_time(std::chrono::milliseconds max_time) {
    _max_time = std::move(max_time);
    return *this;
}

find& find::min(bsoncxx::v_noabi::document::view_or_value min) {
    _min = std::move(min);
    return *this;
}

find& find::no_cursor_timeout(bool no_cursor_timeout) {
    _no_cursor_timeout = no_cursor_timeout;
    return *this;
}

find& find::projection(bsoncxx::v_noabi::document::view_or_value projection) {
    _projection = std::move(projection);
    return *this;
}

find& find::read_preference(mongocxx::v_noabi::read_preference rp) {
    _read_preference = std::move(rp);
    return *this;
}

find& find::return_key(bool return_key) {
    _return_key = return_key;
    return *this;
}

find& find::show_record_id(bool show_record_id) {
    _show_record_id = show_record_id;
    return *this;
}

find& find::skip(std::int64_t skip) {
    _skip = skip;
    return *this;
}

find& find::sort(bsoncxx::v_noabi::document::view_or_value ordering) {
    _ordering = std::move(ordering);
    return *this;
}

const stdx::optional<bool>& find::allow_disk_use() const {
    return _allow_disk_use;
}

const stdx::optional<bool>& find::allow_partial_results() const {
    return _allow_partial_results;
}

const stdx::optional<std::int32_t>& find::batch_size() const {
    return _batch_size;
}

const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& find::collation() const {
    return _collation;
}

const stdx::optional<bsoncxx::v_noabi::string::view_or_value>& find::comment() const {
    return _comment;
}

const stdx::optional<cursor::type>& find::cursor_type() const {
    return _cursor_type;
}

const stdx::optional<mongocxx::v_noabi::hint>& find::hint() const {
    return _hint;
}

const stdx::optional<std::int64_t>& find::limit() const {
    return _limit;
}

const stdx::optional<bsoncxx::v_noabi::document::view_or_value> find::let() const {
    return _let;
}

const stdx::optional<bsoncxx::v_noabi::types::bson_value::view_or_value>& find::comment_option()
    const {
    return _comment_option;
}

const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& find::max() const {
    return _max;
}

const stdx::optional<std::chrono::milliseconds>& find::max_await_time() const {
    return _max_await_time;
}

const stdx::optional<std::chrono::milliseconds>& find::max_time() const {
    return _max_time;
}

const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& find::min() const {
    return _min;
}

const stdx::optional<bool>& find::no_cursor_timeout() const {
    return _no_cursor_timeout;
}

const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& find::projection() const {
    return _projection;
}

const stdx::optional<bool>& find::return_key() const {
    return _return_key;
}

const stdx::optional<bool>& find::show_record_id() const {
    return _show_record_id;
}

const stdx::optional<std::int64_t>& find::skip() const {
    return _skip;
}

const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& find::sort() const {
    return _ordering;
}

const stdx::optional<mongocxx::v_noabi::read_preference>& find::read_preference() const {
    return _read_preference;
}

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx
