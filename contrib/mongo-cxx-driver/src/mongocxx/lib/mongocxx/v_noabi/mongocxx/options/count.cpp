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

#include <mongocxx/options/count.hpp>
#include <mongocxx/private/read_preference.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace options {

count& count::collation(bsoncxx::v_noabi::document::view_or_value collation) {
    _collation = std::move(collation);
    return *this;
}

count& count::hint(mongocxx::v_noabi::hint index_hint) {
    _hint = std::move(index_hint);
    return *this;
}

count& count::comment(bsoncxx::v_noabi::types::bson_value::view_or_value comment) {
    _comment = std::move(comment);
    return *this;
}

count& count::limit(std::int64_t limit) {
    _limit = limit;
    return *this;
}

count& count::max_time(std::chrono::milliseconds max_time) {
    _max_time = std::move(max_time);
    return *this;
}

count& count::skip(std::int64_t skip) {
    _skip = skip;
    return *this;
}

count& count::read_preference(mongocxx::v_noabi::read_preference rp) {
    _read_preference = std::move(rp);
    return *this;
}

const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& count::collation() const {
    return _collation;
}

const stdx::optional<mongocxx::v_noabi::hint>& count::hint() const {
    return _hint;
}

const stdx::optional<bsoncxx::v_noabi::types::bson_value::view_or_value>& count::comment() const {
    return _comment;
}

const stdx::optional<std::int64_t>& count::limit() const {
    return _limit;
}

const stdx::optional<std::chrono::milliseconds>& count::max_time() const {
    return _max_time;
}

const stdx::optional<std::int64_t>& count::skip() const {
    return _skip;
}

const stdx::optional<read_preference>& count::read_preference() const {
    return _read_preference;
}

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx
