// Copyright 2018-present MongoDB Inc.
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

#include <mongocxx/options/estimated_document_count.hpp>
#include <mongocxx/private/read_preference.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace options {

estimated_document_count& estimated_document_count::max_time(std::chrono::milliseconds max_time) {
    _max_time = std::move(max_time);
    return *this;
}

estimated_document_count& estimated_document_count::read_preference(
    mongocxx::v_noabi::read_preference rp) {
    _read_preference = std::move(rp);
    return *this;
}

estimated_document_count& estimated_document_count::comment(
    bsoncxx::v_noabi::types::bson_value::view_or_value comment) {
    _comment = std::move(comment);
    return *this;
}

const stdx::optional<std::chrono::milliseconds>& estimated_document_count::max_time() const {
    return _max_time;
}

const stdx::optional<bsoncxx::v_noabi::types::bson_value::view_or_value>&
estimated_document_count::comment() const {
    return _comment;
}

const stdx::optional<read_preference>& estimated_document_count::read_preference() const {
    return _read_preference;
}

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx
