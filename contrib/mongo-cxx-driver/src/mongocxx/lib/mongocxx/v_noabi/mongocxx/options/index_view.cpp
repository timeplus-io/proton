// Copyright 2017 MongoDB Inc.
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
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/types.hpp>
#include <mongocxx/options/index_view.hpp>

#include <mongocxx/config/private/prelude.hh>

using bsoncxx::v_noabi::builder::basic::kvp;
using bsoncxx::v_noabi::builder::basic::make_document;

namespace mongocxx {
namespace v_noabi {
namespace options {

index_view::index_view() : _max_time(), _write_concern(), _commit_quorum() {}

const bsoncxx::v_noabi::stdx::optional<mongocxx::v_noabi::write_concern>&
index_view::write_concern() const {
    return _write_concern;
}

const bsoncxx::v_noabi::stdx::optional<std::chrono::milliseconds>& index_view::max_time() const {
    return _max_time;
}

const stdx::optional<bsoncxx::v_noabi::document::value> index_view::commit_quorum() const {
    return _commit_quorum;
}

index_view& index_view::max_time(std::chrono::milliseconds max_time) {
    _max_time = max_time;
    return *this;
}

index_view& index_view::write_concern(mongocxx::v_noabi::write_concern write_concern) {
    _write_concern = std::move(write_concern);
    return *this;
}

index_view& index_view::commit_quorum(int commit_quorum) {
    _commit_quorum = stdx::make_optional<bsoncxx::v_noabi::document::value>(
        make_document(kvp("commitQuorum", bsoncxx::v_noabi::types::b_int32{commit_quorum})));
    return *this;
}

index_view& index_view::commit_quorum(std::string commit_quorum) {
    _commit_quorum = stdx::make_optional<bsoncxx::v_noabi::document::value>(
        make_document(kvp("commitQuorum", commit_quorum)));
    return *this;
}

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx
