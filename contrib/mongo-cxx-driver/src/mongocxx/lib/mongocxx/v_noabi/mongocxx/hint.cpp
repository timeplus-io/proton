// Copyright 2015 MongoDB Inc.
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

#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/builder/basic/sub_document.hpp>
#include <bsoncxx/builder/concatenate.hpp>
#include <mongocxx/hint.hpp>

#include <mongocxx/config/private/prelude.hh>

using bsoncxx::v_noabi::builder::concatenate;
using bsoncxx::v_noabi::builder::basic::kvp;
using bsoncxx::v_noabi::builder::basic::sub_document;

namespace mongocxx {
namespace v_noabi {

hint::hint(bsoncxx::v_noabi::document::view_or_value index) : _index_doc(std::move(index)) {}

hint::hint(bsoncxx::v_noabi::string::view_or_value index) : _index_string(std::move(index)) {}

bsoncxx::v_noabi::types::bson_value::view hint::to_value() const {
    if (_index_doc) {
        return bsoncxx::v_noabi::types::bson_value::view{
            bsoncxx::v_noabi::types::b_document{_index_doc->view()}};
    }

    return bsoncxx::v_noabi::types::bson_value::view{
        bsoncxx::v_noabi::types::b_string{*_index_string}};
}

bool MONGOCXX_CALL operator==(const hint& index_hint, std::string index) {
    return ((index_hint._index_string) && (*(index_hint._index_string) == index));
}

bool MONGOCXX_CALL operator==(std::string index, const hint& index_hint) {
    return index_hint == index;
}

bool MONGOCXX_CALL operator!=(const hint& index_hint, std::string index) {
    return !(index_hint == index);
}

bool MONGOCXX_CALL operator!=(std::string index, const hint& index_hint) {
    return !(index_hint == index);
}

bool MONGOCXX_CALL operator==(const hint& index_hint, bsoncxx::v_noabi::document::view index) {
    return index_hint._index_doc && index_hint._index_doc->view() == index;
}

bool MONGOCXX_CALL operator==(bsoncxx::v_noabi::document::view index, const hint& index_hint) {
    return index_hint == index;
}

bool MONGOCXX_CALL operator!=(const hint& index_hint, bsoncxx::v_noabi::document::view index) {
    return !(index_hint == index);
}

bool MONGOCXX_CALL operator!=(bsoncxx::v_noabi::document::view index, const hint& index_hint) {
    return !(index_hint == index);
}

}  // namespace v_noabi
}  // namespace mongocxx
