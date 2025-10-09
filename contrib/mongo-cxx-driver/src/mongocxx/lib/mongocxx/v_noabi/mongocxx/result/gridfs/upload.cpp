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

#include <bsoncxx/builder/basic/array.hpp>
#include <mongocxx/result/gridfs/upload.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace result {
namespace gridfs {

upload::upload(bsoncxx::v_noabi::types::bson_value::view id)
    : _id_owned(bsoncxx::v_noabi::builder::basic::make_array(id)),
      _id(_id_owned.view()[0].get_value()) {}

const bsoncxx::v_noabi::types::bson_value::view& upload::id() const {
    return _id;
}

bool MONGOCXX_CALL operator==(const upload& lhs, const upload& rhs) {
    return lhs.id() == rhs.id();
}
bool MONGOCXX_CALL operator!=(const upload& lhs, const upload& rhs) {
    return !(lhs == rhs);
}

}  // namespace gridfs
}  // namespace result
}  // namespace v_noabi
}  // namespace mongocxx
