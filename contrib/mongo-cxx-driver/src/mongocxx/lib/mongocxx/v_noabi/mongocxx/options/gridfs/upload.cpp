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

#include <bsoncxx/view_or_value.hpp>
#include <mongocxx/options/gridfs/upload.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace options {
namespace gridfs {

upload& upload::chunk_size_bytes(std::int32_t chunk_size_bytes) {
    _chunk_size_bytes = chunk_size_bytes;
    return *this;
}

const stdx::optional<std::int32_t>& upload::chunk_size_bytes() const {
    return _chunk_size_bytes;
}

upload& upload::metadata(bsoncxx::v_noabi::document::view_or_value metadata) {
    _metadata = std::move(metadata);
    return *this;
}

const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& upload::metadata() const {
    return _metadata;
}

}  // namespace gridfs
}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx
