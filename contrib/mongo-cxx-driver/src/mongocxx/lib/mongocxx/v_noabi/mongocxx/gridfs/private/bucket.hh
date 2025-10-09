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

#pragma once

#include <cstddef>
#include <string>

#include <mongocxx/collection.hpp>
#include <mongocxx/gridfs/bucket.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace gridfs {

class bucket::impl {
   public:
    impl(std::string bucket_name,
         std::int32_t default_chunk_size_bytes,
         collection chunks,
         collection files)
        : bucket_name{std::move(bucket_name)},
          default_chunk_size_bytes{default_chunk_size_bytes},
          chunks{std::move(chunks)},
          files{std::move(files)},
          indexes_created{false} {}

    // The name of the bucket.
    std::string bucket_name;

    // The default size of the chunks.
    std::int32_t default_chunk_size_bytes;

    // The collection holding the chunks.
    collection chunks;

    // The collection holding the files.
    collection files;

    // Whether the required indexes have been created.
    bool indexes_created;
};

}  // namespace gridfs
}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
