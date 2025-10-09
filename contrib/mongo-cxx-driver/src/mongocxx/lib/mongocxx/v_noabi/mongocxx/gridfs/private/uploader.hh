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

#include <string>
#include <vector>

#include <bsoncxx/stdx/make_unique.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <mongocxx/gridfs/uploader.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace gridfs {

class uploader::impl {
   public:
    impl(const client_session* session,
         result::gridfs::upload result,
         stdx::string_view filename,
         collection files,
         collection chunks,
         std::int32_t chunk_size,
         stdx::optional<bsoncxx::v_noabi::document::value> metadata)
        : session{session},
          buffer{stdx::make_unique<std::uint8_t[]>(static_cast<size_t>(chunk_size))},
          buffer_off{0},
          chunks{std::move(chunks)},
          chunk_size{chunk_size},
          chunks_written{0},
          closed{false},
          filename{bsoncxx::v_noabi::string::to_string(filename)},
          files{std::move(files)},
          metadata{std::move(metadata)},
          result{std::move(result)} {}

    // Client session to use for upload operations.
    const client_session* session;

    // Bytes that have been written for the current chunk.
    std::unique_ptr<std::uint8_t[]> buffer;

    // The offset from `buffer` to the next byte to be written.
    std::size_t buffer_off;

    // The collection to which the chunks will be written.
    collection chunks;

    // Chunks that have been fully written but not yet uploaded to the server.
    std::vector<bsoncxx::v_noabi::document::value> chunks_collection_documents;

    // The size of a chunk in bytes.
    std::int32_t chunk_size;

    // The number of chunks fully written so far.
    std::int32_t chunks_written;

    // Whether or not the uploader has already been closed.
    bool closed;

    // The name of the file to be written.
    std::string filename;

    // The collection to which the files document will be written.
    collection files;

    // User-specified metadata for the file.
    stdx::optional<bsoncxx::v_noabi::document::value> metadata;

    // Contains the id of the file being written.
    result::gridfs::upload result;
};

}  // namespace gridfs
}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
