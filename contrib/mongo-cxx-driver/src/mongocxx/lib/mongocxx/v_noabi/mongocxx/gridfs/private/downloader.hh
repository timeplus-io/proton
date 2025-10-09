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

#include <cstdlib>
#include <sstream>

#include <mongocxx/exception/error_code.hpp>
#include <mongocxx/exception/gridfs_exception.hpp>
#include <mongocxx/gridfs/downloader.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace gridfs {

class downloader::impl {
   public:
    impl(stdx::optional<cursor> chunks_param,
         chunks_and_bytes_offset start_param,
         std::int32_t chunk_size_param,
         std::int64_t file_len_param,
         bsoncxx::v_noabi::document::value files_doc_param)
        : files_doc{std::move(files_doc_param)},
          chunk_buffer_len{0},
          chunk_buffer_offset{0},
          chunk_buffer_ptr{nullptr},
          start{start_param},
          chunks{chunks_param ? std::move(chunks_param) : stdx::nullopt},
          chunks_curr{chunks ? stdx::make_optional<cursor::iterator>(chunks->begin())
                             : stdx::nullopt},
          chunks_end{chunks ? stdx::make_optional<cursor::iterator>(chunks->end()) : stdx::nullopt},
          chunks_seen{0},
          chunk_size{chunk_size_param},
          closed{false},
          file_chunk_count{0},
          file_len{file_len_param} {
        if (chunk_size) {
            std::lldiv_t num_chunks_div = std::lldiv(file_len, chunk_size);
            if (num_chunks_div.rem) {
                ++num_chunks_div.quot;
                num_chunks_div.rem = 0;
            }

            if (num_chunks_div.quot > std::numeric_limits<std::int32_t>::max()) {
                std::ostringstream err;
                err << "file has " << num_chunks_div.quot << " chunks, which exceeds maximum of "
                    << std::numeric_limits<std::int32_t>::max();
                throw gridfs_exception{error_code::k_gridfs_file_corrupted, err.str()};
            }

            file_chunk_count = static_cast<std::int32_t>(num_chunks_div.quot);
        }
    }

    // The files document for the file being downloaded.
    bsoncxx::v_noabi::document::value files_doc;

    // The number of bytes in the current chunk.
    std::size_t chunk_buffer_len;

    // The offset from `chunk_buffer_ptr` to the next byte to be read.
    std::size_t chunk_buffer_offset;

    // A pointer to the current chunk being read.
    const uint8_t* chunk_buffer_ptr;

    // An offset from which to start downloading the file.
    const chunks_and_bytes_offset start;

    // A cursor iterating over the chunks documents being read. In the case of a zero-length file,
    // this member does not have a value.
    stdx::optional<cursor> chunks;

    // An iterator to the current chunk document. In the case of a zero-length file, this member
    // does not have a value.
    stdx::optional<cursor::iterator> chunks_curr;

    // An iterator to the end of `chunks`. In the case of a zero-length file, this member does not
    // have a value.
    stdx::optional<cursor::iterator> chunks_end;

    // The number of chunks already downloaded from the server.
    std::int32_t chunks_seen;

    // The size of a chunk in bytes.
    std::int32_t chunk_size;

    // Whether or not the downloader has already been closed.
    bool closed;

    // The total number of chunks in the file.
    std::int32_t file_chunk_count;

    // The total length of the file in bytes.
    std::int64_t file_len;
};

}  // namespace gridfs
}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
