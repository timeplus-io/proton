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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <sstream>

#include <bsoncxx/stdx/make_unique.hpp>
#include <bsoncxx/types.hpp>
#include <mongocxx/exception/error_code.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/gridfs/downloader.hpp>
#include <mongocxx/gridfs/private/downloader.hh>
#include <mongocxx/private/numeric_casting.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace gridfs {

downloader::downloader(stdx::optional<cursor> chunks,
                       chunks_and_bytes_offset start,
                       std::int32_t chunk_size,
                       std::int64_t file_len,
                       bsoncxx::v_noabi::document::value files_doc)
    : _impl{stdx::make_unique<impl>(
          std::move(chunks), start, chunk_size, file_len, std::move(files_doc))} {}

downloader::downloader() noexcept = default;
downloader::downloader(downloader&&) noexcept = default;
downloader& downloader::operator=(downloader&&) noexcept = default;
downloader::~downloader() = default;

downloader::operator bool() const noexcept {
    return static_cast<bool>(_impl);
}

std::size_t downloader::read(std::uint8_t* buffer, std::size_t length_requested) {
    if (_get_impl().closed) {
        throw logic_error{error_code::k_gridfs_stream_not_open};
    }

    if (_get_impl().file_len == 0) {
        return 0;
    }

    std::size_t bytes_read = 0;

    while (length_requested > 0 &&
           (_get_impl().chunks_seen != _get_impl().file_chunk_count ||
            _get_impl().chunk_buffer_offset < _get_impl().chunk_buffer_len)) {
        if (_get_impl().chunk_buffer_offset == _get_impl().chunk_buffer_len) {
            fetch_chunk();
        }

        std::size_t length = std::min(
            length_requested, _get_impl().chunk_buffer_len - _get_impl().chunk_buffer_offset);
        std::memcpy(buffer, &_get_impl().chunk_buffer_ptr[_get_impl().chunk_buffer_offset], length);
        buffer = &buffer[length];
        _get_impl().chunk_buffer_offset += length;
        bytes_read += length;
        length_requested -= length;
    }

    return bytes_read;
}

void downloader::close() {
    if (_get_impl().closed) {
        throw logic_error{error_code::k_gridfs_stream_not_open};
    }

    _get_impl().chunks = {};
    _get_impl().closed = true;
}

std::int32_t downloader::chunk_size() const {
    return _get_impl().chunk_size;
}

std::int64_t downloader::file_length() const {
    return _get_impl().file_len;
}

bsoncxx::v_noabi::document::view downloader::files_document() const {
    return _get_impl().files_doc.view();
}

void downloader::fetch_chunk() {
    if (_get_impl().chunks_curr == _get_impl().chunks_end) {
        std::ostringstream err;
        err << "expected file to have " << _get_impl().file_chunk_count
            << " chunk(s), but query to chunks collection only returned " << _get_impl().chunks_seen
            << " chunk(s)";
        throw gridfs_exception{error_code::k_gridfs_file_corrupted, err.str()};
    }
    auto chunks_seen = _get_impl().chunks_seen;

    if (chunks_seen) {
        ++(*_get_impl().chunks_curr);
    } else {
        chunks_seen = _get_impl().start.chunks_offset;
    }

    bsoncxx::v_noabi::document::view chunk_doc = **_get_impl().chunks_curr;

    auto chunk_n_ele = chunk_doc["n"];
    if (!chunk_n_ele || chunk_n_ele.type() != bsoncxx::v_noabi::type::k_int32 ||
        chunk_n_ele.get_int32().value != chunks_seen) {
        std::ostringstream err;
        err << "chunk #" << chunks_seen << ": expected to find field \"n\" with k_int32 type";
        throw gridfs_exception{error_code::k_gridfs_file_corrupted, err.str()};
    }

    if (chunks_seen == std::numeric_limits<std::int32_t>::max()) {
        throw gridfs_exception{error_code::k_gridfs_file_corrupted, "file has too many chunks"};
    }

    auto chunk_data_ele = chunk_doc["data"];
    if (!chunk_data_ele || chunk_data_ele.type() != bsoncxx::v_noabi::type::k_binary) {
        std::ostringstream err;
        err << "chunk #" << chunks_seen << ": expected to find field \"data\" with k_binary type";
        throw gridfs_exception{error_code::k_gridfs_file_corrupted, err.str()};
    }

    auto binary_data = chunk_data_ele.get_binary();

    if (chunks_seen != _get_impl().file_chunk_count - 1) {
        if (binary_data.size != static_cast<std::uint32_t>(_get_impl().chunk_size)) {
            std::ostringstream err;
            err << "chunk #" << chunks_seen << ": expected size of chunk to be "
                << _get_impl().chunk_size << " bytes, but actual size of chunk is "
                << binary_data.size << " bytes";
            throw gridfs_exception{error_code::k_gridfs_file_corrupted, err.str()};
        }
    } else {
        auto expected_size =
            _get_impl().file_len % static_cast<std::int64_t>(_get_impl().chunk_size);

        if (expected_size == 0) {
            expected_size = static_cast<std::int64_t>(_get_impl().chunk_size);
        }

        if (binary_data.size != static_cast<std::uint32_t>(expected_size)) {
            std::ostringstream err;
            err << "chunk #" << chunks_seen << ": expected size of chunk to be " << expected_size
                << " bytes, but actual size of chunk is " << binary_data.size << " bytes";
            throw gridfs_exception{error_code::k_gridfs_file_corrupted, err.str()};
        }
    }

    _get_impl().chunk_buffer_ptr = binary_data.bytes;
    _get_impl().chunk_buffer_len = binary_data.size;

    if (!_get_impl().chunks_seen) {
        if (!int32_to_size_t_safe(_get_impl().start.bytes_offset,
                                  _get_impl().chunk_buffer_offset)) {
            throw gridfs_exception{error_code::k_invalid_parameter,
                                   "expected bytes offset to be in bounds of size_t"};
        }
        _get_impl().chunk_buffer_offset = static_cast<std::size_t>(_get_impl().start.bytes_offset);
        _get_impl().chunks_seen = chunks_seen;
    } else {
        _get_impl().chunk_buffer_offset = 0;
    }

    ++_get_impl().chunks_seen;
}

const downloader::impl& downloader::_get_impl() const {
    if (!_impl) {
        throw logic_error{error_code::k_invalid_gridfs_downloader_object};
    }
    return *_impl;
}

downloader::impl& downloader::_get_impl() {
    auto cthis = const_cast<const downloader*>(this);
    return const_cast<downloader::impl&>(cthis->_get_impl());
}

}  // namespace gridfs
}  // namespace v_noabi
}  // namespace mongocxx
