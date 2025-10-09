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
#include <chrono>
#include <cstring>
#include <limits>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/types.hpp>
#include <mongocxx/exception/error_code.hpp>
#include <mongocxx/exception/gridfs_exception.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/gridfs/private/uploader.hh>
#include <mongocxx/gridfs/uploader.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace {

std::size_t chunks_collection_documents_max_length(std::size_t chunk_size) {
    // 16 * 1000 * 1000 is used instead of 16 * 1024 * 1024 to ensure that the command document sent
    // to the server has space for the other fields.
    return 16 * 1000 * 1000 / chunk_size;
}

}  // namespace

namespace mongocxx {
namespace v_noabi {
namespace gridfs {

uploader::uploader(const client_session* session,
                   bsoncxx::v_noabi::types::bson_value::view id,
                   stdx::string_view filename,
                   collection files,
                   collection chunks,
                   std::int32_t chunk_size,
                   stdx::optional<bsoncxx::v_noabi::document::view_or_value> metadata)
    : _impl{stdx::make_unique<impl>(session,
                                    id,
                                    filename,
                                    files,
                                    chunks,
                                    chunk_size,
                                    metadata
                                        ? stdx::make_optional<bsoncxx::v_noabi::document::value>(
                                              bsoncxx::v_noabi::document::value{metadata->view()})
                                        : stdx::nullopt)} {}

uploader::uploader() noexcept = default;
uploader::uploader(uploader&&) noexcept = default;
uploader& uploader::operator=(uploader&&) noexcept = default;
uploader::~uploader() = default;

uploader::operator bool() const noexcept {
    return static_cast<bool>(_impl);
}

void uploader::write(const std::uint8_t* bytes, std::size_t length) {
    if (_get_impl().closed) {
        throw logic_error{error_code::k_gridfs_stream_not_open};
    }

    while (length > 0) {
        std::size_t buffer_free_space =
            static_cast<std::size_t>(_get_impl().chunk_size) - _get_impl().buffer_off;

        if (buffer_free_space == 0) {
            finish_chunk();
        }

        std::size_t length_written = std::min(length, buffer_free_space);
        std::memcpy(&_get_impl().buffer.get()[_get_impl().buffer_off], bytes, length_written);
        bytes = &bytes[length_written];
        _get_impl().buffer_off += length_written;
        length -= length_written;
    }
}

result::gridfs::upload uploader::close() {
    using bsoncxx::v_noabi::builder::basic::kvp;

    if (_get_impl().closed) {
        throw logic_error{error_code::k_gridfs_stream_not_open};
    }

    _get_impl().closed = true;

    bsoncxx::v_noabi::builder::basic::document file;

    std::int64_t bytes_uploaded = static_cast<std::int64_t>(_get_impl().chunks_written) *
                                  static_cast<std::int64_t>(_get_impl().chunk_size);
    std::int64_t leftover = static_cast<std::int64_t>(_get_impl().buffer_off);

    finish_chunk();
    flush_chunks();

    file.append(kvp("_id", _get_impl().result.id()));
    file.append(kvp("length", bytes_uploaded + leftover));
    file.append(kvp("chunkSize", _get_impl().chunk_size));
    file.append(
        kvp("uploadDate", bsoncxx::v_noabi::types::b_date{std::chrono::system_clock::now()}));
    file.append(kvp("filename", _get_impl().filename));

    if (_get_impl().metadata) {
        file.append(kvp("metadata", *_get_impl().metadata));
    }

    if (_get_impl().session) {
        _get_impl().files.insert_one(*_get_impl().session, file.extract());
    } else {
        _get_impl().files.insert_one(file.extract());
    }

    return _get_impl().result;
}

void uploader::abort() {
    if (_get_impl().closed) {
        throw logic_error{error_code::k_gridfs_stream_not_open};
    }

    _get_impl().closed = true;

    bsoncxx::v_noabi::builder::basic::document filter;
    filter.append(bsoncxx::v_noabi::builder::basic::kvp("files_id", _get_impl().result.id()));

    if (_get_impl().session) {
        _get_impl().chunks.delete_many(*_get_impl().session, filter.extract());
    } else {
        _get_impl().chunks.delete_many(filter.extract());
    }
}

std::int32_t uploader::chunk_size() const {
    return _get_impl().chunk_size;
}

void uploader::finish_chunk() {
    using bsoncxx::v_noabi::builder::basic::kvp;

    if (!_get_impl().buffer_off) {
        return;
    }

    bsoncxx::v_noabi::builder::basic::document chunk;

    std::size_t bytes_in_chunk = _get_impl().buffer_off;

    chunk.append(kvp("files_id", _get_impl().result.id()));
    chunk.append(kvp("n", _get_impl().chunks_written));

    if (_get_impl().chunks_written == std::numeric_limits<std::int32_t>::max()) {
        throw gridfs_exception{error_code::k_gridfs_upload_requires_too_many_chunks};
    }

    ++_get_impl().chunks_written;

    bsoncxx::v_noabi::types::b_binary data{bsoncxx::v_noabi::binary_sub_type::k_binary,
                                           static_cast<std::uint32_t>(bytes_in_chunk),
                                           _get_impl().buffer.get()};

    chunk.append(kvp("data", data));
    _get_impl().chunks_collection_documents.push_back(chunk.extract());

    // To reduce the number of calls to the server, chunks are sent in batches rather than each one
    // being sent immediately upon being written.
    if (_get_impl().chunks_collection_documents.size() >=
        chunks_collection_documents_max_length(static_cast<std::size_t>(_get_impl().chunk_size))) {
        flush_chunks();
    }

    _get_impl().buffer_off = 0;
}

void uploader::flush_chunks() {
    if (_get_impl().chunks_collection_documents.empty()) {
        return;
    }

    if (_get_impl().session) {
        _get_impl().chunks.insert_many(*_get_impl().session,
                                       _get_impl().chunks_collection_documents);
    } else {
        _get_impl().chunks.insert_many(_get_impl().chunks_collection_documents);
    }

    _get_impl().chunks_collection_documents.clear();
}

const uploader::impl& uploader::_get_impl() const {
    if (!_impl) {
        throw logic_error{error_code::k_invalid_gridfs_uploader_object};
    }
    return *_impl;
}

uploader::impl& uploader::_get_impl() {
    auto cthis = const_cast<const uploader*>(this);
    return const_cast<uploader::impl&>(cthis->_get_impl());
}

}  // namespace gridfs
}  // namespace v_noabi
}  // namespace mongocxx
