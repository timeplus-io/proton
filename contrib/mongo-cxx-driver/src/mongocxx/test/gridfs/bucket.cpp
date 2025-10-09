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
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <numeric>
#include <sstream>
#include <vector>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/oid.hpp>
#include <bsoncxx/test/catch.hh>
#include <bsoncxx/types.hpp>
#include <bsoncxx/types/bson_value/view.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/database.hpp>
#include <mongocxx/exception/gridfs_exception.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/gridfs/bucket.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/find.hpp>
#include <mongocxx/options/gridfs/upload.hpp>
#include <mongocxx/options/index.hpp>
#include <mongocxx/test/client_helpers.hh>
#include <mongocxx/uri.hpp>

namespace {
using namespace mongocxx;

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

// Downloads the file `id` from the gridfs collections in `db` specified by `bucket_name` and
// verifies that it has file_name `expected_file_name`, contents `expected_contents`, and chunk size
// `expected_chunk_size`.
void validate_gridfs_file(database db,
                          std::string bucket_name,
                          bsoncxx::types::bson_value::view id,
                          std::string expected_file_name,
                          std::vector<std::uint8_t> expected_contents,
                          std::int32_t expected_chunk_size) {
    auto files_doc = db[bucket_name + ".files"].find_one(make_document(kvp("_id", id)));
    REQUIRE(files_doc);

    REQUIRE(files_doc->view()["_id"].get_value() == id);
    REQUIRE(static_cast<std::size_t>(files_doc->view()["length"].get_int64().value) ==
            expected_contents.size());
    REQUIRE(files_doc->view()["chunkSize"].get_int32().value == expected_chunk_size);
    REQUIRE(files_doc->view()["filename"].get_string().value ==
            stdx::string_view(expected_file_name));

    // md5 is deprecated in GridFS, we don't include it:
    // https://github.com/mongodb/specifications/blob/master/source/gridfs/gridfs-spec.rst
    REQUIRE(!files_doc->view()["md5"]);

    std::int32_t index = 0;

    for (auto&& chunks_doc :
         db[bucket_name + ".chunks"].find(make_document(kvp("files_id", id)),
                                          options::find{}.sort(make_document(kvp("n", 1))))) {
        REQUIRE(chunks_doc["n"].get_int32().value == index);

        auto data = chunks_doc["data"].get_binary();
        REQUIRE(data.sub_type == bsoncxx::binary_sub_type::k_binary);
        REQUIRE(static_cast<std::size_t>(data.size) ==
                std::min(static_cast<std::size_t>(expected_chunk_size),
                         expected_contents.size() -
                             static_cast<std::size_t>(expected_chunk_size * index)));

        std::vector<std::uint8_t> expected_bytes_slice{
            expected_contents.data() + index * expected_chunk_size,
            expected_contents.data() + index * expected_chunk_size + data.size};
        std::vector<std::uint8_t> actual_bytes{data.bytes, data.bytes + data.size};

        REQUIRE(expected_bytes_slice == actual_bytes);
        ++index;
    }

    auto num_chunks_div =
        std::lldiv(static_cast<std::int64_t>(expected_contents.size()), expected_chunk_size);

    if (num_chunks_div.rem) {
        ++num_chunks_div.quot;
    }

    REQUIRE(num_chunks_div.quot <= std::numeric_limits<std::int32_t>::max());
    REQUIRE(index == static_cast<std::int32_t>(num_chunks_div.quot));
}

// Downloads the file `id` from the gridfs collections in `db` specified by `bucket_name` and
// verifies that it has file_name `expected_file_name`, and chunk size `expected_chunk_size`. The
// contents of each chunk are verified by passing them into `validate_chunk`.
void validate_gridfs_file(
    database db,
    std::string bucket_name,
    bsoncxx::types::bson_value::view id,
    std::string expected_file_name,
    std::function<void(const bsoncxx::types::b_binary&, std::size_t)> validate_chunk,
    std::int32_t expected_chunk_size,
    std::int64_t expected_length) {
    auto files_doc = db[bucket_name + ".files"].find_one(make_document(kvp("_id", id)));
    REQUIRE(files_doc);

    REQUIRE(files_doc->view()["_id"].get_oid() == id.get_oid());
    REQUIRE(files_doc->view()["length"].get_int64().value == expected_length);
    REQUIRE(files_doc->view()["chunkSize"].get_int32().value == expected_chunk_size);
    REQUIRE(files_doc->view()["filename"].get_string().value ==
            stdx::string_view(expected_file_name));

    std::size_t i = 0;

    for (auto&& chunks_doc :
         db["fs.chunks"].find(make_document(kvp("files_id", id)),
                              options::find{}.sort(make_document(kvp("n", 1))))) {
        REQUIRE(static_cast<std::size_t>(chunks_doc["n"].get_int32().value) == i);
        auto data = chunks_doc["data"].get_binary();
        validate_chunk(data, i);

        ++i;
    }

    auto num_chunks_div = std::lldiv(expected_length, expected_chunk_size);

    if (num_chunks_div.rem) {
        ++num_chunks_div.quot;
    }

    REQUIRE(num_chunks_div.quot <= std::numeric_limits<std::int32_t>::max());
    REQUIRE(i == static_cast<std::size_t>(num_chunks_div.quot));
}

// Add an arbitrary GridFS file with a specified length, chunk size, and id to a database's default
// GridFS bucket (i.e. the "fs.files" and "fs.chunks" collections).
//
// Returns a vector of the bytes stored in the GridFS chunks.
std::vector<std::uint8_t> manual_gridfs_initialize(database db,
                                                   std::int64_t length,
                                                   std::int32_t chunk_size,
                                                   bsoncxx::types::bson_value::view id) {
    std::vector<std::uint8_t> bytes;

    // Populate the vector with arbitrary values.
    for (std::int64_t i = 0; i < length; ++i) {
        bytes.push_back(static_cast<std::uint8_t>((i + 200) % 256));
    }

    std::vector<bsoncxx::document::value> chunks;
    std::int64_t bytes_written = 0;

    for (std::int32_t i = 0; bytes_written < length; ++i) {
        // The last chunk should be truncated to fit specified file length.
        std::int32_t current_chunk_size = static_cast<std::int32_t>(
            std::min(static_cast<std::int64_t>(chunk_size), length - bytes_written));

        bsoncxx::types::b_binary data = {bsoncxx::binary_sub_type::k_binary,
                                         static_cast<std::uint32_t>(current_chunk_size),
                                         bytes.data() + bytes_written};

        chunks.push_back(make_document(kvp("files_id", id), kvp("n", i), kvp("data", data)));
        bytes_written += current_chunk_size;
    }

    db["fs.chunks"].insert_many(chunks);
    db["fs.files"].insert_one(make_document(kvp("_id", id),
                                            kvp("length", length),
                                            kvp("chunkSize", bsoncxx::types::b_int32{chunk_size})));

    return bytes;
}

// Add an arbitrary GridFS file with a specified length, chunk size, and id to a database's default
// GridFS bucket (i.e. the "fs.files" and "fs.chunks" collections).
//
// `get_expected_chunk_bytes` takes the chunk number as a parameter and returns a vector of the
// bytes in that chunk.
void manual_gridfs_initialize(
    database db,
    std::int32_t num_chunks,
    std::int32_t chunk_size,
    bsoncxx::types::bson_value::view id,
    std::function<std::vector<std::uint8_t>(std::int32_t)> get_expected_chunk_bytes) {
    db["fs.chunks"].create_index(make_document(kvp("files_id", 1), kvp("n", 1)),
                                 options::index{}.unique(true));

    std::int64_t bytes_written = 0;

    for (std::int32_t i = 0; i < num_chunks; ++i) {
        auto bytes = get_expected_chunk_bytes(i);
        bsoncxx::types::b_binary data = {bsoncxx::binary_sub_type::k_binary,
                                         static_cast<std::uint32_t>(bytes.size()),
                                         bytes.data()};

        db["fs.chunks"].insert_one(
            make_document(kvp("files_id", id), kvp("n", i), kvp("data", data)));

        bytes_written += bytes.size();
    }

    db["fs.files"].insert_one(make_document(kvp("_id", id),
                                            kvp("length", bytes_written),
                                            kvp("chunkSize", bsoncxx::types::b_int32{chunk_size})));
}

TEST_CASE("mongocxx::gridfs::bucket default constructor makes invalid bucket", "[gridfs::bucket]") {
    instance::current();

    gridfs::bucket bucket;
    REQUIRE(!bucket);
    REQUIRE_THROWS_AS(bucket.bucket_name(), logic_error);
}

TEST_CASE("mongocxx::gridfs::bucket copy constructor", "[gridfs::bucket]") {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};
    database db = client["gridfs_bucket_copy_constructor"];

    SECTION("constructing from valid") {
        gridfs::bucket bucket_a = db.gridfs_bucket(options::gridfs::bucket{}.bucket_name("a"));
        gridfs::bucket bucket_b{bucket_a};
        REQUIRE(bucket_b);
        REQUIRE(bucket_b.bucket_name() == stdx::string_view{"a"});
    }

    SECTION("constructing from invalid") {
        gridfs::bucket bucket_a;
        gridfs::bucket bucket_b{bucket_a};
        REQUIRE(!bucket_b);
    }
}

TEST_CASE("mongocxx::gridfs::bucket copy assignment operator", "[gridfs::bucket]") {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};
    database db = client["gridfs_bucket_copy_assignment"];

    SECTION("assigning valid to valid") {
        gridfs::bucket bucket_a = db.gridfs_bucket(options::gridfs::bucket{}.bucket_name("a"));
        gridfs::bucket bucket_b = db.gridfs_bucket(options::gridfs::bucket{}.bucket_name("b"));
        bucket_b = bucket_a;
        REQUIRE(bucket_b);
        REQUIRE(bucket_b.bucket_name() == stdx::string_view{"a"});
    }

    SECTION("assigning invalid to valid") {
        gridfs::bucket bucket_a;
        gridfs::bucket bucket_b = db.gridfs_bucket(options::gridfs::bucket{}.bucket_name("b"));
        bucket_b = bucket_a;
        REQUIRE(!bucket_b);
    }

    SECTION("assigning valid to invalid") {
        gridfs::bucket bucket_a = db.gridfs_bucket(options::gridfs::bucket{}.bucket_name("a"));
        gridfs::bucket bucket_b;
        bucket_b = bucket_a;
        REQUIRE(bucket_b);
        REQUIRE(bucket_b.bucket_name() == stdx::string_view{"a"});
    }

    SECTION("assigning invalid to invalid") {
        gridfs::bucket bucket_a;
        gridfs::bucket bucket_b;
        bucket_b = bucket_a;
        REQUIRE(!bucket_b);
    }
}

TEST_CASE("database::gridfs_bucket() throws error when options are invalid", "[gridfs::bucket]") {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};
    database db = client["gridfs_bucket_error_invalid_options"];

    options::gridfs::bucket bucket_options;

    SECTION("empty bucket name") {
        bucket_options.bucket_name("");
        REQUIRE_THROWS_AS(db.gridfs_bucket(bucket_options), logic_error);
    }
    SECTION("zero chunk size") {
        bucket_options.chunk_size_bytes(0);
        REQUIRE_THROWS_AS(db.gridfs_bucket(bucket_options), logic_error);
    }
    SECTION("negative chunk size") {
        bucket_options.chunk_size_bytes(-1);
        REQUIRE_THROWS_AS(db.gridfs_bucket(bucket_options), logic_error);
    }
}

TEST_CASE("uploading throws error when options are invalid", "[gridfs::bucket]") {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};
    database db = client["gridfs_upload_error_invalid_options"];
    gridfs::bucket bucket = db.gridfs_bucket();

    options::gridfs::upload upload_options;
    auto run_test = [&]() {
        REQUIRE_THROWS_AS(bucket.open_upload_stream("filename", upload_options), logic_error);
        REQUIRE_THROWS_AS(bucket.open_upload_stream_with_id(
                              bsoncxx::types::bson_value::view{bsoncxx::types::b_int32{0}},
                              "filename",
                              upload_options),
                          logic_error);

        std::istringstream iss{"foo"};
        REQUIRE_THROWS_AS(bucket.upload_from_stream("filename", &iss, upload_options), logic_error);
        REQUIRE_THROWS_AS(bucket.upload_from_stream_with_id(
                              bsoncxx::types::bson_value::view{bsoncxx::types::b_int32{0}},
                              "filename",
                              &iss,
                              upload_options),
                          logic_error);
    };

    SECTION("zero chunk size") {
        upload_options.chunk_size_bytes(0);
        run_test();
    }

    SECTION("negative chunk size") {
        upload_options.chunk_size_bytes(-1);
        run_test();
    }
}

TEST_CASE("downloading throws error when files document is corrupt", "[gridfs::bucket]") {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};
    database db = client["gridfs_files_doc_corrupt"];
    gridfs::bucket bucket = db.gridfs_bucket();

    db["fs.files"].drop();

    const std::int32_t k_expected_chunk_size_bytes = 255 * 1024;  // Default chunk size.
    const std::int64_t k_expected_file_length = 1024 * 1024;
    stdx::optional<bsoncxx::types::bson_value::view> chunk_size{
        bsoncxx::types::bson_value::view{bsoncxx::types::b_int32{k_expected_chunk_size_bytes}}};
    stdx::optional<bsoncxx::types::bson_value::view> length{
        bsoncxx::types::bson_value::view{bsoncxx::types::b_int64{k_expected_file_length}}};
    bool expect_success = false;

    auto run_test = [&]() {
        {
            bsoncxx::builder::basic::document files_doc;
            files_doc.append(kvp("_id", 0));
            if (length) {
                files_doc.append(kvp("length", *length));
            }
            if (chunk_size) {
                files_doc.append(kvp("chunkSize", *chunk_size));
            }

            db["fs.files"].insert_one(files_doc.extract());
        }
        auto open_download_stream = [&bucket]() {
            return bucket.open_download_stream(
                bsoncxx::types::bson_value::view{bsoncxx::types::b_int32{0}});
        };

        if (expect_success) {
            gridfs::downloader downloader = open_download_stream();
            REQUIRE(downloader.chunk_size() == k_expected_chunk_size_bytes);
            REQUIRE(downloader.file_length() == k_expected_file_length);
        } else {
            REQUIRE_THROWS_AS(open_download_stream(), gridfs_exception);
        }
    };

    // Tests for invalid chunk size.
    SECTION("zero chunk size") {
        chunk_size = bsoncxx::types::bson_value::view{bsoncxx::types::b_int32{0}};
        run_test();
    }
    SECTION("negative chunk size") {
        chunk_size = bsoncxx::types::bson_value::view{bsoncxx::types::b_int32{-1}};
        run_test();
    }
    SECTION("chunk size too large") {
        const std::int32_t k_max_document_size = 16 * 1024 * 1024;
        chunk_size =
            bsoncxx::types::bson_value::view{bsoncxx::types::b_int32{k_max_document_size + 1}};
        run_test();
    }
    SECTION("chunk size of wrong type") {
        chunk_size = bsoncxx::types::bson_value::view{bsoncxx::types::b_string{"invalid"}};
        run_test();
    }
    SECTION("missing chunk size") {
        chunk_size = stdx::nullopt;
        run_test();
    }

    // Tests for invalid length.
    SECTION("negative length") {
        length = bsoncxx::types::bson_value::view{bsoncxx::types::b_int64{-1}};
        run_test();
    }
    SECTION("invalid length") {
        length = bsoncxx::types::bson_value::view{bsoncxx::types::b_string{"invalid"}};
        run_test();
    }
    SECTION("missing length") {
        length = stdx::nullopt;
        run_test();
    }

    // Test for too many chunks.
    SECTION("too many chunks") {
        chunk_size = bsoncxx::types::bson_value::view{bsoncxx::types::b_int32{1}};
        length = bsoncxx::types::bson_value::view{bsoncxx::types::b_int64{
            static_cast<std::int64_t>(std::numeric_limits<std::int32_t>::max()) + 1}};
        run_test();
    }

    // Test for valid length and chunk size.
    SECTION("valid length and chunk size") {
        expect_success = true;
        run_test();
    }
}

TEST_CASE("downloading throws error when chunks document is corrupt", "[gridfs::bucket]") {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};
    database db = client["gridfs_chunk_doc_corrupt"];
    gridfs::bucket bucket = db.gridfs_bucket();

    db["fs.files"].drop();
    db["fs.chunks"].drop();

    const std::uint8_t k_expected_data_byte = 'd';

    stdx::optional<bsoncxx::types::bson_value::view> n{
        bsoncxx::types::bson_value::view{bsoncxx::types::b_int32{0}}};
    stdx::optional<bsoncxx::types::bson_value::view> data{bsoncxx::types::bson_value::view{
        bsoncxx::types::b_binary{bsoncxx::binary_sub_type::k_binary, 1, &k_expected_data_byte}}};
    bool expect_success = true;

    // Tests for invalid n.
    SECTION("missing n") {
        n = stdx::nullopt;
        expect_success = false;
    }
    SECTION("wrong type for n") {
        n = bsoncxx::types::bson_value::view{bsoncxx::types::b_int64{0}};
        expect_success = false;
    }

    // Tests for invalid data.
    SECTION("missing data") {
        data = stdx::nullopt;
        expect_success = false;
    }
    SECTION("wrong type for data") {
        data = bsoncxx::types::bson_value::view{bsoncxx::types::b_bool{true}};
        expect_success = false;
    }

    // Test for valid n and data.
    SECTION("valid n and data") {
        expect_success = true;
    }

    {
        bsoncxx::builder::basic::document chunk_doc;
        chunk_doc.append(kvp("files_id", 0));
        if (n) {
            chunk_doc.append(kvp("n", *n));
        }
        if (data) {
            chunk_doc.append(kvp("data", *data));
        }

        db["fs.chunks"].insert_one(chunk_doc.extract());
        db["fs.files"].insert_one(
            make_document(kvp("_id", 0), kvp("length", 1), kvp("chunkSize", 1)));
    }

    gridfs::downloader downloader =
        bucket.open_download_stream(bsoncxx::types::bson_value::view{bsoncxx::types::b_int32{0}});
    auto downloader_read_one = [&downloader]() {
        stdx::optional<std::uint8_t> result;
        std::uint8_t byte;
        std::size_t bytes_downloaded = downloader.read(&byte, 1);
        if (bytes_downloaded > 0) {
            result = byte;
        }
        return result;
    };

    if (expect_success) {
        stdx::optional<std::uint8_t> result = downloader_read_one();
        REQUIRE(result);
        REQUIRE(*result == k_expected_data_byte);
        result = downloader_read_one();
        REQUIRE(!result);
    } else {
        REQUIRE_THROWS_AS(downloader_read_one(), gridfs_exception);
    }
}

TEST_CASE("mongocxx::gridfs::downloader::read with arbitrary sizes", "[gridfs::downloader]") {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};
    database db = client["gridfs_download_read_test"];
    gridfs::bucket bucket = db.gridfs_bucket();

    db["fs.files"].delete_many({});
    db["fs.chunks"].delete_many({});

    std::int64_t file_length = 100;
    std::int32_t chunk_size = 9;
    std::int32_t read_size = 0;

    auto run_test = [&]() {
        REQUIRE(read_size != 0);

        bsoncxx::types::bson_value::view id{bsoncxx::types::b_oid{bsoncxx::oid{}}};
        std::vector<std::uint8_t> expected =
            manual_gridfs_initialize(db, file_length, chunk_size, id);

        // Allocate a buffer large enough to fit the data read from the downloader.
        std::vector<std::uint8_t> buffer;
        buffer.reserve(static_cast<std::size_t>(read_size));

        std::size_t total_bytes_read = 0;
        auto downloader = bucket.open_download_stream(bsoncxx::types::bson_value::view{id});

        while (std::size_t bytes_read =
                   downloader.read(buffer.data(), static_cast<std::size_t>(read_size))) {
            std::vector<std::uint8_t> expected_bytes{
                expected.data() + total_bytes_read,
                expected.data() + total_bytes_read + bytes_read};
            std::vector<std::uint8_t> actual_bytes{buffer.data(), buffer.data() + bytes_read};

            REQUIRE(expected_bytes == actual_bytes);

            total_bytes_read += bytes_read;
        }

        REQUIRE(total_bytes_read == static_cast<std::size_t>(file_length));
    };

    SECTION("read_size = 1") {
        read_size = 1;
        run_test();
    }

    SECTION("read_size = chunk_size - 1") {
        read_size = chunk_size - 1;
        run_test();
    }

    SECTION("read_size = chunk_size") {
        read_size = chunk_size;
        run_test();
    }

    SECTION("read_size = chunk_size + 1") {
        read_size = chunk_size + 1;
        run_test();
    }

    SECTION("read_size = 2 * chunk_size") {
        read_size = 2 * chunk_size;
        run_test();
    }

    SECTION("read_size = file_length") {
        read_size = static_cast<std::int32_t>(file_length);
        run_test();
    }

    SECTION("read_size > file_length") {
        read_size = static_cast<std::int32_t>(file_length + 1);
        run_test();
    }
}

TEST_CASE("mongocxx::gridfs::uploader::abort works", "[gridfs::uploader]") {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};
    database db = client["gridfs_upload_abort_test"];
    gridfs::bucket bucket = db.gridfs_bucket();

    bsoncxx::types::b_oid id = {bsoncxx::oid{}};
    gridfs::uploader uploader =
        bucket.open_upload_stream_with_id(bsoncxx::types::bson_value::view{id}, "file");

    // The intended purpose of gridfs::uploader::abort is to clean up any chunks that are already
    // uploaded and to prevent any more data from being written to the uploader. The most
    // straightforward way to test this would be to write enough data to the uploader to ensure that
    // one or more chunks is written and then to call abort. However, in the interest of efficiency,
    // the uploader will only send the chunks to the server either when the amount of data contained
    // in the unset chunks reaches the limit that can fit in a single document or when the user
    // indicates that the file is fully written by calling gridfs::uploader::close. Since it takes a
    // long time to write enough data for the uploader to flush the chunks to the server and calling
    // `close` prevents calling abort afterwards, the easiest way to test abort is to manually
    // insert a chunk with the matching `files_id`, calling abort, and then ensuring that the chunk
    // has been deleted.
    bsoncxx::builder::basic::document document{};
    document.append(bsoncxx::builder::basic::kvp("files_id", id));
    db["fs.chunks"].insert_one(document.extract());

    uploader.abort();
    REQUIRE_THROWS(uploader.close());

    REQUIRE(!db["fs.files"].find_one({}));
    REQUIRE(!db["fs.chunks"].find_one({}));
}

TEST_CASE("mongocxx::gridfs::uploader::write with arbitrary sizes", "[gridfs::uploader]") {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};
    database db = client["gridfs_upload_write_test"];
    gridfs::bucket bucket = db.gridfs_bucket();

    auto files_coll = db["fs.files"];
    auto chunks_coll = db["fs.chunks"];

    files_coll.delete_many({});
    chunks_coll.delete_many({});

    std::int64_t file_length = 100;
    std::int32_t chunk_size = 9;
    std::int32_t write_size;

    auto run_test = [&]() {
        std::vector<std::uint8_t> bytes;

        // Populate the vector with arbitrary values.
        for (std::size_t i = 0; i < static_cast<std::size_t>(file_length); ++i) {
            bytes.push_back(static_cast<std::uint8_t>((i + 200) % 256));
        }

        std::size_t bytes_written = 0;
        auto upload_options = options::gridfs::upload{}.chunk_size_bytes(chunk_size);
        auto uploader = bucket.open_upload_stream("test_file", upload_options);

        while (static_cast<std::int64_t>(bytes_written) < file_length) {
            std::size_t actual_write_size = static_cast<std::size_t>(
                std::min(static_cast<std::int64_t>(write_size),
                         file_length - static_cast<std::int64_t>(bytes_written)));

            uploader.write(bytes.data() + bytes_written, actual_write_size);
            bytes_written += actual_write_size;
        }

        auto result = uploader.close();

        validate_gridfs_file(db, "fs", result.id(), "test_file", bytes, chunk_size);
    };

    SECTION("write_size = 1") {
        write_size = 1;
        run_test();
    }

    SECTION("write_size = chunk_size - 1") {
        write_size = chunk_size - 1;
        run_test();
    }

    SECTION("write_size = chunk_size") {
        write_size = chunk_size;
        run_test();
    }

    SECTION("write_size = chunk_size + 1") {
        write_size = chunk_size + 1;
        run_test();
    }

    SECTION("write_size = 2 * chunk_size") {
        write_size = 2 * chunk_size;
        run_test();
    }

    SECTION("write_size = file_length") {
        write_size = static_cast<std::int32_t>(file_length);
        run_test();
    }
}

TEST_CASE("gridfs upload/download round trip", "[gridfs::uploader] [gridfs::downloader]") {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};
    database db = client["gridfs_upload_download_round_trip_test"];
    gridfs::bucket bucket = db.gridfs_bucket();

    db["fs.files"].delete_many({});
    db["fs.chunks"].delete_many({});

    std::array<std::uint8_t, 100> uploaded_bytes;

    // Initialize array with arbitrary values.
    for (std::uint8_t i = 0; i < 100; ++i) {
        uploaded_bytes[i] = static_cast<std::uint8_t>(200 - i);
    }

    std::array<std::uint8_t, 100> downloaded_bytes;

    auto uploader = bucket.open_upload_stream("file");
    uploader.write(uploaded_bytes.data(), 100);
    auto result = uploader.close();

    auto downloader = bucket.open_download_stream(result.id());
    auto bytes_read = downloader.read(downloaded_bytes.data(), 100);
    REQUIRE(bytes_read == 100);

    std::uint8_t c;
    REQUIRE(downloader.read(&c, 1) == 0);

    REQUIRE(uploaded_bytes == downloaded_bytes);
}

TEST_CASE("gridfs::bucket::open_upload_stream_with_id works", "[gridfs::bucket]") {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};
    auto db = client["gridfs_bucket_open_upload_stream_with_id"];
    gridfs::bucket bucket = db.gridfs_bucket();

    db["fs.files"].drop();
    db["fs.chunks"].drop();

    std::size_t chunk_size = 4;
    std::vector<std::uint8_t> bytes = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    bsoncxx::types::bson_value::view id{bsoncxx::types::b_oid{bsoncxx::oid{}}};
    auto uploader = bucket.open_upload_stream_with_id(
        id,
        "file",
        options::gridfs::upload{}.chunk_size_bytes(static_cast<std::int32_t>(chunk_size)));

    for (std::size_t i = 0; i < bytes.size(); i += chunk_size) {
        uploader.write(bytes.data() + i, std::min(chunk_size, bytes.size() - i));
    }

    uploader.close();

    validate_gridfs_file(db, "fs", id, "file", bytes, static_cast<std::int32_t>(chunk_size));
}

TEST_CASE("gridfs::bucket::upload_from_stream works", "[gridfs::bucket]") {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};
    auto db = client["gridfs_bucket_upload_from_stream_works"];
    gridfs::bucket bucket = db.gridfs_bucket();

    db["fs.files"].delete_many({});
    db["fs.chunks"].delete_many({});

    std::string numbers = "0123456789";

    std::istringstream ss{numbers};

    std::int32_t chunk_size = 4;
    options::gridfs::upload opts;
    opts.chunk_size_bytes(chunk_size);

    bsoncxx::types::bson_value::view id{bsoncxx::types::b_oid{bsoncxx::oid{}}};

    auto validate = [&]() {
        validate_gridfs_file(db,
                             "fs",
                             id,
                             "file",
                             std::vector<std::uint8_t>{numbers.begin(), numbers.end()},
                             chunk_size);
    };

    SECTION("upload_from_stream") {
        id = bucket.upload_from_stream("file", &ss, opts).id();
        validate();
    }

    SECTION("upload_from_stream_with_id") {
        bucket.upload_from_stream_with_id(id, "file", &ss, opts);
        validate();
    }
}

TEST_CASE("gridfs::bucket::upload_from_stream doesn't infinite loop when passed bad ifstream",
          "[gridfs::bucket]") {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};
    database db = client["gridfs_upload_from_stream_no_infinite_loop_ifstream"];
    gridfs::bucket bucket = db.gridfs_bucket();

    std::ifstream stream{"file_that_does_not_exist.txt"};

    SECTION("upload_from_stream") {
        REQUIRE_THROWS(bucket.upload_from_stream("file", &stream));
    }

    SECTION("upload_from_stream_with_id") {
        bsoncxx::types::bson_value::view id{bsoncxx::types::b_oid{bsoncxx::oid{}}};
        REQUIRE_THROWS(bucket.upload_from_stream_with_id(id, "file", &stream));
    }
}

TEST_CASE("gridfs upload_from_stream aborts on failure", "[gridfs::bucket]") {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};
    database db = client["gridfs_upload_from_stream_abort_on_failure"];
    gridfs::bucket bucket = db.gridfs_bucket();

    db["fs.files"].delete_many({});
    db["fs.chunks"].delete_many({});

    std::ifstream in{"file_that_does_not_exist.txt"};

    auto id = bsoncxx::types::bson_value::view{bsoncxx::types::b_oid{bsoncxx::oid{}}};

    // Aborting the upload should clear all chunks with the given file id.
    db["fs.chunks"].insert_one(make_document(kvp("files_id", id)));

    REQUIRE_THROWS(bucket.upload_from_stream_with_id(id, "file", &in, {}));
    REQUIRE(!db["fs.chunks"].find_one({}));
}

TEST_CASE("gridfs::bucket::download_to_stream works", "[gridfs::bucket]") {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};
    database db = client["gridfs_download_to_stream_works"];
    gridfs::bucket bucket = db.gridfs_bucket();

    db["fs.files"].drop();
    db["fs.chunks"].drop();

    std::int64_t length = 19;  // length is the length of the test file in bytes.
    std::int32_t chunk_size = 4;
    bsoncxx::types::bson_value::view id{bsoncxx::types::b_oid{bsoncxx::oid{}}};
    auto file_bytes = manual_gridfs_initialize(db, length, chunk_size, id);
    std::ostringstream os;

    SECTION("download complete file") {
        bucket.download_to_stream(id, &os);

        auto str = os.str();
        std::vector<std::uint8_t> actual_bytes{str.begin(), str.end()};

        REQUIRE(file_bytes == actual_bytes);
    }

    SECTION("download complete file with start/end offsets") {
        bucket.download_to_stream(id, &os, 0, static_cast<size_t>(length));

        auto str = os.str();
        std::vector<std::uint8_t> actual_bytes{str.begin(), str.end()};

        REQUIRE(file_bytes == actual_bytes);
    }

    SECTION("download partial file") {
        auto check_downloaded_content = [&](std::size_t start, std::size_t end) {
            bucket.download_to_stream(id, &os, start, end);

            auto str = os.str();
            std::vector<std::uint8_t> actual_bytes{str.begin(), str.end()};
            REQUIRE(actual_bytes.size() == end - start);
            const std::vector<std::uint8_t> expected_bytes{
                file_bytes.begin() + static_cast<std::vector<uint8_t>::difference_type>(start),
                file_bytes.begin() + static_cast<std::vector<uint8_t>::difference_type>(end)};
            REQUIRE(expected_bytes == actual_bytes);
        };

        SECTION("whole file") {
            check_downloaded_content(0, static_cast<std::size_t>(length));
        }

        SECTION("no data") {
            check_downloaded_content(0, 0);
        }

        SECTION("at file start") {
            SECTION("partial chunk") {
                check_downloaded_content(0, static_cast<std::size_t>(chunk_size - 1));
            }

            SECTION("complete chunk") {
                check_downloaded_content(0, static_cast<std::size_t>(chunk_size));
            }

            SECTION("across chunks") {
                check_downloaded_content(0, static_cast<std::size_t>(2 * chunk_size));
            }
        }

        SECTION("middle of the file") {
            SECTION("partial chunk") {
                const auto start = chunk_size + chunk_size / 2;
                const auto end = start + 1;
                check_downloaded_content(static_cast<std::size_t>(start),
                                         static_cast<std::size_t>(end));
            }

            SECTION("complete chunk") {
                const auto start = chunk_size;
                const auto end = start + chunk_size;
                check_downloaded_content(static_cast<std::size_t>(start),
                                         static_cast<std::size_t>(end));
            }

            SECTION("across 2 chunks") {
                const auto start = chunk_size / 2;
                const auto end = start + chunk_size;
                check_downloaded_content(static_cast<std::size_t>(start),
                                         static_cast<std::size_t>(end));
            }

            SECTION("across 2 chunks at the end") {
                const auto start = chunk_size - 1;
                const auto end = start + chunk_size - 1;
                check_downloaded_content(static_cast<std::size_t>(start),
                                         static_cast<std::size_t>(end));
            }
            SECTION("across 3 chunks") {
                const auto start = chunk_size / 2;
                const auto end = start + 2 * chunk_size;
                check_downloaded_content(static_cast<std::size_t>(start),
                                         static_cast<std::size_t>(end));
            }
        }

        SECTION("at file end") {
            const auto last_chunk_start = chunk_size * (length / chunk_size);
            SECTION("across chunks") {
                check_downloaded_content(
                    static_cast<std::size_t>(last_chunk_start - (2 * chunk_size) + 1),
                    static_cast<std::size_t>(length));
            }

            SECTION("partial chunk") {
                check_downloaded_content(static_cast<std::size_t>(last_chunk_start + 1),
                                         static_cast<std::size_t>(length));
            }

            SECTION("complete chunk") {
                check_downloaded_content(static_cast<std::size_t>(last_chunk_start),
                                         static_cast<std::size_t>(length));
            }
        }
    }
}

TEST_CASE("gridfs::bucket::delete_file works", "[gridfs::bucket]") {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};
    database db = client["gridfs_delete_file_works"];
    gridfs::bucket bucket = db.gridfs_bucket();

    db["fs.files"].drop();
    db["fs.chunks"].drop();

    std::int64_t length = 10;
    std::int32_t chunk_size = 4;
    bsoncxx::types::bson_value::view id{bsoncxx::types::b_oid{bsoncxx::oid{}}};
    manual_gridfs_initialize(db, length, chunk_size, id);

    bucket.delete_file(id);

    REQUIRE(!db["fs.files"].find_one({}));
    REQUIRE(!db["fs.chunks"].find_one({}));
}

TEST_CASE("gridfs::bucket::find works", "[gridfs::bucket]") {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};
    database db = client["gridfs_find_works"];
    gridfs::bucket bucket = db.gridfs_bucket();

    db["fs.files"].drop();
    db["fs.chunks"].drop();

    std::int64_t length = 10;
    std::int32_t chunk_size = 4;
    bsoncxx::types::bson_value::view id1{bsoncxx::types::b_int32{1}};
    bsoncxx::types::bson_value::view id2{bsoncxx::types::b_int32{2}};

    manual_gridfs_initialize(db, length, chunk_size, id1);
    manual_gridfs_initialize(db, length, chunk_size, id2);

    SECTION("find all files") {
        auto cursor = bucket.find({});
        REQUIRE(std::distance(cursor.begin(), cursor.end()) == 2);
    }

    SECTION("find id = 1") {
        auto cursor = bucket.find(make_document(kvp("_id", id1)));
        REQUIRE(std::distance(cursor.begin(), cursor.end()) == 1);
    }

    SECTION("find id = 2") {
        auto cursor = bucket.find(make_document(kvp("_id", id2)));
        REQUIRE(std::distance(cursor.begin(), cursor.end()) == 1);
    }

    SECTION("find returning nothing") {
        bsoncxx::types::bson_value::view id3{bsoncxx::types::b_int32{3}};
        auto cursor = bucket.find(make_document(kvp("_id", id3)));
        REQUIRE(std::distance(cursor.begin(), cursor.end()) == 0);
    }
}

TEST_CASE("gridfs upload large file", "[gridfs::bucket]") {
    instance::current();

    char* enable_slow_tests = std::getenv("MONGOCXX_ENABLE_SLOW_TESTS");

    if (!enable_slow_tests || stdx::string_view{enable_slow_tests}.empty()) {
        return;
    }

    client client{uri{}, test_util::add_test_server_api()};
    database db = client["gridfs_upload_large_file"];
    gridfs::bucket bucket = db.gridfs_bucket();

    db["fs.files"].drop();
    db["fs.chunks"].drop();

    constexpr std::size_t chunk_size = 7 * 1024 * 1024;
    std::int64_t length = static_cast<std::int64_t>(std::numeric_limits<std::uint32_t>::max()) + 2;

    auto uploader = bucket.open_upload_stream(
        "large_file", options::gridfs::upload{}.chunk_size_bytes(chunk_size));

    std::vector<std::uint8_t> bytes;

    for (std::int64_t i = 0; i * static_cast<std::int64_t>(chunk_size) < length; ++i) {
        std::size_t current_chunk_size =
            static_cast<std::size_t>(std::min(static_cast<std::int64_t>(chunk_size),
                                              length - i * static_cast<std::int64_t>(chunk_size)));
        bytes.assign(current_chunk_size, static_cast<std::uint8_t>(i % 200));

        uploader.write(bytes.data(), current_chunk_size);
    }

    auto result = uploader.close();
    auto id = result.id();

    validate_gridfs_file(
        db,
        "fs",
        id,
        "large_file",
        [&, length](const bsoncxx::types::b_binary& data, std::size_t i) {
            REQUIRE(data.sub_type == bsoncxx::binary_sub_type::k_binary);
            REQUIRE(static_cast<std::int64_t>(data.size) ==
                    std::min(static_cast<std::int64_t>(chunk_size),
                             length - static_cast<std::int64_t>(i * chunk_size)));

            INFO("chunk_number: " << i);

            REQUIRE(std::all_of(data.bytes, data.bytes + data.size, [i](std::uint8_t byte) {
                return byte == static_cast<std::uint8_t>(i % 200);
            }));
        },
        chunk_size,
        length);
}

TEST_CASE("gridfs download large file", "[gridfs::bucket]") {
    instance::current();

    char* enable_slow_tests = std::getenv("MONGOCXX_ENABLE_SLOW_TESTS");

    if (!enable_slow_tests || stdx::string_view{enable_slow_tests}.empty()) {
        return;
    }

    client client{uri{}, test_util::add_test_server_api()};
    database db = client["gridfs_download_large_file"];
    gridfs::bucket bucket = db.gridfs_bucket();

    db["fs.files"].drop();
    db["fs.chunks"].drop();

    constexpr std::size_t chunk_size = 7 * 1024 * 1024;
    std::int64_t length = static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max()) + 2;

    auto num_chunks_div = std::lldiv(length, chunk_size);
    std::int32_t num_chunks = static_cast<std::int32_t>(num_chunks_div.quot);

    if (num_chunks_div.rem) {
        ++num_chunks;
    }

    bsoncxx::types::bson_value::view id{bsoncxx::types::b_oid{bsoncxx::oid{}}};
    manual_gridfs_initialize(
        db,
        num_chunks,
        static_cast<std::int32_t>(chunk_size),
        id,
        [&, num_chunks, length](std::int32_t chunk_num) {
            std::int32_t current_chunk_size = static_cast<std::int32_t>(chunk_size);

            if (chunk_num == num_chunks - 1) {
                current_chunk_size =
                    static_cast<std::int32_t>(static_cast<std::uint64_t>(length) -
                                              static_cast<std::uint32_t>(chunk_num) * chunk_size);
            }

            std::vector<std::uint8_t> bytes;
            bytes.assign(static_cast<std::size_t>(current_chunk_size),
                         static_cast<std::uint8_t>(chunk_num % 200));

            return bytes;
        });

    auto downloader = bucket.open_download_stream(id);

    std::vector<std::uint8_t> bytes;
    bytes.resize(chunk_size);

    std::int64_t total_bytes_read = 0;
    std::int32_t i = 0;

    while (std::size_t current_bytes_read = downloader.read(bytes.data(), chunk_size)) {
        INFO("chunk number: " << i);

        REQUIRE(
            std::all_of(bytes.data(), bytes.data() + current_bytes_read, [i](std::uint8_t byte) {
                return byte == static_cast<std::uint8_t>(i % 200);
            }));

        total_bytes_read += current_bytes_read;
        ++i;
    }

    REQUIRE(total_bytes_read == length);
}

std::string _gen_database_name(std::string name) {
    using namespace std::chrono;
    milliseconds ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
    return name + '_' + std::to_string(ms.count());
}

TEST_CASE("gridfs does not create additional indexes", "[gridfs::uploader] [gridfs::downloader]") {
    using bsoncxx::builder::basic::document;
    using bsoncxx::builder::basic::sub_array;

    instance::current();
    client client{uri{}, test_util::add_test_server_api()};
    auto db_name = _gen_database_name("gridfs_index_creation");
    database db = client[db_name];

    SECTION("when the default index is already created on fs.files") {
        REQUIRE_NOTHROW(db.run_command(make_document(
            kvp("createIndexes", "fs.files"), kvp("indexes", [&](sub_array sub_arr) {
                sub_arr.append(make_document(
                    kvp("key", make_document(kvp("filename", 1.0), kvp("uploadDate", 1.0))),
                    kvp("name", "filename_1_uploadDate_1")));
            }))));
    }

    SECTION("when the default index is already created on fs.chunks") {
        REQUIRE_NOTHROW(db.run_command(
            make_document(kvp("createIndexes", "fs.chunks"), kvp("indexes", [&](sub_array sub_arr) {
                              sub_arr.append(make_document(
                                  kvp("key", make_document(kvp("files_id", 1.0), kvp("n", 1.0))),
                                  kvp("name", "files_id_1_n_1"),
                                  kvp("unique", true)));
                          }))));
    }

    const size_t file_size = 100;
    std::array<std::uint8_t, file_size> to_write, to_read;
    std::iota(begin(to_write), end(to_write), 0);

    auto bucket = db.gridfs_bucket();
    auto uploader = bucket.open_upload_stream("test_file");

    uploader.write(to_write.data(), file_size);
    auto result = uploader.close();

    auto downloader = bucket.open_download_stream(result.id());
    auto bytes_read = downloader.read(to_read.data(), file_size);
    REQUIRE(bytes_read == file_size);

    for (const auto& bucket_collection_name : {"fs.chunks", "fs.files"}) {
        auto indexes = db[bucket_collection_name].list_indexes();
        REQUIRE(std::distance(indexes.begin(), indexes.end()) == 2);
    }
}

}  // namespace
