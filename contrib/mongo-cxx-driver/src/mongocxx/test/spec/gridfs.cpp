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

#include <cmath>
#include <exception>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include <bsoncxx/array/view.hpp>
#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/oid.hpp>
#include <bsoncxx/stdx/make_unique.hpp>
#include <bsoncxx/stdx/string_view.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <bsoncxx/test/catch.hh>
#include <bsoncxx/types/bson_value/view.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/cursor.hpp>
#include <mongocxx/database.hpp>
#include <mongocxx/gridfs/bucket.hpp>
#include <mongocxx/gridfs/downloader.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/gridfs/upload.hpp>
#include <mongocxx/result/gridfs/upload.hpp>
#include <mongocxx/test/client_helpers.hh>
#include <mongocxx/test/spec/util.hh>

namespace {
using namespace bsoncxx;
using namespace mongocxx;

// This function is a workaround for clang 3.8 and mnmlstc/core, where `return
// optional<item_t>{an_item_t};` fails to compile.
bsoncxx::stdx::optional<test_util::item_t> make_optional(test_util::item_t item) {
    bsoncxx::stdx::optional<test_util::item_t> option{};
    option = item;

    return option;
}

// Query the GridFS files collection and fetch the length of the file.
//
// Returns -1 if the file is not found.
std::int64_t get_length_of_gridfs_file(gridfs::bucket bucket, types::bson_value::view id) {
    builder::basic::document filter;
    filter.append(builder::basic::kvp("_id", id));
    cursor cursor = bucket.find(filter.extract());

    if (cursor.begin() == cursor.end()) {
        return -1;
    }

    document::view doc = *cursor.begin();

    REQUIRE(doc["length"]);
    return (doc["length"].get_int64().value);
}

bsoncxx::stdx::optional<test_util::item_t> transform_hex(test_util::item_t pair,
                                                         builder::basic::array* context) {
    if (!pair.first) {
        return make_optional(pair);
    }

    auto key = *(pair.first);
    auto value = pair.second;

    if (bsoncxx::string::to_string(key) != "data" || value.type() != type::k_document) {
        return make_optional(pair);
    }

    auto data = value.get_document().value;

    if (!data["$hex"] || data["$hex"].type() != type::k_string) {
        return make_optional(pair);
    }

    std::basic_string<std::uint8_t> bytes =
        test_util::convert_hex_string_to_bytes(data["$hex"].get_string().value);
    types::b_binary binary_data = {
        binary_sub_type::k_binary, static_cast<std::uint32_t>(bytes.size()), bytes.data()};

    context->append(binary_data);

    auto view = context->view();
    auto length = std::distance(view.cbegin(), view.cend());

    return make_optional(std::make_pair(bsoncxx::stdx::optional<bsoncxx::stdx::string_view>("data"),
                                        view[static_cast<std::uint32_t>(length - 1)].get_value()));
}

// The GridFS spec specifies the expected binary data in the form of { $hex: "<hexadecimal string>"
// }. This function converts any element value matching that format within the document to a
// bsoncxx::types::b_binary value.
document::value convert_hex_data_to_binary(document::view document) {
    return test_util::transform_document(document, transform_hex);
}

bsoncxx::stdx::optional<test_util::item_t> convert_length_to_int64(test_util::item_t pair,
                                                                   builder::basic::array*) {
    if (!pair.first) {
        return make_optional(pair);
    }

    auto key = *(pair.first);
    auto value = pair.second;

    if (bsoncxx::string::to_string(key) != "length" || value.type() != type::k_int32) {
        return make_optional(pair);
    }

    types::b_int64 length = {value.get_int32()};

    return make_optional(
        std::make_pair(bsoncxx::stdx::optional<bsoncxx::stdx::string_view>("length"),
                       types::bson_value::view{length}));
}

void compare_collections(database db) {
    cursor expected_files_cursor = db["expected.files"].find({});
    cursor actual_files_cursor = db["fs.files"].find({});

    std::vector<document::view> expected_files{expected_files_cursor.begin(),
                                               expected_files_cursor.end()};
    std::vector<document::view> actual_files{actual_files_cursor.begin(),
                                             actual_files_cursor.end()};

    REQUIRE(expected_files.size() == actual_files.size());

    for (std::size_t i = 0; i < expected_files.size(); ++i) {
        document::view expected = expected_files[i];
        document::view actual = actual_files[i];

        REQUIRE(expected["_id"]);
        REQUIRE(actual["_id"]);
        REQUIRE(expected["_id"].get_oid().value == actual["_id"].get_oid().value);

        REQUIRE(expected["length"]);
        REQUIRE(actual["length"]);
        REQUIRE(expected["length"].get_int64().value == actual["length"].get_int64().value);

        REQUIRE(expected["chunkSize"]);
        REQUIRE(actual["chunkSize"]);
        REQUIRE(expected["chunkSize"].get_int32().value == actual["chunkSize"].get_int32().value);

        REQUIRE(expected["filename"]);
        REQUIRE(actual["filename"]);
        REQUIRE(expected["filename"].get_string().value == actual["filename"].get_string().value);
    }

    cursor expected_chunks_cursor = db["expected.chunks"].find({});
    cursor actual_chunks_cursor = db["fs.chunks"].find({});

    std::vector<document::view> expected_chunks{expected_chunks_cursor.begin(),
                                                expected_chunks_cursor.end()};
    std::vector<document::view> actual_chunks{actual_chunks_cursor.begin(),
                                              actual_chunks_cursor.end()};

    REQUIRE(expected_chunks.size() == actual_chunks.size());

    for (std::size_t i = 0; i < expected_chunks.size(); ++i) {
        document::view expected = expected_chunks[i];
        document::view actual = actual_chunks[i];

        REQUIRE(expected["files_id"]);
        REQUIRE(actual["files_id"]);
        REQUIRE(expected["files_id"].get_oid().value == actual["files_id"].get_oid().value);

        REQUIRE(expected["n"]);
        REQUIRE(actual["n"]);
        REQUIRE(expected["n"].get_int32().value == actual["n"].get_int32().value);

        REQUIRE(expected["data"]);
        REQUIRE(actual["data"]);
        REQUIRE(expected["data"].get_binary() == actual["data"].get_binary());
    }
}

// Run the download tests from the GridFS spec.
void test_download(database db,
                   gridfs::bucket bucket,
                   document::view operation,
                   document::view assert_doc) {
    static_cast<void>(db);  // Unused.

    REQUIRE(operation["arguments"]);
    document::view arguments = operation["arguments"].get_document().value;

    REQUIRE(arguments["id"]);
    types::bson_value::view id = arguments["id"].get_value();

    // Allocate the space needed to store the result.
    std::int64_t length = get_length_of_gridfs_file(bucket, id);
    std::unique_ptr<std::uint8_t[]> actual(nullptr);

    if (length > 0) {
        actual = bsoncxx::stdx::make_unique<std::uint8_t[]>(static_cast<std::size_t>(length));
    }

    if (assert_doc["error"]) {
        bsoncxx::stdx::string_view error = assert_doc["error"].get_string().value;

        // If the GridFS file is not found, an error should be thrown when the download stream is
        // opened.
        if (bsoncxx::string::to_string(error) == "FileNotFound") {
            REQUIRE_THROWS_AS(bucket.open_download_stream(id), std::exception);
            return;
        }

        // Otherwise, an error should occur when reading from the stream.
        gridfs::downloader downloader = bucket.open_download_stream(id);
        REQUIRE_THROWS_AS(downloader.read(actual.get(), static_cast<std::size_t>(length)),
                          std::exception);

        return;
    }

    REQUIRE(assert_doc["result"]);
    document::view result = assert_doc["result"].get_document().value;

    gridfs::downloader downloader = bucket.open_download_stream(id);

    std::size_t actual_size = downloader.read(actual.get(), static_cast<std::size_t>(length));
    REQUIRE(static_cast<std::int64_t>(actual_size) == length);

    // The GridFS spec specifies the expected binary data in the form of { $hex: "<hexadecimal
    // string>" }, which needs to be converted to an array of bytes.
    REQUIRE(result["$hex"]);
    std::string hex = bsoncxx::string::to_string(result["$hex"].get_string().value);
    std::basic_string<std::uint8_t> expected = test_util::convert_hex_string_to_bytes(hex);

    REQUIRE(actual_size == expected.size());

    for (std::size_t i = 0; i < actual_size; i++) {
        REQUIRE(actual.get()[i] == expected[i]);
    }
}

// Run the download tests from the GridFS spec.
void test_upload(database db,
                 gridfs::bucket bucket,
                 document::view operation,
                 document::view assert_doc) {
    REQUIRE(operation["arguments"]);
    document::view arguments = operation["arguments"].get_document().value;

    REQUIRE(arguments["options"]);
    document::view options = arguments["options"].get_document().value;

    options::gridfs::upload upload_options;

    if (options["chunkSizeBytes"]) {
        upload_options.chunk_size_bytes(options["chunkSizeBytes"].get_int32().value);
    }

    if (options["metadata"]) {
        upload_options.metadata(options["metadata"].get_document().value);
    }

    REQUIRE(arguments["filename"]);
    bsoncxx::stdx::string_view filename = arguments["filename"].get_string().value;

    gridfs::uploader uploader = bucket.open_upload_stream(filename, upload_options);

    REQUIRE(arguments["source"]);
    document::view source = arguments["source"].get_document().value;

    REQUIRE(source["$hex"]);
    std::string hex = bsoncxx::string::to_string(source["$hex"].get_string().value);
    std::basic_string<std::uint8_t> source_bytes = test_util::convert_hex_string_to_bytes(hex);

    uploader.write(source_bytes.data(), source_bytes.size());
    result::gridfs::upload upload_result = uploader.close();
    auto id = upload_result.id();

    REQUIRE(assert_doc["data"]);

    bsoncxx::document::view data = assert_doc["data"].get_array().value;

    for (auto array_element : data) {
        REQUIRE(array_element.type() == type::k_document);

        bsoncxx::document::value transformed_data = test_util::transform_document(
            array_element.get_document().value,
            [id](test_util::item_t pair,
                 builder::basic::array* context) -> bsoncxx::stdx::optional<test_util::item_t> {
                if (!pair.first) {
                    return make_optional(pair);
                }

                auto key = *(pair.first);
                auto value = pair.second;

                std::string key_string = bsoncxx::string::to_string(key);

                if ((key_string != "_id" && key_string != "files_id") ||
                    value.type() != type::k_string) {
                    auto new_pair = transform_hex(pair, context);

                    if (!new_pair) {
                        return bsoncxx::stdx::optional<test_util::item_t>{};
                    }

                    if (*new_pair != pair) {
                        return new_pair;
                    }

                    return convert_length_to_int64(pair, context);
                }

                std::string id_str = bsoncxx::string::to_string(value.get_string().value);

                if (id_str == "*actual") {
                    return bsoncxx::stdx::optional<test_util::item_t>{};
                }

                if (id_str != "*result") {
                    return make_optional(pair);
                }

                return make_optional(std::make_pair(pair.first, types::bson_value::view{id}));
            });

        db.run_command(transformed_data.view());
    }

    compare_collections(db);
}

void test_delete(database db,
                 gridfs::bucket bucket,
                 document::view operation,
                 document::view assert_doc) {
    REQUIRE(operation["arguments"]);
    document::view arguments = operation["arguments"].get_document().value;

    REQUIRE(arguments["id"]);
    types::bson_value::view id = arguments["id"].get_value();

    if (assert_doc["error"]) {
        REQUIRE_THROWS_AS(bucket.delete_file(id), std::exception);
    } else {
        bucket.delete_file(id);
    }

    if (assert_doc["data"]) {
        for (auto&& element : assert_doc["data"].get_array().value) {
            auto command = convert_hex_data_to_binary(element.get_document().value);
            db.run_command(command.view());
        }
    }

    compare_collections(db);
}

// Downloading a file by name is listed as part of the "Advanced API" in the GridFS spec, which the
// driver does not implement. Since tests for this functionality are part of the suite, this
// function is used as a placeholder for the harness to call instead of actually running the tests.
void test_download_by_name(database, gridfs::bucket, document::view, document::view) {}

std::map<std::string,
         std::function<void(database db, gridfs::bucket, document::view, document::view)>>
    gridfs_test_runners = {{"delete", test_delete},
                           {"download", test_download},
                           {"download_by_name", test_download_by_name},
                           {"upload", test_upload}};

// Clears the collections and initializes them as the spec describes.
void initialize_collections(database db, document::view data) {
    collection files_coll = db["fs.files"];
    collection chunks_coll = db["fs.chunks"];

    collection expected_files_coll = db["expected.files"];
    collection expected_chunks_coll = db["expected.chunks"];

    // We delete all documents from the collections instead of dropping the collection, as the
    // former has much better performance for the GridFS test collections.
    files_coll.delete_many({});
    chunks_coll.delete_many({});
    expected_files_coll.delete_many({});
    expected_chunks_coll.delete_many({});

    std::vector<document::value> files_documents;
    std::vector<document::value> chunks_documents;

    REQUIRE(data["files"]);
    REQUIRE(data["chunks"]);

    auto sanitize =
        [](test_util::item_t pair,
           builder::basic::array* context) -> bsoncxx::stdx::optional<test_util::item_t> {
        auto new_pair = transform_hex(pair, context);

        if (!new_pair) {
            return bsoncxx::stdx::optional<test_util::item_t>{};
        }

        if (*new_pair != pair) {
            return new_pair;
        }

        return convert_length_to_int64(pair, context);
    };

    for (auto&& document : data["files"].get_array().value) {
        // Any instances of { $hex: "..." } are converted to bsoncxx::types::b_binary values before
        // insertion.
        files_documents.push_back(
            test_util::transform_document(document.get_document().value, sanitize));
    }

    for (auto&& document : data["chunks"].get_array().value) {
        // Any instances of { $hex: "..." } are converted to bsoncxx::types::b_binary values before
        // insertion.
        chunks_documents.push_back(
            test_util::transform_document(document.get_document().value, sanitize));
    }

    if (!files_documents.empty()) {
        files_coll.insert_many(files_documents);
    }

    if (!chunks_documents.empty()) {
        chunks_coll.insert_many(chunks_documents);
    }

    if (!files_documents.empty()) {
        expected_files_coll.insert_many(files_documents);
    }

    if (!chunks_documents.empty()) {
        expected_chunks_coll.insert_many(chunks_documents);
    }
}

// Runs the "arrange" section a GridFS spec test.
void arrange(database db, array::view data) {
    for (auto&& element : data) {
        auto doc = convert_hex_data_to_binary(element.get_document().value);
        db.run_command(doc.view());
    }
}

void run_gridfs_tests_in_file(std::string test_path, client* client) {
    INFO("Test path: " << test_path);
    bsoncxx::stdx::optional<document::value> test_spec = test_util::parse_test_file(test_path);

    REQUIRE(test_spec);

    document::view test_spec_view = test_spec->view();

    array::view tests = test_spec_view["tests"].get_array().value;

    database db = (*client)["gridfs_tests"];
    gridfs::bucket bucket = db.gridfs_bucket();

    for (auto&& test : tests) {
        std::string description =
            bsoncxx::string::to_string(test["description"].get_string().value);
        INFO("Test description: " << description);
        initialize_collections(db, test_spec_view["data"].get_document().value);

        if (test["arrange"]) {
            document::view arrange_doc = test["arrange"].get_document().value;
            arrange(db, arrange_doc["data"].get_array().value);
        }

        REQUIRE(test["act"]);
        document::view act = test["act"].get_document().value;

        REQUIRE(test["assert"]);
        document::view assert_doc = test["assert"].get_document().value;

        REQUIRE(act["operation"]);
        auto test_runner =
            gridfs_test_runners[bsoncxx::string::to_string(act["operation"].get_string().value)];
        test_runner(db, bucket, act, assert_doc);
    }
}

TEST_CASE("GridFS spec automated tests", "[gridfs_spec]") {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};

    // Because the GridFS spec tests use write commands that were only added to MongoDB in version
    // 2.6, the tests will not run against any server versions older than that.
    if (test_util::compare_versions(test_util::get_server_version(client), "2.6") < 0) {
        return;
    }

    auto cb = [&](const std::string& test_file) { run_gridfs_tests_in_file(test_file, &client); };

    mongocxx::spec::run_tests_in_suite("GRIDFS_TESTS_PATH", cb);
}
}  // namespace
