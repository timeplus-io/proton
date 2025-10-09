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

#include <chrono>
#include <iostream>
#include <sstream>
#include <thread>
#include <vector>

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/stdx/string_view.hpp>
#include <bsoncxx/types.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/client_encryption.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/find.hpp>
#include <mongocxx/options/server_api.hpp>
#include <mongocxx/uri.hpp>

// NOTE: Any time this file is modified, a DOCS ticket should be opened to sync the changes with the
// corresponding page on mongodb.com/docs. See CXX-1249 and DRIVERS-356 for more info.

template <typename T>
void check_field(const T& document,
                 const char* field,
                 bool should_have,
                 int example_no,
                 const char* example_type = NULL) {
    std::string example_type_formatted = example_type ? example_type + std::string(" ") : "";
    if (should_have) {
        if (!document[field]) {
            throw std::logic_error(std::string("document in ") + example_type_formatted +
                                   std::string("example ") + std::to_string(example_no) +
                                   " should not have field " + field);
        }
    } else {
        if (document[field]) {
            throw std::logic_error(std::string("document in ") + example_type_formatted +
                                   std::string("example ") + std::to_string(example_no) +
                                   " should not have field " + field);
        }
    }
}

template <typename T>
void check_has_field(const T& document, const char* field, int example_no) {
    check_field(document, field, true, example_no);
}

template <typename T>
void check_has_no_field(const T& document, const char* field, int example_no) {
    check_field(document, field, false, example_no);
}

template <typename T>
void check_has_field(const T& document,
                     const char* field,
                     int example_no,
                     const char* example_type) {
    check_field(document, field, true, example_no, example_type);
}

template <typename T>
void check_has_no_field(const T& document,
                        const char* field,
                        int example_no,
                        const char* example_type) {
    check_field(document, field, false, example_no, example_type);
}

bool should_run_client_side_encryption_test(void) {
    const char* const vars[] = {
        "MONGOCXX_TEST_AWS_SECRET_ACCESS_KEY",
        "MONGOCXX_TEST_AWS_ACCESS_KEY_ID",
        "MONGOCXX_TEST_AZURE_TENANT_ID",
        "MONGOCXX_TEST_AZURE_CLIENT_ID",
        "MONGOCXX_TEST_AZURE_CLIENT_SECRET",
        "MONGOCXX_TEST_CSFLE_TLS_CA_FILE",
        "MONGOCXX_TEST_CSFLE_TLS_CERTIFICATE_KEY_FILE",
        "MONGOCXX_TEST_GCP_EMAIL",
        "MONGOCXX_TEST_GCP_PRIVATEKEY",
    };

    const auto iter = std::find_if_not(
        std::begin(vars), std::end(vars), [](const char* var) { return std::getenv(var); });

    if (iter != std::end(vars)) {
        std::cerr << "Skipping Queryable Encryption tests: environment variable " << *iter
                  << " not defined" << std::endl;
        return false;
    }

    return true;
}

mongocxx::options::client add_test_server_api(mongocxx::options::client opts = {}) {
    const auto api_version = std::getenv("MONGODB_API_VERSION");

    if (!api_version) {
        return opts;
    }

    const auto api_version_sv = bsoncxx::stdx::string_view(api_version);

    if (!api_version_sv.empty()) {
        if (api_version_sv.compare("1") == 0) {
            opts.server_api_opts(
                mongocxx::options::server_api(mongocxx::options::server_api::version::k_version_1));
        } else {
            throw std::logic_error("invalid MONGODB_API_VERSION: " + std::string(api_version_sv));
        }
    }

    return opts;
}

std::string getenv_or_fail(const char* s) {
    const char* env = std::getenv(s);
    if (!env) {
        throw std::runtime_error("missing a required environment variable: " + std::string(s));
    }
    return std::string(env);
}

// Returns a document with credentials for KMS providers.
// If include_external is true, all KMS providers are set.
// If include_external is false, only the local provider is set.
bsoncxx::document::value _make_kms_doc(bool include_external = true) {
    using bsoncxx::builder::basic::sub_document;
    using bsoncxx::builder::stream::close_array;
    using bsoncxx::builder::stream::close_document;
    using bsoncxx::builder::stream::document;
    using bsoncxx::builder::stream::finalize;
    using bsoncxx::builder::stream::open_array;
    using bsoncxx::builder::stream::open_document;

    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;

    const uint8_t kLocalMasterKey[] =
        "\x32\x78\x34\x34\x2b\x78\x64\x75\x54\x61\x42\x42\x6b\x59\x31\x36\x45\x72"
        "\x35\x44\x75\x41\x44\x61\x67\x68\x76\x53\x34\x76\x77\x64\x6b\x67\x38\x74"
        "\x70\x50\x70\x33\x74\x7a\x36\x67\x56\x30\x31\x41\x31\x43\x77\x62\x44\x39"
        "\x69\x74\x51\x32\x48\x46\x44\x67\x50\x57\x4f\x70\x38\x65\x4d\x61\x43\x31"
        "\x4f\x69\x37\x36\x36\x4a\x7a\x58\x5a\x42\x64\x42\x64\x62\x64\x4d\x75\x72"
        "\x64\x6f\x6e\x4a\x31\x64";

    static_assert(sizeof(kLocalMasterKey) - 1u == 96u, "key should be exactly 96 bytes");

    auto kms_doc = bsoncxx::builder::basic::document{};

    if (include_external) {
        kms_doc.append(kvp("aws", [&](sub_document subdoc) {
            subdoc.append(
                kvp("secretAccessKey", getenv_or_fail("MONGOCXX_TEST_AWS_SECRET_ACCESS_KEY")));
            subdoc.append(kvp("accessKeyId", getenv_or_fail("MONGOCXX_TEST_AWS_ACCESS_KEY_ID")));
        }));

        kms_doc.append(kvp("azure", [&](sub_document subdoc) {
            subdoc.append(kvp("tenantId", getenv_or_fail("MONGOCXX_TEST_AZURE_TENANT_ID")));
            subdoc.append(kvp("clientId", getenv_or_fail("MONGOCXX_TEST_AZURE_CLIENT_ID")));
            subdoc.append(kvp("clientSecret", getenv_or_fail("MONGOCXX_TEST_AZURE_CLIENT_SECRET")));
        }));

        kms_doc.append(kvp("gcp", [&](sub_document subdoc) {
            subdoc.append(kvp("email", getenv_or_fail("MONGOCXX_TEST_GCP_EMAIL")));
            subdoc.append(kvp("privateKey", getenv_or_fail("MONGOCXX_TEST_GCP_PRIVATEKEY")));
        }));

        kms_doc.append(kvp("kmip", [&](sub_document subdoc) {
            subdoc.append(kvp("endpoint", "localhost:5698"));
        }));
    }

    bsoncxx::types::b_binary local_master_key{
        bsoncxx::binary_sub_type::k_binary, 96, kLocalMasterKey};

    kms_doc.append(
        kvp("local", [&](sub_document subdoc) { subdoc.append(kvp("key", local_master_key)); }));

    return {kms_doc.extract()};
}

static bsoncxx::document::value get_is_master(const mongocxx::client& client) {
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;

    static auto reply = client["admin"].run_command(make_document(kvp("isMaster", 1)));
    return reply;
}

static bool is_replica_set(const mongocxx::client& client) {
    auto reply = get_is_master(client);
    return static_cast<bool>(reply.view()["setName"]);
}

static bool version_at_least(mongocxx::database& db,
                             int minimum_major,
                             int minimum_minor,
                             int minimum_patch) {
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;

    auto resp = db.run_command(make_document(kvp("buildInfo", 1)));
    auto version = resp.find("version")->get_string().value;
    std::string major_string;
    std::string minor_string;
    std::string patch_string;
    int split = 0;
    for (auto i : version) {
        if (i == '.') {
            split += 1;
            continue;
        }
        if (split == 0) {
            major_string += i;
        } else if (split == 1) {
            minor_string += i;
        } else if (split == 2) {
            patch_string += i;
        }
    }

    std::vector<int> server_semver{
        std::stoi(major_string), std::stoi(minor_string), std::stoi(minor_string)};
    std::vector<int> minimum_semver{minimum_major, minimum_minor, minimum_patch};
    for (size_t i = 0; i < server_semver.size(); i++) {
        if (server_semver[i] < minimum_semver[i]) {
            return false;
        } else if (server_semver[i] > minimum_semver[i]) {
            return true;
        }
    }
    return true;
}

void insert_examples(mongocxx::database db) {
    db["inventory"].drop();

    {
        // Start Example 1
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_array;
        using bsoncxx::builder::basic::make_document;

        db["inventory"].insert_one(make_document(
            kvp("item", "canvas"),
            kvp("qty", 100),
            kvp("tags", make_array("cotton")),
            kvp("size", make_document(kvp("h", 28), kvp("w", 35.5), kvp("uom", "cm")))));
        // End Example 1

        if (db["inventory"].count_documents({}) != 1) {
            throw std::logic_error("wrong count in example 1");
        }
    }

    {
        // Start Example 2
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(make_document(kvp("item", "canvas")));
        // End Example 2

        if (std::distance(cursor.begin(), cursor.end()) != 1) {
            throw std::logic_error("wrong count in example 2");
        }
    }

    {
        // Start Example 3
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_array;
        using bsoncxx::builder::basic::make_document;

        std::vector<bsoncxx::document::value> docs{
            make_document(kvp("item", "journal"),
                          kvp("qty", 25),
                          kvp("tags", make_array("blank", "red")),
                          kvp("size", make_document(kvp("h", 14), kvp("w", 21), kvp("uom", "cm")))),
            make_document(
                kvp("item", "mat"),
                kvp("qty", 85),
                kvp("tags", make_array("gray")),
                kvp("size", make_document(kvp("h", 27.9), kvp("w", 35.5), kvp("uom", "cm")))),
            make_document(
                kvp("item", "mousepad"),
                kvp("qty", 25),
                kvp("tags", make_array("gel", "blue")),
                kvp("size", make_document(kvp("h", 19), kvp("w", 22.85), kvp("uom", "cm")))),
        };

        db["inventory"].insert_many(docs);
        // End Example 3

        if (db["inventory"].count_documents({}) != 4) {
            throw std::logic_error("wrong count in example 3");
        }
    }
}

void query_top_level_fields_examples(mongocxx::database db) {
    db["inventory"].drop();

    {
        // Start Example 6
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        std::vector<bsoncxx::document::value> docs{
            make_document(kvp("item", "journal"),
                          kvp("qty", 25),
                          kvp("size", make_document(kvp("h", 14), kvp("w", 21), kvp("uom", "cm"))),
                          kvp("status", "A")),
            make_document(kvp("item", "notebook"),
                          kvp("qty", 50),
                          kvp("size", make_document(kvp("h", 8.5), kvp("w", 11), kvp("uom", "in"))),
                          kvp("status", "A")),
            make_document(kvp("item", "paper"),
                          kvp("qty", 100),
                          kvp("size", make_document(kvp("h", 8.5), kvp("w", 11), kvp("uom", "in"))),
                          kvp("status", "D")),
            make_document(
                kvp("item", "planner"),
                kvp("qty", 75),
                kvp("size", make_document(kvp("h", 22.85), kvp("w", 30), kvp("uom", "cm"))),
                kvp("status", "D")),
            make_document(
                kvp("item", "postcard"),
                kvp("qty", 45),
                kvp("size", make_document(kvp("h", 10), kvp("w", 15.25), kvp("uom", "cm"))),
                kvp("status", "A")),
        };

        db["inventory"].insert_many(docs);
        // End Example 6

        if (db["inventory"].count_documents({}) != 5) {
            throw std::logic_error("wrong count in example 6");
        }
    }

    {
        // Start Example 7
        auto cursor = db["inventory"].find({});
        // End Example 7

        if (std::distance(cursor.begin(), cursor.end()) != 5) {
            throw std::logic_error("wrong count in example 7");
        }
    }

    {
        // Start Example 9
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(make_document(kvp("status", "D")));
        // End Example 9

        if (std::distance(cursor.begin(), cursor.end()) != 2) {
            throw std::logic_error("wrong count in example 9");
        }
    }

    {
        // Start Example 10
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_array;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(
            make_document(kvp("status", make_document(kvp("$in", make_array("A", "D"))))));
        // End Example 10

        if (std::distance(cursor.begin(), cursor.end()) != 5) {
            throw std::logic_error("wrong count in example 10");
        }
    }

    {
        // Start Example 11
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(
            make_document(kvp("status", "A"), kvp("qty", make_document(kvp("$lt", 30)))));
        // End Example 11

        if (std::distance(cursor.begin(), cursor.end()) != 1) {
            throw std::logic_error("wrong count in example 11");
        }
    }

    {
        // Start Example 12
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_array;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(make_document(
            kvp("$or",
                make_array(make_document(kvp("status", "A")),
                           make_document(kvp("qty", make_document(kvp("$lt", 30))))))));
        // End Example 12

        if (std::distance(cursor.begin(), cursor.end()) != 3) {
            throw std::logic_error("wrong count in example 12");
        }
    }

    {
        // Start Example 13
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_array;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(make_document(
            kvp("status", "A"),
            kvp("$or",
                make_array(make_document(kvp("qty", make_document(kvp("$lt", 30)))),
                           make_document(kvp("item", bsoncxx::types::b_regex{"^p"}))))));
        // End Example 13

        if (std::distance(cursor.begin(), cursor.end()) != 2) {
            throw std::logic_error("wrong count in example 13");
        }
    }
}

void query_embedded_documents_examples(mongocxx::database db) {
    db["inventory"].drop();

    {
        // Start Example 14
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        std::vector<bsoncxx::document::value> docs{
            make_document(kvp("item", "journal"),
                          kvp("qty", 25),
                          kvp("size", make_document(kvp("h", 14), kvp("w", 21), kvp("uom", "cm"))),
                          kvp("status", "A")),
            make_document(kvp("item", "notebook"),
                          kvp("qty", 50),
                          kvp("size", make_document(kvp("h", 8.5), kvp("w", 11), kvp("uom", "in"))),
                          kvp("status", "A")),
            make_document(kvp("item", "paper"),
                          kvp("qty", 100),
                          kvp("size", make_document(kvp("h", 8.5), kvp("w", 11), kvp("uom", "in"))),
                          kvp("status", "D")),
            make_document(
                kvp("item", "planner"),
                kvp("qty", 75),
                kvp("size", make_document(kvp("h", 22.85), kvp("w", 30), kvp("uom", "cm"))),
                kvp("status", "D")),
            make_document(
                kvp("item", "postcard"),
                kvp("qty", 45),
                kvp("size", make_document(kvp("h", 10), kvp("w", 15.25), kvp("uom", "cm"))),
                kvp("status", "A")),
        };

        db["inventory"].insert_many(docs);
        // End Example 14

        if (db["inventory"].count_documents({}) != 5) {
            throw std::logic_error("wrong count in example 14");
        }
    }

    {
        // Start Example 15
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(make_document(
            kvp("size", make_document(kvp("h", 14), kvp("w", 21), kvp("uom", "cm")))));
        // End Example 15

        if (std::distance(cursor.begin(), cursor.end()) != 1) {
            throw std::logic_error("wrong count in example 15");
        }
    }

    {
        // Start Example 16
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(make_document(
            kvp("size", make_document(kvp("w", 21), kvp("h", 14), kvp("uom", "cm")))));
        // End Example 16

        if (std::distance(cursor.begin(), cursor.end()) != 0) {
            throw std::logic_error("wrong count in example 16");
        }
    }

    {
        // Start Example 17
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(make_document(kvp("size.uom", "in")));
        // End Example 17

        if (std::distance(cursor.begin(), cursor.end()) != 2) {
            throw std::logic_error("wrong count in example 17");
        }
    }

    {
        // Start Example 18
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor =
            db["inventory"].find(make_document(kvp("size.h", make_document(kvp("$lt", 15)))));
        // End Example 18

        if (std::distance(cursor.begin(), cursor.end()) != 4) {
            throw std::logic_error("wrong count in example 18");
        }
    }

    {
        // Start Example 19
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor =
            db["inventory"].find(make_document(kvp("size.h", make_document(kvp("$lt", 15))),
                                               kvp("size.uom", "in"),
                                               kvp("status", "D")));
        // End Example 19

        if (std::distance(cursor.begin(), cursor.end()) != 1) {
            throw std::logic_error("wrong count in example 19");
        }
    }
}

void query_arrays_examples(mongocxx::database db) {
    db["inventory"].drop();

    {
        // Start Example 20
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_array;
        using bsoncxx::builder::basic::make_document;

        std::vector<bsoncxx::document::value> docs{
            make_document(kvp("item", "journal"),
                          kvp("qty", 25),
                          kvp("tags", make_array("blank", "red")),
                          kvp("dim_cm", make_array(14, 21))),
            make_document(kvp("item", "notebook"),
                          kvp("qty", 50),
                          kvp("tags", make_array("red", "blank")),
                          kvp("dim_cm", make_array(14, 21))),
            make_document(kvp("item", "paper"),
                          kvp("qty", 100),
                          kvp("tags", make_array("red", "blank", "plain")),
                          kvp("dim_cm", make_array(14, 21))),
            make_document(kvp("item", "planner"),
                          kvp("qty", 75),
                          kvp("tags", make_array("blank", "red")),
                          kvp("dim_cm", make_array(22.85, 30))),
            make_document(kvp("item", "postcard"),
                          kvp("qty", 45),
                          kvp("tags", make_array("blue")),
                          kvp("dim_cm", make_array(10, 15.25))),
        };

        db["inventory"].insert_many(docs);
        // End Example 20

        if (db["inventory"].count_documents({}) != 5) {
            throw std::logic_error("wrong count in example 20");
        }
    }

    {
        // Start Example 21
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_array;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(make_document(kvp("tags", make_array("red", "blank"))));
        // End Example 21

        if (std::distance(cursor.begin(), cursor.end()) != 1) {
            throw std::logic_error("wrong count in example 21");
        }
    }

    {
        // Start Example 22
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_array;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(
            make_document(kvp("tags", make_document(kvp("$all", make_array("red", "blank"))))));
        // End Example 22

        if (std::distance(cursor.begin(), cursor.end()) != 4) {
            throw std::logic_error("wrong count in example 22");
        }
    }

    {
        // Start Example 23
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(make_document(kvp("tags", "red")));
        // End Example 23

        if (std::distance(cursor.begin(), cursor.end()) != 4) {
            throw std::logic_error("wrong count in example 23");
        }
    }

    {
        // Start Example 24
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor =
            db["inventory"].find(make_document(kvp("dim_cm", make_document(kvp("$gt", 25)))));
        // End Example 24

        if (std::distance(cursor.begin(), cursor.end()) != 1) {
            throw std::logic_error("wrong count in example 24");
        }
    }

    {
        // Start Example 25
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(
            make_document(kvp("dim_cm", make_document(kvp("$gt", 15), kvp("$lt", 20)))));
        // End Example 25

        if (std::distance(cursor.begin(), cursor.end()) != 4) {
            throw std::logic_error("wrong count in example 25");
        }
    }

    {
        // Start Example 26
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(make_document(
            kvp("dim_cm",
                make_document(kvp("$elemMatch", make_document(kvp("$gt", 22), kvp("$lt", 30)))))));
        // End Example 26

        if (std::distance(cursor.begin(), cursor.end()) != 1) {
            throw std::logic_error("wrong count in example 26");
        }
    }

    {
        // Start Example 27
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor =
            db["inventory"].find(make_document(kvp("dim_cm.1", make_document(kvp("$gt", 25)))));
        // End Example 27

        if (std::distance(cursor.begin(), cursor.end()) != 1) {
            throw std::logic_error("wrong count in example 27");
        }
    }

    {
        // Start Example 28
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor =
            db["inventory"].find(make_document(kvp("tags", make_document(kvp("$size", 3)))));
        // End Example 28

        if (std::distance(cursor.begin(), cursor.end()) != 1) {
            throw std::logic_error("wrong count in example 28");
        }
    }
}

void query_array_embedded_documents_examples(mongocxx::database db) {
    db["inventory"].drop();

    {
        // Start Example 29
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_array;
        using bsoncxx::builder::basic::make_document;

        std::vector<bsoncxx::document::value> docs{
            make_document(kvp("item", "journal"),
                          kvp("instock",
                              make_array(make_document(kvp("warehouse", "A"), kvp("qty", 5)),
                                         make_document(kvp("warehouse", "C"), kvp("qty", 15))))),
            make_document(
                kvp("item", "notebook"),
                kvp("instock", make_array(make_document(kvp("warehouse", "C"), kvp("qty", 5))))),
            make_document(kvp("item", "paper"),
                          kvp("instock",
                              make_array(make_document(kvp("warehouse", "A"), kvp("qty", 60)),
                                         make_document(kvp("warehouse", "B"), kvp("qty", 15))))),
            make_document(kvp("item", "planner"),
                          kvp("instock",
                              make_array(make_document(kvp("warehouse", "A"), kvp("qty", 40)),
                                         make_document(kvp("warehouse", "B"), kvp("qty", 5))))),
            make_document(kvp("item", "postcard"),
                          kvp("instock",
                              make_array(make_document(kvp("warehouse", "B"), kvp("qty", 15)),
                                         make_document(kvp("warehouse", "C"), kvp("qty", 35))))),
        };

        db["inventory"].insert_many(docs);
        // End Example 29

        if (db["inventory"].count_documents({}) != 5) {
            throw std::logic_error("wrong count in example 29");
        }
    }

    {
        // Start Example 30
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(
            make_document(kvp("instock", make_document(kvp("warehouse", "A"), kvp("qty", 5)))));
        // End Example 30

        if (std::distance(cursor.begin(), cursor.end()) != 1) {
            throw std::logic_error("wrong count in example 30");
        }
    }

    {
        // Start Example 31
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(
            make_document(kvp("instock", make_document(kvp("qty", 5), kvp("warehouse", "A")))));
        // End Example 31

        if (std::distance(cursor.begin(), cursor.end()) != 0) {
            throw std::logic_error("wrong count in example 31");
        }
    }

    {
        // Start Example 32
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(
            make_document(kvp("instock.0.qty", make_document(kvp("$lte", 20)))));
        // End Example 32

        if (std::distance(cursor.begin(), cursor.end()) != 3) {
            throw std::logic_error("wrong count in example 32");
        }
    }

    {
        // Start Example 33
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor =
            db["inventory"].find(make_document(kvp("instock.qty", make_document(kvp("$lte", 20)))));
        // End Example 33

        if (std::distance(cursor.begin(), cursor.end()) != 5) {
            throw std::logic_error("wrong count in example 33");
        }
    }

    {
        // Start Example 34
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(make_document(
            kvp("instock",
                make_document(
                    kvp("$elemMatch", make_document(kvp("qty", 5), kvp("warehouse", "A")))))));
        // End Example 34

        if (std::distance(cursor.begin(), cursor.end()) != 1) {
            throw std::logic_error("wrong count in example 34");
        }
    }

    {
        // Start Example 35
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(make_document(
            kvp("instock",
                make_document(kvp(
                    "$elemMatch",
                    make_document(kvp("qty", make_document(kvp("$gt", 10), kvp("$lte", 20)))))))));
        // End Example 35

        if (std::distance(cursor.begin(), cursor.end()) != 3) {
            throw std::logic_error("wrong count in example 35");
        }
    }

    {
        // Start Example 36
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(
            make_document(kvp("instock.qty", make_document(kvp("$gt", 10), kvp("$lte", 20)))));
        // End Example 36

        if (std::distance(cursor.begin(), cursor.end()) != 4) {
            throw std::logic_error("wrong count in example 36");
        }
    }

    {
        // Start Example 37
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(
            make_document(kvp("instock.qty", 5), kvp("instock.warehouse", "A")));
        // End Example 37

        if (std::distance(cursor.begin(), cursor.end()) != 2) {
            throw std::logic_error("wrong count in example 37");
        }
    }
}

void query_null_missing_fields_examples(mongocxx::database db) {
    db["inventory"].drop();

    {
        // Start Example 38
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        std::vector<bsoncxx::document::value> docs{
            make_document(kvp("_id", 1), kvp("item", bsoncxx::types::b_null{})),
            make_document(kvp("_id", 2)),
        };

        db["inventory"].insert_many(docs);
        // End Example 38

        if (db["inventory"].count_documents({}) != 2) {
            throw std::logic_error("wrong count in example 38");
        }
    }

    {
        // Start Example 39
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(make_document(kvp("item", bsoncxx::types::b_null{})));
        // End Example 39

        if (std::distance(cursor.begin(), cursor.end()) != 2) {
            throw std::logic_error("wrong count in example 39");
        }
    }

    {
        // Start Example 40
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor =
            db["inventory"].find(make_document(kvp("item", make_document(kvp("$type", 10)))));
        // End Example 40

        if (std::distance(cursor.begin(), cursor.end()) != 1) {
            throw std::logic_error("wrong count in example 40");
        }
    }

    {
        // Start Example 41
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor =
            db["inventory"].find(make_document(kvp("item", make_document(kvp("$exists", false)))));
        // End Example 41

        if (std::distance(cursor.begin(), cursor.end()) != 1) {
            throw std::logic_error("wrong count in example 41");
        }
    }
}

void projection_insertion_example(mongocxx::database db) {
    db["inventory"].drop();

    {
        // Start Example 42
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_array;
        using bsoncxx::builder::basic::make_document;

        std::vector<bsoncxx::document::value> docs{
            make_document(
                kvp("item", "journal"),
                kvp("status", "A"),
                kvp("size", make_document(kvp("h", 14), kvp("w", 21), kvp("uom", "cm"))),
                kvp("instock", make_array(make_document(kvp("warehouse", "A"), kvp("qty", 5))))),
            make_document(
                kvp("item", "notebook"),
                kvp("status", "A"),
                kvp("size", make_document(kvp("h", 8.5), kvp("w", 11), kvp("uom", "in"))),
                kvp("instock", make_array(make_document(kvp("warehouse", "C"), kvp("qty", 5))))),
            make_document(
                kvp("item", "paper"),
                kvp("status", "D"),
                kvp("size", make_document(kvp("h", 8.5), kvp("w", 11), kvp("uom", "in"))),
                kvp("instock", make_array(make_document(kvp("warehouse", "A"), kvp("qty", 60))))),
            make_document(
                kvp("item", "planner"),
                kvp("status", "D"),
                kvp("size", make_document(kvp("h", 22.85), kvp("w", 30), kvp("uom", "cm"))),
                kvp("instock", make_array(make_document(kvp("warehouse", "A"), kvp("qty", 40))))),
            make_document(
                kvp("item", "postcard"),
                kvp("status", "A"),
                kvp("size", make_document(kvp("h", 10), kvp("w", 15.25), kvp("uom", "cm"))),
                kvp("instock",
                    make_array(make_document(kvp("warehouse", "B"), kvp("qty", 15)),
                               make_document(kvp("warehouse", "C"), kvp("qty", 35))))),
        };

        db["inventory"].insert_many(docs);
        // End Example 42

        if (db["inventory"].count_documents({}) != 5) {
            throw std::logic_error("wrong count in example 42");
        }
    }
}

void projection_examples(mongocxx::database db) {
    projection_insertion_example(db);

    {
        // Start Example 43
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(make_document(kvp("status", "A")));
        // End Example 43

        if (std::distance(cursor.begin(), cursor.end()) != 3) {
            throw std::logic_error("wrong count in example 43");
        }
    }

    {
        // Start Example 44
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(
            make_document(kvp("status", "A")),
            mongocxx::options::find{}.projection(make_document(kvp("item", 1), kvp("status", 1))));
        // End Example 44

        for (auto&& document : cursor) {
            check_has_field(document, "_id", 44);
            check_has_field(document, "item", 44);
            check_has_field(document, "status", 44);
            check_has_no_field(document, "size", 44);
            check_has_no_field(document, "instock", 44);
        }
    }

    {
        // Start Example 45
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(make_document(kvp("status", "A")),
                                           mongocxx::options::find{}.projection(make_document(
                                               kvp("item", 1), kvp("status", 1), kvp("_id", 0))));
        // End Example 45

        for (auto&& document : cursor) {
            check_has_no_field(document, "_id", 45);
            check_has_field(document, "item", 45);
            check_has_field(document, "status", 45);
            check_has_no_field(document, "size", 45);
            check_has_no_field(document, "instock", 45);
        }
    }

    {
        // Start Example 46
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(make_document(kvp("status", "A")),
                                           mongocxx::options::find{}.projection(
                                               make_document(kvp("status", 0), kvp("instock", 0))));
        // End Example 46

        for (auto&& document : cursor) {
            check_has_field(document, "_id", 46);
            check_has_field(document, "item", 46);
            check_has_no_field(document, "status", 46);
            check_has_field(document, "size", 46);
            check_has_no_field(document, "instock", 46);
        }
    }

    {
        // Start Example 47
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor =
            db["inventory"].find(make_document(kvp("status", "A")),
                                 mongocxx::options::find{}.projection(make_document(
                                     kvp("item", 1), kvp("status", 1), kvp("size.uom", 1))));
        // End Example 47

        for (auto&& document : cursor) {
            check_has_field(document, "_id", 47);
            check_has_field(document, "item", 47);
            check_has_field(document, "status", 47);
            check_has_field(document, "size", 47);
            check_has_no_field(document, "instock", 47);

            auto size = document["size"].get_document().value;

            check_has_field(size, "uom", 47);
            check_has_no_field(size, "h", 47);
            check_has_no_field(size, "w", 47);
        }
    }

    {
        // Start Example 48
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(
            make_document(kvp("status", "A")),
            mongocxx::options::find{}.projection(make_document(kvp("size.uom", 0))));
        // End Example 48

        for (auto&& document : cursor) {
            check_has_field(document, "_id", 48);
            check_has_field(document, "item", 48);
            check_has_field(document, "status", 48);
            check_has_field(document, "size", 48);
            check_has_field(document, "instock", 48);

            auto size = document["size"].get_document().value;

            check_has_no_field(size, "uom", 48);
            check_has_field(size, "h", 48);
            check_has_field(size, "w", 48);
        }
    }

    {
        // Start Example 49
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor =
            db["inventory"].find(make_document(kvp("status", "A")),
                                 mongocxx::options::find{}.projection(make_document(
                                     kvp("item", 1), kvp("status", 1), kvp("instock.qty", 1))));
        // End Example 49

        for (auto&& document : cursor) {
            check_has_field(document, "_id", 49);
            check_has_field(document, "item", 49);
            check_has_field(document, "status", 49);
            check_has_no_field(document, "size", 49);
            check_has_field(document, "instock", 49);

            auto instock = document["instock"].get_array().value;

            for (auto&& sub_document : instock) {
                check_has_no_field(sub_document, "warehouse", 49);
                check_has_field(sub_document, "qty", 49);
            }
        }
    }

    {
        // Start Example 50
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(make_document(kvp("status", "A")),
                                           mongocxx::options::find{}.projection(make_document(
                                               kvp("item", 1),
                                               kvp("status", 1),
                                               kvp("instock", make_document(kvp("$slice", -1))))));
        // End Example 50

        for (auto&& document : cursor) {
            check_has_field(document, "_id", 50);
            check_has_field(document, "item", 50);
            check_has_field(document, "status", 50);
            check_has_no_field(document, "size", 50);
            check_has_field(document, "instock", 50);

            auto instock = document["instock"].get_array().value;

            if (std::distance(instock.begin(), instock.end()) != 1) {
                throw std::logic_error("wrong count in example 50");
            }
        }
    }
}

void projection_with_aggregation_example(mongocxx::database db) {
    {
        if (!version_at_least(db, 4, 4, 0)) {
            return;
        }

        projection_insertion_example(db);

        // Start Aggregation Projection Example 1
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_array;
        using bsoncxx::builder::basic::make_document;

        auto cursor = db["inventory"].find(
            make_document(),
            mongocxx::options::find{}.projection(make_document(
                kvp("_id", 0),
                kvp("item", 1),
                kvp("status",
                    make_document(kvp(
                        "$switch",
                        make_document(
                            kvp("branches",
                                make_array(
                                    make_document(
                                        kvp("case",
                                            make_document(kvp("$eq", make_array("$status", "A")))),
                                        kvp("then", "Available")),
                                    make_document(
                                        kvp("case",
                                            make_document(kvp("$eq", make_array("$status", "D")))),
                                        kvp("then", "Discontinued")))),
                            kvp("default", "No status found"))))),
                kvp("area",
                    make_document(kvp(
                        "$concat",
                        make_array(
                            make_document(kvp(
                                "$toString",
                                make_document(kvp("$multiply", make_array("$size.h", "$size.w"))))),
                            " ",
                            "$size.uom")))),
                kvp("reportNumber", make_document(kvp("$literal", 1))))));
        // End Aggregation Projection Example 1

        for (auto&& document : cursor) {
            check_has_no_field(document, "_id", 1, "aggregation projection");
            check_has_field(document, "item", 1, "aggregation projection");
            check_has_field(document, "status", 1, "aggregation projection");
            check_has_no_field(document, "size", 1, "aggregation projection");
            check_has_no_field(document, "instock", 1, "aggregation projection");
            check_has_field(document, "area", 1, "aggregation projection");
            check_has_field(document, "reportNumber", 1, "aggregation projection");
        }
    }
}

void update_examples(mongocxx::database db) {
    db["inventory"].drop();

    {
        // Start Example 51
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        std::vector<bsoncxx::document::value> docs{
            make_document(
                kvp("item", "canvas"),
                kvp("qty", 100),
                kvp("size", make_document(kvp("h", 28), kvp("w", 35.5), kvp("uom", "cm"))),
                kvp("status", "A")),
            make_document(kvp("item", "journal"),
                          kvp("qty", 25),
                          kvp("size", make_document(kvp("h", 14), kvp("w", 21), kvp("uom", "cm"))),
                          kvp("status", "A")),
            make_document(
                kvp("item", "mat"),
                kvp("qty", 85),
                kvp("size", make_document(kvp("h", 27.9), kvp("w", 35.5), kvp("uom", "cm"))),
                kvp("status", "A")),
            make_document(
                kvp("item", "mousepad"),
                kvp("qty", 25),
                kvp("size", make_document(kvp("h", 19), kvp("w", 22.85), kvp("uom", "cm"))),
                kvp("status", "P")),
            make_document(kvp("item", "notebook"),
                          kvp("qty", 50),
                          kvp("size", make_document(kvp("h", 8.5), kvp("w", 11), kvp("uom", "in"))),
                          kvp("status", "P")),
            make_document(kvp("item", "paper"),
                          kvp("qty", 100),
                          kvp("size", make_document(kvp("h", 8.5), kvp("w", 11), kvp("uom", "in"))),
                          kvp("status", "D")),
            make_document(
                kvp("item", "planner"),
                kvp("qty", 75),
                kvp("size", make_document(kvp("h", 22.85), kvp("w", 30), kvp("uom", "cm"))),
                kvp("status", "D")),
            make_document(
                kvp("item", "postcard"),
                kvp("qty", 45),
                kvp("size", make_document(kvp("h", 10), kvp("w", 15.25), kvp("uom", "cm"))),
                kvp("status", "A")),
            make_document(kvp("item", "sketchbook"),
                          kvp("qty", 80),
                          kvp("size", make_document(kvp("h", 14), kvp("w", 21), kvp("uom", "cm"))),
                          kvp("status", "A")),
            make_document(
                kvp("item", "sketch pad"),
                kvp("qty", 95),
                kvp("size", make_document(kvp("h", 22.85), kvp("w", 30.5), kvp("uom", "cm"))),
                kvp("status", "A")),
        };

        db["inventory"].insert_many(docs);
        // End Example 51

        if (db["inventory"].count_documents({}) != 10) {
            throw std::logic_error("wrong count in example 51");
        }
    }

    {
        // Start Example 52
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        db["inventory"].update_one(
            make_document(kvp("item", "paper")),
            make_document(kvp("$set", make_document(kvp("size.uom", "cm"), kvp("status", "P"))),
                          kvp("$currentDate", make_document(kvp("lastModified", true)))));
        // End Example 52

        for (auto&& document : db["inventory"].find(make_document(kvp("item", "paper")))) {
            if (document["size"].get_document().value["uom"].get_string().value !=
                bsoncxx::stdx::string_view{"cm"}) {
                throw std::logic_error("error in example 52");
            }
            if (document["status"].get_string().value != bsoncxx::stdx::string_view{"P"}) {
                throw std::logic_error("error in example 52");
            }
            check_has_field(document, "lastModified", 52);
        }
    }

    {
        // Start Example 53
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        db["inventory"].update_many(
            make_document(kvp("qty", make_document((kvp("$lt", 50))))),
            make_document(kvp("$set", make_document(kvp("size.uom", "in"), kvp("status", "P"))),
                          kvp("$currentDate", make_document(kvp("lastModified", true)))));
        // End Example 53

        for (auto&& document :
             db["inventory"].find(make_document(kvp("qty", make_document(kvp("$lt", 50)))))) {
            if (document["size"].get_document().value["uom"].get_string().value !=
                bsoncxx::stdx::string_view{"in"}) {
                throw std::logic_error("error in example 53");
            }
            if (document["status"].get_string().value != bsoncxx::stdx::string_view{"P"}) {
                throw std::logic_error("error in example 53");
            }
            check_has_field(document, "lastModified", 53);
        }
    }

    {
        // Start Example 54
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_array;
        using bsoncxx::builder::basic::make_document;

        db["inventory"].replace_one(
            make_document(kvp("item", "paper")),
            make_document(kvp("item", "paper"),
                          kvp("instock",
                              make_array(make_document(kvp("warehouse", "A"), kvp("qty", 60)),
                                         make_document(kvp("warehouse", "B"), kvp("qty", 40))))));
        // End Example 54

        for (auto&& document : db["inventory"].find(make_document(kvp("item", "paper")))) {
            if (std::distance(document.begin(), document.end()) != 3) {
                throw std::logic_error("wrong count in example 54");
            }
            check_has_field(document, "_id", 54);
            check_has_field(document, "item", 54);
            check_has_field(document, "instock", 54);

            auto instock = document["instock"].get_array().value;

            if (std::distance(instock.begin(), instock.end()) != 2) {
                throw std::logic_error("wrong count in example 54");
            }
        }
    }
}

void delete_examples(mongocxx::database db) {
    db["inventory"].drop();

    {
        // Start Example 55
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        std::vector<bsoncxx::document::value> docs{
            make_document(kvp("item", "journal"),
                          kvp("qty", 25),
                          kvp("size", make_document(kvp("h", 14), kvp("w", 21), kvp("uom", "cm"))),
                          kvp("status", "A")),
            make_document(kvp("item", "notebook"),
                          kvp("qty", 50),
                          kvp("size", make_document(kvp("h", 8.5), kvp("w", 11), kvp("uom", "in"))),
                          kvp("status", "P")),
            make_document(kvp("item", "paper"),
                          kvp("qty", 100),
                          kvp("size", make_document(kvp("h", 8.5), kvp("w", 11), kvp("uom", "in"))),
                          kvp("status", "D")),
            make_document(
                kvp("item", "planner"),
                kvp("qty", 75),
                kvp("size", make_document(kvp("h", 22.85), kvp("w", 30), kvp("uom", "cm"))),
                kvp("status", "D")),
            make_document(
                kvp("item", "postcard"),
                kvp("qty", 45),
                kvp("size", make_document(kvp("h", 10), kvp("w", 15.25), kvp("uom", "cm"))),
                kvp("status", "A")),
        };

        db["inventory"].insert_many(docs);
        // End Example 55

        if (db["inventory"].count_documents({}) != 5) {
            throw std::logic_error("wrong count in example 55");
        }
    }

    {
        // Start Example 57
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        db["inventory"].delete_many(make_document(kvp("status", "A")));
        // End Example 57

        if (db["inventory"].count_documents({}) != 3) {
            throw std::logic_error("wrong count in example 57");
        }
    }

    {
        // Start Example 58
        using bsoncxx::builder::basic::kvp;
        using bsoncxx::builder::basic::make_document;

        db["inventory"].delete_one(make_document(kvp("status", "D")));
        // End Example 58

        if (db["inventory"].count_documents({}) != 2) {
            throw std::logic_error("wrong count in example 58");
        }
    }

    {
        // Start Example 56
        db["inventory"].delete_many({});
        // End Example 56

        if (db["inventory"].count_documents({}) != 0) {
            throw std::logic_error("wrong count in example 56");
        }
    }
}

static bool is_snapshot_ready(mongocxx::client& client, mongocxx::collection& collection) {
    auto opts = mongocxx::options::client_session{};
    opts.snapshot(true);

    auto session = client.start_session(opts);
    try {
        auto maybe_value = collection.find_one(session, {});
        if (maybe_value) {
            return true;
        }
        return false;
    } catch (const mongocxx::operation_exception& e) {
        if (e.code().value() == 246) {  // snapshot unavailable
            return false;
        }
        throw;
    }
    return true;
}

// Seed the pets database and wait for the snapshot to become available.
// This follows the pattern from the Python driver as seen below:
// https://github.com/mongodb/mongo-python-driver/commit/e325b24b78e431cb889c5902d00b8f4af2c700c3#diff-c5d782e261f04fca18024ab18c3ed38fb45ede24cde4f9092e012f6fcbbe0df5R1368
static void wait_for_snapshot_ready(mongocxx::client& client,
                                    std::vector<mongocxx::collection> collections) {
    size_t sleep_time = 1;

    for (;;) {
        bool is_ready = true;
        for (auto& collection : collections) {
            if (!is_snapshot_ready(client, collection)) {
                is_ready = false;
                break;  // inner
            }
        }
        if (is_ready) {
            break;  // outer
        } else {
            std::this_thread::sleep_for(std::chrono::seconds(sleep_time++));
        }
    }
}

static void setup_pets(mongocxx::client& client) {
    using namespace mongocxx;
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;

    auto db = client["pets"];
    db.drop();
    db["cats"].insert_one(make_document(kvp("adoptable", true)));
    db["dogs"].insert_one(make_document(kvp("adoptable", true)));
    db["dogs"].insert_one(make_document(kvp("adoptable", false)));
    wait_for_snapshot_ready(client, {db["cats"], db["dogs"]});
}

static void snapshot_example1(mongocxx::client& client) {
    setup_pets(client);

    // Start Snapshot Query Example 1
    using namespace mongocxx;
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;

    auto db = client["pets"];

    int64_t adoptable_pets_count = 0;

    auto opts = mongocxx::options::client_session{};
    opts.snapshot(true);
    auto session = client.start_session(opts);

    {
        pipeline p;

        p.match(make_document(kvp("adoptable", true))).count("adoptableCatsCount");
        auto cursor = db["cats"].aggregate(session, p);

        for (auto doc : cursor) {
            adoptable_pets_count += doc.find("adoptableCatsCount")->get_int32();
        }
    }

    {
        pipeline p;

        p.match(make_document(kvp("adoptable", true))).count("adoptableDogsCount");
        auto cursor = db["dogs"].aggregate(session, p);

        for (auto doc : cursor) {
            adoptable_pets_count += doc.find("adoptableDogsCount")->get_int32();
        }
    }

    // End Snapshot Query Example 1

    if (adoptable_pets_count != 2) {
        throw std::logic_error(
            "wrong number of adoptable pets in Snapshot Query Example 1, expecting 2 got: " +
            std::to_string(adoptable_pets_count));
    }
}

static void setup_retail(mongocxx::client& client) {
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;
    using bsoncxx::types::b_date;
    using std::chrono::system_clock;

    auto db = client["retail"];
    db.drop();
    b_date sales_date{system_clock::now()};
    db["sales"].insert_one(
        make_document(kvp("shoeType", "boot"), kvp("price", 30), kvp("saleDate", sales_date)));
    wait_for_snapshot_ready(client, {db["sales"]});
}

static void snapshot_example2(mongocxx::client& client) {
    setup_retail(client);

    // Start Snapshot Query Example 2
    using namespace mongocxx;
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_array;
    using bsoncxx::builder::basic::make_document;

    auto opts = mongocxx::options::client_session{};
    opts.snapshot(true);
    auto session = client.start_session(opts);

    auto db = client["retail"];

    pipeline p;

    p.match(make_document(kvp("$expr",
                              make_document(kvp("$gt",
                                                make_array("$saleDate",
                                                           make_document(kvp("startDate", "$$NOW"),
                                                                         kvp("unit", "day"),
                                                                         kvp("amount", 1))))))))
        .count("totalDailySales");

    auto cursor = db["sales"].aggregate(session, p);

    auto doc = *cursor.begin();
    auto total_daily_sales = doc.find("totalDailySales")->get_int32();

    // End Snapshot Query Example 2
    if (total_daily_sales != 1) {
        throw std::logic_error("wrong number of total sales in example 60, expecting 1 got: " +
                               std::to_string(total_daily_sales));
    }
}

// https://jira.mongodb.com/browse/CXX-2505
static void queryable_encryption_api(mongocxx::client& client) {
    // Start Queryable Encryption Example
    using namespace mongocxx;
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_array;
    using bsoncxx::builder::basic::make_document;
    using bsoncxx::types::bson_value::value;

    // Drop data from prior test runs.
    client["keyvault"]["datakeys"].drop();
    client["docsExamples"].drop();

    // Create two data keys.
    class client key_vault_client {
        uri{}, add_test_server_api(),
    };

    options::client_encryption ce_opts;
    ce_opts.key_vault_client(&key_vault_client);
    ce_opts.key_vault_namespace({"keyvault", "datakeys"});
    ce_opts.kms_providers(_make_kms_doc(false));
    client_encryption client_encryption(std::move(ce_opts));

    auto key1_id = client_encryption.create_data_key("local");
    auto key2_id = client_encryption.create_data_key("local");

    // Create an encryptedFieldsMap.
    auto encrypted_fields_map = make_document(kvp(
        "docsExamples.encrypted",
        make_document(kvp(
            "fields",
            make_array(make_document(kvp("path", "encryptedIndexed"),
                                     kvp("bsonType", "string"),
                                     kvp("keyId", key1_id),
                                     kvp("queries", make_document(kvp("queryType", "equality")))),
                       make_document(kvp("path", "encryptedUnindexed"),
                                     kvp("bsonType", "string"),
                                     kvp("keyId", key2_id)))))));

    // Create an Queryable Encryption collection.
    options::auto_encryption auto_encrypt_opts{};
    auto_encrypt_opts.key_vault_namespace({"keyvault", "datakeys"});
    auto_encrypt_opts.kms_providers(_make_kms_doc(false));
    auto_encrypt_opts.encrypted_fields_map(encrypted_fields_map.view());

    // Optional, If mongocryptd is not in PATH, then find the binary at MONGOCRYPTD_PATH.
    char* mongocryptd_path = std::getenv("MONGOCRYPTD_PATH");
    if (mongocryptd_path) {
        auto_encrypt_opts.extra_options(
            make_document(kvp("mongocryptdSpawnPath", mongocryptd_path)));
    }

    mongocxx::options::client encrypted_client_opts;
    encrypted_client_opts.auto_encryption_opts(std::move(auto_encrypt_opts));
    class client encrypted_client {
        uri{}, add_test_server_api(encrypted_client_opts)
    };

    // Create the Queryable Encryption collection docsExample.encrypted.
    // Because docsExample.encrypted is in encryptedFieldsMap, it is created with Queryable
    // Encryption support.
    auto db = encrypted_client["docsExamples"];
    db.create_collection("encrypted");
    auto encrypted_collection = db["encrypted"];

    // Auto encrypt an insert and find.
    {
        // Encrypt an insert.
        encrypted_collection.insert_one(make_document(kvp("_id", 1),
                                                      kvp("encryptedIndexed", "indexedValue"),
                                                      kvp("encryptedUnindexed", "unindexedValue")));

        // Encrypt a find.
        auto res =
            encrypted_collection.find_one(make_document(kvp("encryptedIndexed", "indexedValue")));

        auto doc = res.value();

        if (doc["encryptedIndexed"] != value("indexedValue")) {
            throw std::logic_error("expected 'indexedValue'");
        }
        if (doc["encryptedUnindexed"] != value("unindexedValue")) {
            throw std::logic_error("expected 'unindexedValue'");
        }
    }

    // Find documents without decryption.
    {
        auto unencrypted_collection = client["docsExamples"]["encrypted"];
        auto res = unencrypted_collection.find_one(make_document(kvp("_id", 1)));
        auto doc = res.value();
        {
            auto val = doc["encryptedIndexed"];
            if (val.type() != bsoncxx::type::k_binary) {
                throw std::logic_error("expected encryptedIndexed to be Binary");
            }
        }
        {
            auto val = doc["encryptedUnindexed"];
            if (val.type() != bsoncxx::type::k_binary) {
                throw std::logic_error("expected encryptedUnindexed to be Binary");
            }
        }
    }
    // End Queryable Encryption Example
}

int main() {
    // The mongocxx::instance constructor and destructor initialize and shut down the driver,
    // respectively. Therefore, a mongocxx::instance must be created before using the driver and
    // must remain alive for as long as the driver is in use.
    mongocxx::instance inst{};

    mongocxx::client conn{mongocxx::uri{}};
    auto db = conn["documentation_examples"];

    try {
        insert_examples(db);
        query_top_level_fields_examples(db);
        query_embedded_documents_examples(db);
        query_arrays_examples(db);
        query_array_embedded_documents_examples(db);
        query_null_missing_fields_examples(db);
        projection_examples(db);
        projection_with_aggregation_example(db);
        update_examples(db);
        delete_examples(db);
        if (is_replica_set(conn) && version_at_least(db, 5, 0, 0)) {
            snapshot_example1(conn);
            snapshot_example2(conn);
        }
        if (should_run_client_side_encryption_test() && is_replica_set(conn) &&
            version_at_least(db, 7, 0, 0)) {
            queryable_encryption_api(conn);
        }
    } catch (const std::logic_error& e) {
        std::cerr << e.what() << std::endl;
        return EXIT_FAILURE;
    }
}
