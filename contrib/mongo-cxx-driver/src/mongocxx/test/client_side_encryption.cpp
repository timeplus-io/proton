// Copyright 2020 MongoDB Inc.
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

#include <fstream>
#include <sstream>
#include <string>
#include <tuple>

#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include <bsoncxx/document/element.hpp>
#include <bsoncxx/stdx/make_unique.hpp>
#include <bsoncxx/stdx/string_view.hpp>
#include <bsoncxx/test/catch.hh>
#include <bsoncxx/types.hpp>
#include <bsoncxx/types/bson_value/make_value.hpp>
#include <bsoncxx/types/bson_value/value.hpp>
#include <bsoncxx/types/bson_value/view.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/client_encryption.hpp>
#include <mongocxx/exception/error_code.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/client.hpp>
#include <mongocxx/options/client_encryption.hpp>
#include <mongocxx/options/data_key.hpp>
#include <mongocxx/private/libbson.hh>
#include <mongocxx/test/client_helpers.hh>
#include <mongocxx/test/spec/monitoring.hh>
#include <mongocxx/uri.hpp>
#include <mongocxx/write_concern.hpp>
#include <third_party/catch/include/helpers.hpp>

#include <mongocxx/config/prelude.hpp>

namespace {
const auto kLocalMasterKey =
    "\x32\x78\x34\x34\x2b\x78\x64\x75\x54\x61\x42\x42\x6b\x59\x31\x36\x45\x72"
    "\x35\x44\x75\x41\x44\x61\x67\x68\x76\x53\x34\x76\x77\x64\x6b\x67\x38\x74"
    "\x70\x50\x70\x33\x74\x7a\x36\x67\x56\x30\x31\x41\x31\x43\x77\x62\x44\x39"
    "\x69\x74\x51\x32\x48\x46\x44\x67\x50\x57\x4f\x70\x38\x65\x4d\x61\x43\x31"
    "\x4f\x69\x37\x36\x36\x4a\x7a\x58\x5a\x42\x64\x42\x64\x62\x64\x4d\x75\x72"
    "\x64\x6f\x6e\x4a\x31\x64";

// This is the base64 encoding of LOCALAAAAAAAAAAAAAAAAA==.
const auto kLocalKeyUUID = "\x2c\xe0\x80\x2c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";

// This is the base64 encoding of AWSAAAAAAAAAAAAAAAAAAA==.
const auto kAwsKeyUUID = "\x01\x64\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";

// This is the base64 encoding of AZUREAAAAAAAAAAAAAAAAA==.
const auto kAzureKeyUUID = "\x01\x95\x11\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";

// This is the base64 encoding of GCPAAAAAAAAAAAAAAAAAAA==.
const auto kGcpKeyUUID = "\x18\x23\xc0\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";

// This is the base64 encoding of KMIPAAAAAAAAAAAAAAAAAA==.
const auto kKmipKeyUUID = "\x28\xc2\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";

using bsoncxx::builder::concatenate;

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_array;
using bsoncxx::builder::basic::make_document;

using bsoncxx::builder::basic::sub_document;
using bsoncxx::builder::stream::close_array;
using bsoncxx::builder::stream::close_document;
using bsoncxx::builder::stream::document;
using bsoncxx::builder::stream::finalize;
using bsoncxx::builder::stream::open_array;
using bsoncxx::builder::stream::open_document;

using bsoncxx::types::bson_value::make_value;

using namespace mongocxx;

// Takes a path relative to the CLIENT_SIDE_ENCRYPTION_TESTS_PATH variable, with leading '/'.
bsoncxx::document::value _doc_from_file(stdx::string_view sub_path) {
    char* encryption_tests_path = std::getenv("CLIENT_SIDE_ENCRYPTION_TESTS_PATH");
    REQUIRE(encryption_tests_path);

    std::string path = std::string(encryption_tests_path) + sub_path.data();
    CAPTURE(path);

    std::ifstream file{path};
    REQUIRE(file.is_open());

    std::string file_contents((std::istreambuf_iterator<char>(file)),
                              std::istreambuf_iterator<char>());

    return bsoncxx::from_json(file_contents);
}

void _setup_drop_collections(const client& client) {
    write_concern wc_majority;
    wc_majority.acknowledge_level(write_concern::level::k_majority);

    auto datakeys = client["keyvault"]["datakeys"];
    datakeys.drop(wc_majority);

    auto coll = client["db"]["coll"];
    coll.drop(wc_majority);
}
// Returns a document with credentials for KMS providers.
// If include_external is true, all KMS providers are set.
// If include_external is false, only the local provider is set.
bsoncxx::document::value _make_kms_doc(bool include_external = true) {
    auto kms_doc = bsoncxx::builder::basic::document{};

    if (include_external) {
        kms_doc.append(kvp("aws", [&](sub_document subdoc) {
            subdoc.append(kvp("secretAccessKey",
                              test_util::getenv_or_fail("MONGOCXX_TEST_AWS_SECRET_ACCESS_KEY")));
            subdoc.append(
                kvp("accessKeyId", test_util::getenv_or_fail("MONGOCXX_TEST_AWS_ACCESS_KEY_ID")));
        }));

        kms_doc.append(kvp("azure", [&](sub_document subdoc) {
            subdoc.append(
                kvp("tenantId", test_util::getenv_or_fail("MONGOCXX_TEST_AZURE_TENANT_ID")));
            subdoc.append(
                kvp("clientId", test_util::getenv_or_fail("MONGOCXX_TEST_AZURE_CLIENT_ID")));
            subdoc.append(kvp("clientSecret",
                              test_util::getenv_or_fail("MONGOCXX_TEST_AZURE_CLIENT_SECRET")));
        }));

        kms_doc.append(kvp("gcp", [&](sub_document subdoc) {
            subdoc.append(kvp("email", test_util::getenv_or_fail("MONGOCXX_TEST_GCP_EMAIL")));
            subdoc.append(
                kvp("privateKey", test_util::getenv_or_fail("MONGOCXX_TEST_GCP_PRIVATEKEY")));
        }));

        kms_doc.append(kvp("kmip", [&](sub_document subdoc) {
            subdoc.append(kvp("endpoint", "localhost:5698"));
        }));
    }

    char key_storage[96];
    memcpy(&(key_storage[0]), kLocalMasterKey, 96);

    bsoncxx::types::b_binary local_master_key{
        bsoncxx::binary_sub_type::k_binary, 96, (const uint8_t*)&key_storage};

    kms_doc.append(
        kvp("local", [&](sub_document subdoc) { subdoc.append(kvp("key", local_master_key)); }));

    return {kms_doc.extract()};
}

bsoncxx::document::value _make_tls_opts() {
    bsoncxx::builder::basic::document tls_opts;

    tls_opts.append(kvp("kmip", [&](sub_document subdoc) {
        subdoc.append(
            kvp("tlsCAFile", test_util::getenv_or_fail("MONGOCXX_TEST_CSFLE_TLS_CA_FILE")));
        subdoc.append(
            kvp("tlsCertificateKeyFile",
                test_util::getenv_or_fail("MONGOCXX_TEST_CSFLE_TLS_CERTIFICATE_KEY_FILE")));
    }));

    return tls_opts.extract();
}

void _add_client_encrypted_opts(options::client* client_opts,
                                bsoncxx::document::view_or_value schema_map,
                                bsoncxx::document::view_or_value kms_doc,
                                bsoncxx::document::view_or_value tls_opts,
                                mongocxx::client* key_vault_client = nullptr) {
    options::auto_encryption auto_encrypt_opts{};

    // KMS
    auto_encrypt_opts.kms_providers(std::move(kms_doc));
    auto_encrypt_opts.tls_opts(std::move(tls_opts));
    auto_encrypt_opts.key_vault_namespace({"keyvault", "datakeys"});

    if (!schema_map.view().empty()) {
        auto_encrypt_opts.schema_map({std::move(schema_map)});
    }

    // For evergreen testing
    char* bypass_spawn = std::getenv("ENCRYPTION_TESTS_BYPASS_SPAWN");
    char* mongocryptd_path = std::getenv("MONGOCRYPTD_PATH");

    const auto shared_lib_path = std::getenv("CRYPT_SHARED_LIB_PATH");
    if (shared_lib_path) {
        auto_encrypt_opts.extra_options(make_document(kvp("cryptSharedLibPath", shared_lib_path),
                                                      kvp("cryptSharedLibRequired", true)));
    } else if (bypass_spawn || mongocryptd_path) {
        auto cmd = bsoncxx::builder::basic::document{};

        if (bypass_spawn && strcmp(bypass_spawn, "TRUE") == 0) {
            cmd.append(bsoncxx::builder::basic::kvp("mongocryptdBypassSpawn", true));
        }

        if (mongocryptd_path) {
            cmd.append(bsoncxx::builder::basic::kvp("mongocryptdSpawnPath", mongocryptd_path));
        }

        auto_encrypt_opts.extra_options({cmd.extract()});
    }

    if (key_vault_client) {
        auto_encrypt_opts.key_vault_client(key_vault_client);
    }

    client_opts->auto_encryption_opts(std::move(auto_encrypt_opts));
}

void _add_cse_opts(options::client_encryption* opts,
                   mongocxx::client* client,
                   bool include_aws = true) {
    // KMS providers
    opts->kms_providers(_make_kms_doc(include_aws));

    // TLS options
    opts->tls_opts(_make_tls_opts());

    // Key vault client
    opts->key_vault_client(client);

    // Key vault namespace
    opts->key_vault_namespace({"keyvault", "datakeys"});
}

template <typename Callable>
void run_datakey_and_double_encryption(Callable create_data_key,
                                       stdx::string_view provider,
                                       client* setup_client,
                                       client* client_encrypted,
                                       client_encryption* client_encryption,
                                       mongocxx::spec::apm_checker* apm_checker) {
    // Test creating and using data keys:
    INFO("using KMS provider: " << provider);

    // 1. Call client_encryption.createDataKey()
    auto datakey_id = create_data_key();

    // Expect a BSON binary with subtype 4 to be returned, referred to as datakey_id.
    REQUIRE(datakey_id.view().type() == bsoncxx::type::k_binary);
    REQUIRE(datakey_id.view().get_binary().sub_type == bsoncxx::binary_sub_type::k_uuid);

    // Use client to run a find on keyvault.datakeys by querying with the _id
    // set to the datakey_id.
    auto datakeys = setup_client->database("keyvault").collection("datakeys");
    mongocxx::read_concern rc_majority;
    rc_majority.acknowledge_level(mongocxx::read_concern::level::k_majority);
    datakeys.read_concern(rc_majority);
    auto query = make_document(kvp("_id", datakey_id));
    auto cursor = datakeys.find(query.view());

    // Expect that exactly one document is returned with the correct "masterKey.provider"
    std::size_t i = 0;
    for (auto&& doc : cursor) {
        REQUIRE(doc["masterKey"]["provider"].get_string().value == provider);
        i++;
    }

    REQUIRE(i == 1);

    // Check that client captured a command_started event for the insert command containing a
    // majority writeConcern.
    auto event = document{} << "command_started_event" << open_document << "command"
                            << open_document << "insert"
                            << "datakeys"
                            << "writeConcern" << open_document << "w"
                            << "majority" << close_document << close_document << close_document
                            << finalize;

    auto arr = bsoncxx::builder::basic::array{};
    arr.append(event);
    apm_checker->has(arr.view());
    apm_checker->clear();

    // 2. Call client_encryption.encrypt() with the value "hello there", the algorithm
    // AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic, and the key_id of datakey_id
    options::encrypt opts{};
    opts.algorithm(options::encrypt::encryption_algorithm::k_deterministic);
    opts.key_id(std::move(datakey_id));

    auto to_encrypt = make_value("hello there");
    auto encrypted_val = client_encryption->encrypt(to_encrypt.view(), opts);

    // Expect the return value to be a BSON binary subtype 6, referred to as encrypted
    auto encrypted = encrypted_val.view();
    REQUIRE(encrypted.type() == bsoncxx::type::k_binary);
    REQUIRE(encrypted.get_binary().sub_type == bsoncxx::binary_sub_type::k_encrypted);

    // Use client_encrypted to insert { _id: provider, "value": <encrypted> } into db.coll
    auto insert_doc = make_document(kvp("_id", provider), kvp("value", encrypted));
    client_encrypted->database("db").collection("coll").insert_one(insert_doc.view());

    // Use client_encrypted to run a find querying with _id of provider and expect value
    // to be "hello there"
    auto filter = make_document(kvp("_id", provider));
    auto res = client_encrypted->database("db").collection("coll").find_one(filter.view());
    REQUIRE(res);
    auto decrypted_bson_val = res->view()["value"];
    REQUIRE(decrypted_bson_val.type() == bsoncxx::type::k_string);
    REQUIRE(decrypted_bson_val.get_string().value == stdx::string_view{"hello there"});

    // 3. Call client_encryption.encrypt() with the value "hello there", the algorithm
    // AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic, and the key_alt_name of provider_altname
    options::encrypt opts2{};
    opts2.algorithm(options::encrypt::encryption_algorithm::k_deterministic);
    std::string altname(provider);
    altname += "_altname";
    opts2.key_alt_name(altname);

    auto encrypted_val2 = client_encryption->encrypt(to_encrypt.view(), opts2);

    // Expect the return value to be a BSON binary subtype 6. Expect the value to exactly
    // match the value of "encrypted"
    auto encrypted2 = encrypted_val2.view();
    REQUIRE(encrypted2.type() == bsoncxx::type::k_binary);
    REQUIRE(encrypted2.get_binary().sub_type == bsoncxx::binary_sub_type::k_encrypted);
    REQUIRE(encrypted == encrypted2);

    // Then, test explicit encryption on an auto encrypted field:
    // Use client_encrypted to attempt to insert { "encrypted_placeholder": (encrypted2) }
    // Expect an exception to be thrown, since this is an attempt to auto encrypt an already
    // encrypted value.
    auto double_encrypted = make_document(kvp("encrypted_placeholder", encrypted2));
    REQUIRE_THROWS(
        client_encrypted->database("db").collection("coll").insert_one(double_encrypted.view()));
}

TEST_CASE("Datakey and double encryption", "[client_side_encryption]") {
    instance::current();

    // Setup
    // 1. Create a mongoclient without encryption
    options::client client_opts;
    spec::apm_checker apm_checker;
    client_opts.apm_opts(apm_checker.get_apm_opts(true /* command_started_events_only */));

    mongocxx::client setup_client{uri{}, test_util::add_test_server_api(client_opts)};

    if (!mongocxx::test_util::should_run_client_side_encryption_test()) {
        return;
    }

    if (test_util::get_max_wire_version(setup_client) < 8) {
        // Automatic encryption requires wire version 8.
        WARN("Skipping - max wire version is < 8");
        return;
    }

    // 2. Drop keyvault.datakeys and db.coll
    _setup_drop_collections(setup_client);

    // 3. Create and configure client_encrypted, client_encryption.
    options::client encrypted_client_opts;
    _add_client_encrypted_opts(
        &encrypted_client_opts,
        document() << "db.coll" << open_document << "bsonType"
                   << "object"
                   << "properties" << open_document << "encrypted_placeholder" << open_document
                   << "encrypt" << open_document << "keyId"
                   << "/placeholder"
                   << "bsonType"
                   << "string"
                   << "algorithm"
                   << "AEAD_AES_256_CBC_HMAC_SHA_512-Random" << close_document << close_document
                   << close_document << close_document << finalize,
        _make_kms_doc(),
        _make_tls_opts());

    mongocxx::client client_encrypted{
        uri{},
        test_util::add_test_server_api(encrypted_client_opts),
    };

    // Configure both with aws and local KMS providers, and schema map
    options::client_encryption cse_opts;
    _add_cse_opts(&cse_opts, &setup_client);
    client_encryption client_encryption{std::move(cse_opts)};

    // Run test with local
    run_datakey_and_double_encryption(
        [&]() {
            // 1. Call client_encryption.createDataKey() with the local KMS provider
            // and keyAltNames set to ["local_altname"].
            options::data_key data_key_opts;
            data_key_opts.key_alt_names({"local_altname"});
            return client_encryption.create_data_key("local", data_key_opts);
        },
        "local",
        &setup_client,
        &client_encrypted,
        &client_encryption,
        &apm_checker);

    // Run with AWS
    run_datakey_and_double_encryption(
        [&]() {
            // 1. Call client_encryption.createDataKey() with the aws KMS provider
            // and keyAltNames set to ["aws_altname"].
            options::data_key data_key_opts;
            data_key_opts.key_alt_names({"aws_altname"});

            auto doc = make_document(
                kvp("region", "us-east-1"),
                kvp("key",
                    "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"));
            data_key_opts.master_key(doc.view());

            return client_encryption.create_data_key("aws", data_key_opts);
        },
        "aws",
        &setup_client,
        &client_encrypted,
        &client_encryption,
        &apm_checker);

    // Run with Azure
    run_datakey_and_double_encryption(
        [&]() {
            // 1. Call client_encryption.createDataKey() with the azure KMS provider
            // and keyAltNames set to ["azure_altname"].
            options::data_key data_key_opts;
            data_key_opts.key_alt_names({"azure_altname"});

            auto doc = make_document(kvp("keyVaultEndpoint", "key-vault-csfle.vault.azure.net"),
                                     kvp("keyName", "key-name-csfle"));
            data_key_opts.master_key(doc.view());

            return client_encryption.create_data_key("azure", data_key_opts);
        },
        "azure",
        &setup_client,
        &client_encrypted,
        &client_encryption,
        &apm_checker);

    // Run with GCP
    run_datakey_and_double_encryption(
        [&]() {
            // 1. Call client_encryption.createDataKey() with the gcp KMS provider
            // and keyAltNames set to ["gcp_altname"].
            options::data_key data_key_opts;
            data_key_opts.key_alt_names({"gcp_altname"});

            auto doc = make_document(kvp("projectId", "devprod-drivers"),
                                     kvp("location", "global"),
                                     kvp("keyRing", "key-ring-csfle"),
                                     kvp("keyName", "key-name-csfle"));
            data_key_opts.master_key(doc.view());

            return client_encryption.create_data_key("gcp", data_key_opts);
        },
        "gcp",
        &setup_client,
        &client_encrypted,
        &client_encryption,
        &apm_checker);

    // Run with KMIP
    run_datakey_and_double_encryption(
        [&]() {
            // 1. Call client_encryption.createDataKey() with the KMIP KMS provider
            // and keyAltNames set to ["kmip_altname"].
            options::data_key data_key_opts;
            data_key_opts.key_alt_names({"kmip_altname"});

            auto doc = make_document();
            data_key_opts.master_key(doc.view());

            return client_encryption.create_data_key("kmip", data_key_opts);
        },
        "kmip",
        &setup_client,
        &client_encrypted,
        &client_encryption,
        &apm_checker);
}

void run_external_key_vault_test(bool with_external_key_vault) {
    mongocxx::client external_key_vault_client{
        uri{"mongodb://fake-user:fake-pwd@localhost:27017"},
        test_util::add_test_server_api(),
    };

    // Create a MongoClient without encryption enabled (referred to as client).
    mongocxx::client client{
        uri{},
        test_util::add_test_server_api(),
    };

    // Using client, drop the collections keyvault.datakeys and db.coll.
    _setup_drop_collections(client);

    // Insert the document external/external-key.json into keyvault.datakeys.
    auto datakeys = client["keyvault"]["datakeys"];

    write_concern wc_majority;
    wc_majority.acknowledge_level(write_concern::level::k_majority);
    options::insert insert_opts;
    insert_opts.write_concern(std::move(wc_majority));

    auto external_key = _doc_from_file("/external/external-key.json");
    datakeys.insert_one(external_key.view(), insert_opts);

    auto external_schema = _doc_from_file("/external/external-schema.json");
    auto schema_map = make_document(kvp("db.coll", external_schema));

    // Create a MongoClient configured with auto encryption (referred to as client_encrypted),
    // that is configured with an external key vault client if with_external_key_vault is true
    options::client encrypted_client_opts;

    if (with_external_key_vault) {
        _add_client_encrypted_opts(&encrypted_client_opts,
                                   std::move(schema_map),
                                   _make_kms_doc(false),
                                   _make_tls_opts(),
                                   &external_key_vault_client);
    } else {
        _add_client_encrypted_opts(
            &encrypted_client_opts, std::move(schema_map), _make_kms_doc(false), _make_tls_opts());
    }

    mongocxx::client client_encrypted{
        uri{},
        test_util::add_test_server_api(encrypted_client_opts),
    };

    // A ClientEncryption object (referred to as client_encryption) that is configured with an
    // external key vault client if with_external_key_vault is true.
    options::client_encryption cse_opts;
    if (with_external_key_vault) {
        _add_cse_opts(&cse_opts, &external_key_vault_client, false);
    } else {
        // c driver uses "client" here
        _add_cse_opts(&cse_opts, &client, false);
    }

    client_encryption client_encryption{std::move(cse_opts)};

    // Use client_encrypted to insert the document {"encrypted": "test"} into db.coll.
    auto doc = make_document(kvp("encrypted", "test"));
    auto coll = client_encrypted["db"]["coll"];

    // If withExternalKeyVault == true, expect an authentication exception to be thrown.
    // Otherwise, expect the insert to succeed.
    if (with_external_key_vault) {
        REQUIRE_THROWS(coll.insert_one(doc.view()));
    } else {
        coll.insert_one(doc.view());
    }

    // Use client_encryption to explicitly encrypt the string "test" with key ID
    // LOCALAAAAAAAAAAAAAAAAA== and deterministic algorithm. If withExternalKeyVault == true,
    // expect an authentication exception to be thrown. Otherwise, expect the insert to succeed.

    options::encrypt opts;
    auto key_id = external_key.view()["_id"].get_value();
    opts.key_id(key_id);
    opts.algorithm(options::encrypt::encryption_algorithm::k_deterministic);

    auto value = make_value("test");

    if (with_external_key_vault) {
        REQUIRE_THROWS(client_encryption.encrypt(value, opts));
    } else {
        auto res = client_encryption.encrypt(value, opts);
    }
}

TEST_CASE("External key vault", "[client_side_encryption]") {
    instance::current();

    if (!mongocxx::test_util::should_run_client_side_encryption_test()) {
        return;
    }

    mongocxx::client setup_client{
        uri{},
        test_util::add_test_server_api(),
    };

    if (test_util::get_max_wire_version(setup_client) < 8) {
        // Automatic encryption requires wire version 8.
        WARN("Skipping - max wire version is < 8");
        return;
    }

    run_external_key_vault_test(true);
    run_external_key_vault_test(false);
}

TEST_CASE("BSON size limits and batch splitting", "[client_side_encryption]") {
    instance::current();

    if (!mongocxx::test_util::should_run_client_side_encryption_test()) {
        return;
    }

    // Create a MongoClient without encryption enabled (referred to as client).
    mongocxx::client client{
        uri{},
        test_util::add_test_server_api(),
    };

    if (test_util::get_max_wire_version(client) < 8) {
        // Automatic encryption requires wire version 8.
        WARN("Skipping - max wire version is < 8");
        return;
    }

    // Load in json schema limits/limits-schema.json and limits/limits-key.json
    auto limits_schema = _doc_from_file("/limits/limits-schema.json");
    auto limits_key = _doc_from_file("/limits/limits-key.json");

    // Using client, drop db.coll and keyvault.datakeys.
    _setup_drop_collections(client);

    // Create the collection db.coll configured with the JSON schema limits/limits-schema.json.
    auto db = client["db"];
    write_concern wc_majority;
    wc_majority.acknowledge_level(write_concern::level::k_majority);
    db.write_concern(wc_majority);
    auto cmd = document{} << "create"
                          << "coll"
                          << "validator" << open_document << "$jsonSchema" << limits_schema.view()
                          << close_document << finalize;
    db.run_command(cmd.view());

    // Insert the document limits/limits-key.json into keyvault.datakeys.
    auto datakeys = client["keyvault"]["datakeys"];
    datakeys.write_concern(wc_majority);
    datakeys.insert_one(limits_key.view());

    // Create a MongoClient configured with auto encryption (referred to as client_encrypted),
    // with local KMS provider as follows:
    //     { "local": { "key": <base64 decoding of LOCAL_MASTERKEY> } }
    // and with the keyVaultNamespace set to keyvault.datakeys.
    options::client client_encrypted_opts;
    _add_client_encrypted_opts(
        &client_encrypted_opts, limits_schema.view(), _make_kms_doc(false), _make_tls_opts());

    // Add a counter to verify splits
    int n_inserts = 0;
    options::apm apm_opts;
    apm_opts.on_command_started([&](const events::command_started_event& event) {
        if (event.command_name().compare("insert") == 0) {
            n_inserts++;
        }
    });

    client_encrypted_opts.apm_opts(apm_opts);

    mongocxx::client client_encrypted{uri{}, test_util::add_test_server_api(client_encrypted_opts)};

    // Using client_encrypted perform the following operations:
    // Insert { "_id": "over_2mib_under_16mib",
    //          "unencrypted": <the string "a" repeated 2097152 times> }.
    std::string over_2mib_under_16mib(2097152, 'a');
    auto over_2mib_under_16mib_doc = make_document(kvp("_id", "over_2mib_under_16mib"),
                                                   kvp("unencrypted", over_2mib_under_16mib));

    // Expect this to succeed since this is still under the maxBsonObjectSize limit.
    auto coll = client_encrypted["db"]["coll"];
    coll.insert_one(over_2mib_under_16mib_doc.view());
    REQUIRE(n_inserts == 1);

    // Insert the document limits/limits-doc.json concatenated with
    // { "_id": "encryption_exceeds_2mib",
    //   "unencrypted": < the string 'a' repeated (2097152 - 2000) times > }
    // Note: limits-doc.json is a 1005 byte BSON document that encrypts to a ~10,000 byte document.
    std::string encryption_exceeds_2mib(2097152 - 2000, 'a');
    auto limits_doc = _doc_from_file("/limits/limits-doc.json");
    auto encryption_exceeds_2mib_doc = document{} << "_id"
                                                  << "encryption_exceeds_2mib"
                                                  << "unencrypted" << encryption_exceeds_2mib
                                                  << concatenate(limits_doc.view()) << finalize;

    // Expect this to succeed since after encryption this still is below the normal
    // maximum BSON document size. Note, before auto encryption this document is under
    // the 2 MiB limit. After encryption it exceeds the 2 MiB limit, but does NOT exceed
    // the 16 MiB limit.
    n_inserts = 0;
    coll.insert_one(encryption_exceeds_2mib_doc.view());
    REQUIRE(n_inserts == 1);

    // Bulk insert the following:
    // { "_id": "over_2mib_1", "unencrypted": <the string 'a' repeated (2097152) times> }
    // { "_id": "over_2mib_2", "unencrypted": <the string 'a' repeated (2097152) times> }
    auto over_2mib_1 = document{} << "_id"
                                  << "over_2mib_1"
                                  << "unencrypted" << over_2mib_under_16mib << finalize;
    auto over_2mib_2 = document{} << "_id"
                                  << "over_2mib_2"
                                  << "unencrypted" << over_2mib_under_16mib << finalize;

    std::vector<bsoncxx::document::view> docs;
    docs.push_back(std::move(over_2mib_1));
    docs.push_back(std::move(over_2mib_2));

    // Expect the bulk write to succeed and split after first doc (i.e. two inserts occur).
    n_inserts = 0;
    auto res = coll.insert_many(docs);
    REQUIRE(n_inserts == 2);

    // Bulk insert the following:

    // The document limits/limits-doc.json concatenated with
    // { "_id": "encryption_exceeds_2mib_1",
    //   "unencrypted": < the string 'a' repeated (2097152 - 2000) times > }
    // The document limits/limits-doc.json concatenated with
    // { "_id": "encryption_exceeds_2mib_2",
    //   "unencrypted": < the string 'a' repeated (2097152 - 2000) times > }
    docs.clear();
    auto concat_1 = document{} << "_id"
                               << "encryption_exceeds_2mib_1"
                               << "unencrypted" << encryption_exceeds_2mib
                               << concatenate(limits_doc.view()) << finalize;
    auto concat_2 = document{} << "_id"
                               << "encryption_exceeds_2mib_2"
                               << "unencrypted" << encryption_exceeds_2mib
                               << concatenate(limits_doc.view()) << finalize;

    docs.push_back(std::move(concat_1));
    docs.push_back(std::move(concat_2));

    // Expect the bulk write to succeed.
    res = coll.insert_many(docs);

    // Insert { "_id": "under_16mib", "unencrypted": <the string 'a' repeated 16777216 - 2000 times>
    // }
    std::string under_16mib(16777216 - 2000, 'a');
    auto under_16mib_doc = document{} << "_id"
                                      << "under_16mib"
                                      << "unencrypted" << under_16mib << finalize;

    // Expect this to succeed since this is still (just) under the maxBsonObjectSize limit.
    n_inserts = 0;
    coll.insert_one(under_16mib_doc.view());
    REQUIRE(n_inserts == 1);

    // Insert the document limits/limits-doc.json concatenated with
    // { "_id": "encryption_exceeds_16mib",
    //   "unencrypted": < the string 'a' repeated (16777216 - 2000) times > }
    auto encryption_exceeds_16mib = document{} << "_id"
                                               << "encryption_exceeds_16mib"
                                               << "unencrypted" << under_16mib
                                               << concatenate(limits_doc.view()) << finalize;

    // Expect this to fail since encryption results in a document exceeding
    // the maxBsonObjectSize limit.
    REQUIRE_THROWS(coll.insert_one(encryption_exceeds_16mib.view()));
}

TEST_CASE("Views are prohibited", "[client_side_encryption]") {
    instance::current();

    if (!mongocxx::test_util::should_run_client_side_encryption_test()) {
        return;
    }

    // Create a MongoClient without encryption enabled (referred to as client).
    mongocxx::client client{
        uri{},
        test_util::add_test_server_api(),
    };

    if (test_util::get_max_wire_version(client) < 8) {
        // Automatic encryption requires wire version 8.
        WARN("Skipping - max wire version is < 8");
        return;
    }

    // Using client, drop and create a view named db.view with an empty pipeline.
    // E.g. using the command { "create": "view", "viewOn": "coll" }.
    auto db = client["db"];
    db["view"].drop();

    auto cmd = make_document(kvp("create", "view"), kvp("viewOn", "coll"));
    db.run_command(cmd.view());

    // Create a MongoClient configured with auto encryption (referred to as client_encrypted)
    // Configure with the local KMS provider as follows:
    // { "local": { "key": <base64 decoding of LOCAL_MASTERKEY> } }
    // Configure with the keyVaultNamespace set to keyvault.datakeys.
    options::client opts;
    _add_client_encrypted_opts(&opts, {}, _make_kms_doc(), _make_tls_opts());
    mongocxx::client client_encrypted{uri{}, test_util::add_test_server_api(opts)};

    // Using client_encrypted, attempt to insert a document into db.view.
    // Expect an exception to be thrown containing the message: "cannot auto encrypt a view".
    REQUIRE_THROWS_MATCHES(client_encrypted["db"]["view"].insert_one(cmd.view()),
                           mongocxx::exception,
                           test_util::mongocxx_exception_matcher{"cannot auto encrypt a view"});
}

void _run_corpus_test(bool use_schema_map) {
    // The corpus test exhaustively enumerates all ways to encrypt all BSON value types.
    // Note, the test data includes BSON binary subtype 4 (or standard UUID),
    // which MUST be decoded and encoded as subtype 4. Run the test as follows.

    // Create a MongoClient without encryption enabled (referred to as client).
    mongocxx::client client{
        uri{},
        test_util::add_test_server_api(),
    };

    auto corpus_schema = _doc_from_file("/corpus/corpus-schema.json");
    auto corpus_key_local = _doc_from_file("/corpus/corpus-key-local.json");
    auto corpus_key_aws = _doc_from_file("/corpus/corpus-key-aws.json");
    auto corpus_key_azure = _doc_from_file("/corpus/corpus-key-azure.json");
    auto corpus_key_gcp = _doc_from_file("/corpus/corpus-key-gcp.json");
    auto corpus_key_kmip = _doc_from_file("/corpus/corpus-key-kmip.json");

    // Using client, drop and create the collection db.coll configured with the included
    // JSON schema corpus/corpus-schema.json.
    _setup_drop_collections(client);

    auto db = client["db"];
    auto cmd = document{} << "create"
                          << "coll"
                          << "validator" << open_document << "$jsonSchema" << corpus_schema.view()
                          << close_document << finalize;

    db.run_command(cmd.view());

    // Insert all of the key documents.
    write_concern wc_majority;
    wc_majority.acknowledge_level(write_concern::level::k_majority);
    options::insert insert_opts;
    insert_opts.write_concern(std::move(wc_majority));

    auto keyvault = client["keyvault"]["datakeys"];
    keyvault.insert_one(std::move(corpus_key_local), insert_opts);
    keyvault.insert_one(std::move(corpus_key_aws), insert_opts);
    keyvault.insert_one(std::move(corpus_key_azure), insert_opts);
    keyvault.insert_one(std::move(corpus_key_gcp), insert_opts);
    keyvault.insert_one(std::move(corpus_key_kmip), insert_opts);

    // Configure kms credentials as follows:
    // {
    //     "aws": { <AWS credentials> },
    //     "azure": { <Azure credentials> },
    //     "gcp": { <GCP credentials> },
    //     "local": { "key": <base64 decoding of LOCAL_MASTERKEY> },
    //     "kmip": { "endpoint": "localhost:5698" }
    // }

    char local_key_id_storage[16];
    char aws_key_id_storage[16];
    char azure_key_id_storage[16];
    char gcp_key_id_storage[16];
    char kmip_key_id_storage[16];
    memcpy(&(local_key_id_storage[0]), kLocalKeyUUID, 16);
    memcpy(&(aws_key_id_storage[0]), kAwsKeyUUID, 16);
    memcpy(&(azure_key_id_storage[0]), kAzureKeyUUID, 16);
    memcpy(&(gcp_key_id_storage[0]), kGcpKeyUUID, 16);
    memcpy(&(kmip_key_id_storage[0]), kKmipKeyUUID, 16);

    bsoncxx::types::b_binary local_key_id{
        bsoncxx::binary_sub_type::k_uuid, 16, (const uint8_t*)&local_key_id_storage};
    bsoncxx::types::b_binary aws_key_id{
        bsoncxx::binary_sub_type::k_uuid, 16, (const uint8_t*)&aws_key_id_storage};
    bsoncxx::types::b_binary azure_key_id{
        bsoncxx::binary_sub_type::k_uuid, 16, (const uint8_t*)&azure_key_id_storage};
    bsoncxx::types::b_binary gcp_key_id{
        bsoncxx::binary_sub_type::k_uuid, 16, (const uint8_t*)&gcp_key_id_storage};
    bsoncxx::types::b_binary kmip_key_id{
        bsoncxx::binary_sub_type::k_uuid, 16, (const uint8_t*)&kmip_key_id_storage};

    auto local_key_value = make_value(local_key_id);
    auto aws_key_value = make_value(aws_key_id);
    auto azure_key_value = make_value(azure_key_id);
    auto gcp_key_value = make_value(gcp_key_id);
    auto kmip_key_value = make_value(kmip_key_id);

    // Create the following and configure both objects with keyVaultNamespace set to
    // keyvault.datakeys:
    // A MongoClient configured with auto encryption (referred to as client_encrypted)
    options::client client_encrypted_opts;
    if (use_schema_map) {
        _add_client_encrypted_opts(
            &client_encrypted_opts, corpus_schema.view(), _make_kms_doc(), _make_tls_opts());
    } else {
        _add_client_encrypted_opts(&client_encrypted_opts, {}, _make_kms_doc(), _make_tls_opts());
    }

    mongocxx::client client_encrypted{
        uri{},
        test_util::add_test_server_api(client_encrypted_opts),
    };

    // A ClientEncryption object (referred to as client_encryption)
    options::client_encryption cse_opts;
    cse_opts.kms_providers(_make_kms_doc());
    cse_opts.tls_opts(_make_tls_opts());
    cse_opts.key_vault_client(&client);
    cse_opts.key_vault_namespace({"keyvault", "datakeys"});
    mongocxx::client_encryption client_encryption{std::move(cse_opts)};

    // Load corpus/corpus.json to a variable named corpus.
    auto corpus = _doc_from_file("/corpus/corpus.json");
    auto corpus_encrypted_expected = _doc_from_file("/corpus/corpus-encrypted.json");

    // Create a new BSON document, named corpus_copied.
    auto corpus_copied_builder = bsoncxx::builder::basic::document{};

    // Iterate over each field of corpus.
    for (bsoncxx::document::element ele : corpus.view()) {
        auto field_name_view = ele.key();
        std::string field_name(field_name_view);

        // If the field name is _id, altname_aws, altname_azure, altname_gcp, altname_local, copy
        // the field to corpus_copied.
        std::vector<std::string> copied_fields = {
            "_id", "altname_aws", "altname_azure", "altname_gcp", "altname_kmip", "altname_local"};
        if (std::find(copied_fields.begin(), copied_fields.end(), field_name) !=
            copied_fields.end()) {
            corpus_copied_builder.append(kvp(field_name, ele.get_value()));
            continue;
        }

        auto subdoc_val = ele.get_document();
        auto subdoc = subdoc_val.view();

        // The corpus contains subdocuments with the following fields:
        // kms is either aws or local
        // type is a BSON type string names coming from here)
        // algo is either rand or det for random or deterministic encryption
        // method is either auto, for automatic encryption or explicit for explicit encryption
        // identifier is either id or altname for the key identifier
        // allowed is a boolean indicating whether the encryption for the given parameters is
        // permitted.
        // value is the value to be tested.
        auto kms = subdoc["kms"].get_string().value;
        auto type = subdoc["type"].get_string().value;
        auto algo = subdoc["algo"].get_string().value;
        auto method = subdoc["method"].get_string().value;
        auto identifier = subdoc["identifier"].get_string().value;
        auto allowed = subdoc["allowed"].get_bool().value;
        auto to_encrypt = subdoc["value"].get_value();

        // If method is auto, copy the field to corpus_copied.
        // If method is explicit, encrypt explicitly.
        if (method == stdx::string_view("auto")) {
            corpus_copied_builder.append(kvp(field_name, ele.get_value()));
        } else if (method == stdx::string_view{"explicit"}) {
            options::encrypt encrypt_opts;

            // Encrypt with the algorithm described by algo
            if (algo == stdx::string_view{"rand"}) {
                encrypt_opts.algorithm(options::encrypt::encryption_algorithm::k_random);
            } else if (algo == stdx::string_view{"det"}) {
                encrypt_opts.algorithm(options::encrypt::encryption_algorithm::k_deterministic);
            } else {
                throw exception{error_code::k_invalid_parameter, "unsupported algorithm"};
            }

            if (identifier == stdx::string_view{"id"}) {
                if (kms == stdx::string_view{"local"}) {
                    // If kms is local set the key_id to the UUID with base64 value
                    // LOCALAAAAAAAAAAAAAAAAA==.
                    encrypt_opts.key_id(local_key_value.view());
                } else if (kms == stdx::string_view{"aws"}) {
                    // If kms is aws set the key_id to the UUID with base64 value
                    // AWSAAAAAAAAAAAAAAAAAAA==.
                    encrypt_opts.key_id(aws_key_value.view());
                } else if (kms == stdx::string_view{"azure"}) {
                    // If kms is azure set the key_id to the UUID with base64 value
                    // AZUREAAAAAAAAAAAAAAAAA==.
                    encrypt_opts.key_id(azure_key_value.view());
                } else if (kms == stdx::string_view("gcp")) {
                    // If kms is gcp set the key_id to the UUID with base64 value
                    // GCPAAAAAAAAAAAAAAAAAAA==.
                    encrypt_opts.key_id(gcp_key_value.view());
                } else if (kms == stdx::string_view("kmip")) {
                    // If kms is kmip set the key_id to the UUID with base64 value
                    // KMIPAAAAAAAAAAAAAAAAAA==.
                    encrypt_opts.key_id(kmip_key_value.view());
                } else {
                    throw exception{error_code::k_invalid_parameter, "unsupported kms identifier"};
                }
            } else if (identifier == stdx::string_view{"altname"}) {
                if (kms == stdx::string_view{"local"}) {
                    // If kms is local set the key_alt_name to "local".
                    encrypt_opts.key_alt_name("local");
                } else if (kms == stdx::string_view{"aws"}) {
                    // If kms is aws set the key_alt_name to "aws".
                    encrypt_opts.key_alt_name("aws");
                } else if (kms == stdx::string_view("azure")) {
                    // If kms is azure set the key_alt_name to "azure".
                    encrypt_opts.key_alt_name("azure");
                } else if (kms == stdx::string_view("gcp")) {
                    // If kms is gcp set the key_alt_name to "gcp".
                    encrypt_opts.key_alt_name("gcp");
                } else if (kms == stdx::string_view("kmip")) {
                    // If kms is kmip set the key_alt_name to "kmip".
                    encrypt_opts.key_alt_name("kmip");
                } else {
                    throw exception{error_code::k_invalid_parameter, "unsupported kms altname"};
                }
            } else {
                throw exception{error_code::k_invalid_parameter, "unsupported identifier"};
            }

            if (allowed) {
                try {
                    // If allowed is true, copy the field and encrypted value to corpus_copied.
                    auto encrypted_val =
                        client_encryption.encrypt(to_encrypt, std::move(encrypt_opts));

                    auto new_field = document{} << "kms" << kms << "type" << type << "algo" << algo
                                                << "method" << method << "identifier" << identifier
                                                << "allowed" << allowed << "value" << encrypted_val
                                                << finalize;

                    corpus_copied_builder.append(kvp(field_name, std::move(new_field)));
                } catch (const std::exception& e) {
                    FAIL("caught an exception for encrypting an allowed field "
                         << field_name << ": " << e.what());
                }
            } else {
                REQUIRE_THROWS(client_encryption.encrypt(to_encrypt, std::move(encrypt_opts)));
                corpus_copied_builder.append(kvp(field_name, subdoc));
            }
        }
    }

    auto corpus_copied = corpus_copied_builder.extract();

    auto encrypted_coll = client_encrypted["db"]["coll"];

    // Using client_encrypted, insert corpus_copied into db.coll.
    try {
        encrypted_coll.insert_one(corpus_copied.view(), insert_opts);
    } catch (const std::exception& e) {
        FAIL("failed to insert the corpus document: " << e.what());
    }

    // Using client_encrypted, find the inserted document from db.coll to a variable
    // named corpus_decrypted. Since it should have been automatically decrypted, assert
    // the document exactly matches corpus.
    auto res = encrypted_coll.find_one({});
    REQUIRE(res);
    auto corpus_decrypted = res.value();
    REQUIRE(corpus_decrypted == corpus);

    // Load corpus/corpus_encrypted.json to a variable named corpus_encrypted_expected.
    // Using client find the inserted document from db.coll to a variable named
    // corpus_encrypted_actual.
    res = client["db"]["coll"].find_one({});
    REQUIRE(res);
    auto corpus_encrypted_actual = res.value();

    // Iterate over each field of corpus_encrypted_expected and check the following:
    for (bsoncxx::document::element ele : corpus_encrypted_expected.view()) {
        auto field_name_view = ele.key();
        std::string field_name(field_name_view);

        std::vector<std::string> copied_fields = {"_id", "altname_aws", "altname_local"};
        if (std::find(copied_fields.begin(), copied_fields.end(), field_name) !=
            copied_fields.end()) {
            continue;
        }

        auto subdoc_val = ele.get_document();
        auto subdoc = subdoc_val.view();

        auto algo = subdoc["algo"].get_string().value;
        auto allowed = subdoc["allowed"].get_bool().value;
        auto value = subdoc["value"].get_value();

        auto actual_field = corpus_encrypted_actual.view()[field_name];

        // If the algo is det, that the value equals the value of the corresponding field
        // in corpus_encrypted_actual.
        if (algo == stdx::string_view{"det"}) {
            REQUIRE(value == actual_field["value"].get_value());
        }

        // If the algo is rand and allowed is true, that the value does not equal the value
        // of the corresponding field in corpus_encrypted_actual.
        if (algo == stdx::string_view{"rand"} && allowed) {
            REQUIRE(value != actual_field["value"].get_value());
        }

        // If allowed is true, decrypt the value with client_encryption. Decrypt the value of
        // the corresponding field of corpus_encrypted and validate that they are both equal.
        if (allowed) {
            auto decrypted_actual = client_encryption.decrypt(actual_field["value"].get_value());
            auto decrypted_expected = client_encryption.decrypt(value);
            REQUIRE(decrypted_expected == decrypted_actual);
        } else {
            // If allowed is false, validate the value exactly equals the value of the corresponding
            // field of corpus (neither was encrypted).
            REQUIRE(value == corpus.view()[field_name]["value"].get_value());
        }
    }
}

TEST_CASE("Corpus", "[client_side_encryption]") {
    instance::current();

    // Data keys created with AWS KMS may specify a custom endpoint to contact
    // (instead of the default endpoint derived from the AWS region).

    if (!mongocxx::test_util::should_run_client_side_encryption_test()) {
        return;
    }

    mongocxx::client setup_client{
        uri{},
        test_util::add_test_server_api(),
    };

    if (test_util::get_max_wire_version(setup_client) < 8) {
        // Automatic encryption requires wire version 8.
        WARN("Skipping - max wire version is < 8");
        return;
    }
    _run_corpus_test(true);
    _run_corpus_test(false);
}

void _round_trip(mongocxx::client_encryption* client_encryption,
                 bsoncxx::types::bson_value::view datakey) {
    auto to_encrypt = make_value("test");

    options::encrypt encrypt_opts;
    encrypt_opts.algorithm(options::encrypt::encryption_algorithm::k_deterministic);
    encrypt_opts.key_id(datakey);
    auto encrypted_val = client_encryption->encrypt(to_encrypt, encrypt_opts);
    auto decrypted_val = client_encryption->decrypt(encrypted_val);

    REQUIRE(decrypted_val == to_encrypt);
}

void _run_endpoint_test(mongocxx::client* setup_client,
                        bsoncxx::document::view masterkey,
                        std::string kms_provider,
                        stdx::optional<std::string> error_msg = stdx::nullopt,
                        stdx::optional<std::string> invalid_error_msg = stdx::nullopt) {
    INFO("masterkey" << bsoncxx::to_json(masterkey));

    mongocxx::options::client_encryption ce_opts;
    mongocxx::options::client_encryption ce_opts_invalid;
    bsoncxx::builder::basic::document kms_doc;
    bsoncxx::builder::basic::document kms_doc_invalid;
    bsoncxx::builder::basic::document tls_opts;

    kms_doc.append(kvp("aws", [&](sub_document subdoc) {
        subdoc.append(kvp("secretAccessKey",
                          test_util::getenv_or_fail("MONGOCXX_TEST_AWS_SECRET_ACCESS_KEY")));
        subdoc.append(
            kvp("accessKeyId", test_util::getenv_or_fail("MONGOCXX_TEST_AWS_ACCESS_KEY_ID")));
    }));

    kms_doc.append(kvp("azure", [&](sub_document subdoc) {
        subdoc.append(kvp("tenantId", test_util::getenv_or_fail("MONGOCXX_TEST_AZURE_TENANT_ID")));
        subdoc.append(kvp("clientId", test_util::getenv_or_fail("MONGOCXX_TEST_AZURE_CLIENT_ID")));
        subdoc.append(
            kvp("clientSecret", test_util::getenv_or_fail("MONGOCXX_TEST_AZURE_CLIENT_SECRET")));
        subdoc.append(kvp("identityPlatformEndpoint", "login.microsoftonline.com:443"));
    }));

    kms_doc.append(kvp("gcp", [&](sub_document subdoc) {
        subdoc.append(kvp("email", test_util::getenv_or_fail("MONGOCXX_TEST_GCP_EMAIL")));
        subdoc.append(kvp("privateKey", test_util::getenv_or_fail("MONGOCXX_TEST_GCP_PRIVATEKEY")));
        subdoc.append(kvp("endpoint", "oauth2.googleapis.com:443"));
    }));

    kms_doc.append(kvp(
        "kmip", [&](sub_document subdoc) { subdoc.append(kvp("endpoint", "localhost:5698")); }));

    tls_opts.append(kvp("kmip", [&](sub_document subdoc) {
        subdoc.append(
            kvp("tlsCAFile", test_util::getenv_or_fail("MONGOCXX_TEST_CSFLE_TLS_CA_FILE")));
        subdoc.append(
            kvp("tlsCertificateKeyFile",
                test_util::getenv_or_fail("MONGOCXX_TEST_CSFLE_TLS_CERTIFICATE_KEY_FILE")));
    }));

    ce_opts.key_vault_client(setup_client);
    ce_opts.key_vault_namespace({"keyvault", "datakeys"});
    ce_opts.kms_providers(kms_doc.view());
    ce_opts.tls_opts(tls_opts.view());
    mongocxx::client_encryption client_encryption{ce_opts};

    kms_doc_invalid.append(kvp("azure", [&](sub_document subdoc) {
        subdoc.append(kvp("tenantId", test_util::getenv_or_fail("MONGOCXX_TEST_AZURE_TENANT_ID")));
        subdoc.append(kvp("clientId", test_util::getenv_or_fail("MONGOCXX_TEST_AZURE_CLIENT_ID")));
        subdoc.append(
            kvp("clientSecret", test_util::getenv_or_fail("MONGOCXX_TEST_AZURE_CLIENT_SECRET")));
        subdoc.append(kvp("identityPlatformEndpoint", "doesnotexist.invalid:443"));
    }));

    kms_doc_invalid.append(kvp("gcp", [&](sub_document subdoc) {
        subdoc.append(kvp("email", test_util::getenv_or_fail("MONGOCXX_TEST_GCP_EMAIL")));
        subdoc.append(kvp("privateKey", test_util::getenv_or_fail("MONGOCXX_TEST_GCP_PRIVATEKEY")));
        subdoc.append(kvp("endpoint", "doesnotexist.invalid:443"));
    }));

    kms_doc_invalid.append(kvp("kmip", [&](sub_document subdoc) {
        subdoc.append(kvp("endpoint", "doesnotexist.local:5698"));
    }));

    ce_opts_invalid.key_vault_client(setup_client);
    ce_opts_invalid.key_vault_namespace({"keyvault", "datakeys"});
    ce_opts_invalid.kms_providers(kms_doc_invalid.view());
    mongocxx::client_encryption client_encryption_invalid{ce_opts_invalid};

    options::data_key opts;
    opts.master_key(masterkey);
    if (error_msg) {
        REQUIRE_THROWS_MATCHES(client_encryption.create_data_key(kms_provider, std::move(opts)),
                               mongocxx::exception,
                               test_util::mongocxx_exception_matcher{*error_msg});
    } else {
        auto datakey = client_encryption.create_data_key(kms_provider, std::move(opts));
        _round_trip(&client_encryption, datakey);
    }

    if (invalid_error_msg) {
        REQUIRE_THROWS_MATCHES(
            client_encryption_invalid.create_data_key(kms_provider, std::move(opts)),
            mongocxx::exception,
            test_util::mongocxx_exception_matcher{*invalid_error_msg});
    }
}

TEST_CASE("Custom endpoint", "[client_side_encryption]") {
    instance::current();

    // Data keys created with AWS KMS may specify a custom endpoint to contact
    // (instead of the default endpoint derived from the AWS region).

    if (!mongocxx::test_util::should_run_client_side_encryption_test()) {
        return;
    }

    mongocxx::client setup_client{
        uri{},
        test_util::add_test_server_api(),
    };

    if (test_util::get_max_wire_version(setup_client) < 8) {
        // Automatic encryption requires wire version 8.
        WARN("Skipping - max wire version is < 8");
        return;
    }

    // Call client_encryption.createDataKey() with "aws" as the provider and the following
    // masterKey:
    // {
    //   region: "us-east-1",
    //   key: "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
    // }
    // Expect this to succeed. Use the returned UUID of the key to explicitly encrypt and
    // decrypt the string "test" to validate it works.
    SECTION("Test Case 1") {
        auto simple_masterkey = make_document(
            kvp("region", "us-east-1"),
            kvp("key",
                "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"));
        _run_endpoint_test(&setup_client, simple_masterkey.view(), "aws");
    }

    // Call client_encryption.createDataKey() with "aws" as the provider and the following
    // masterKey:
    // {
    //   region: "us-east-1",
    //   key: "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
    //   endpoint: "kms.us-east-1.amazonaws.com"
    // }
    // Expect this to succeed. Use the returned UUID of the key to explicitly encrypt and
    // decrypt the string "test" to validate it works.
    SECTION("Test Case 2") {
        auto endpoint_masterkey =
            document{}
            << "region"
            << "us-east-1"
            << "key"
            << "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
            << "endpoint"
            << "kms.us-east-1.amazonaws.com" << finalize;
        _run_endpoint_test(&setup_client, endpoint_masterkey.view(), "aws");
    }

    // Call client_encryption.createDataKey() with "aws" as the provider and the following
    // masterKey:
    // {
    //   region: "us-east-1",
    //   key: "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
    //   endpoint: "kms.us-east-1.amazonaws.com:443"
    // }
    // Expect this to succeed. Use the returned UUID of the key to explicitly encrypt and
    // decrypt the string "test" to validate it works.
    SECTION("Test Case 3") {
        auto endpoint_masterkey2 =
            document{}
            << "region"
            << "us-east-1"
            << "key"
            << "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
            << "endpoint"
            << "kms.us-east-1.amazonaws.com:443" << finalize;
        _run_endpoint_test(&setup_client, endpoint_masterkey2.view(), "aws");
    }

    // Call client_encryption.createDataKey() with "aws" as the provider and the following
    // masterKey:
    // {
    //   region: "us-east-1",
    //   key: "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
    //   endpoint: "kms.us-east-1.amazonaws.com:12345"
    // }
    // Expect this to fail with a socket connection error.
    SECTION("Test Case 4") {
        auto socket_error_masterkey =
            document{}
            << "region"
            << "us-east-1"
            << "key"
            << "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
            << "endpoint"
            << "kms.us-east-1.amazonaws.com:12345" << finalize;
        _run_endpoint_test(&setup_client, socket_error_masterkey.view(), "aws", {{"error"}});
    }

    // Call client_encryption.createDataKey() with "aws" as the provider and the following
    // masterKey:
    // {
    //   region: "us-east-1",
    //   key: "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
    //   endpoint: "kms.us-east-2.amazonaws.com"
    // }
    // Expect this to fail with an exception.
    SECTION("Test Case 5") {
        auto endpoint_error_masterkey =
            document{}
            << "region"
            << "us-east-1"
            << "key"
            << "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
            << "endpoint"
            << "kms.us-east-2.amazonaws.com" << finalize;
        _run_endpoint_test(&setup_client, endpoint_error_masterkey.view(), "aws", {{""}});
    }

    // Call client_encryption.createDataKey() with "aws" as the provider and the following
    // masterKey:
    // {
    //   region: "us-east-1",
    //   key: "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
    //   endpoint: "doesnotexist.invalid"
    // }
    // Expect this to fail with a network exception indicating failure to resolve
    // "doesnotexist.invalid".
    SECTION("Test Case 6") {
        auto parse_error_masterkey =
            document{}
            << "region"
            << "us-east-1"
            << "key"
            << "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
            << "endpoint"
            << "doesnotexist.invalid" << finalize;
        _run_endpoint_test(&setup_client,
                           parse_error_masterkey.view(),
                           "aws",
                           {{"Failed to resolve doesnotexist.invalid: generic server error"}});
    }

    // Call `client_encryption.createDataKey()` with "azure" as the provider and the following
    // masterKey:
    // {
    //     "keyVaultEndpoint": "key-vault-csfle.vault.azure.net",
    //     "keyName": "key-name-csfle"
    // }
    // Expect this to succeed. Use the returned UUID of the key to explicitly encrypt and decrypt
    // the string "test" to validate it works. Call ``client_encryption_invalid.createDataKey()``
    // with the same masterKey.
    // Expect this to fail with a network exception indicating failure to resolve
    // "doesnotexist.invalid".
    SECTION("Test Case 7") {
        auto azure_masterkey = document{} << "keyVaultEndpoint"
                                          << "key-vault-csfle.vault.azure.net"
                                          << "keyName"
                                          << "key-name-csfle" << finalize;
        _run_endpoint_test(&setup_client,
                           azure_masterkey.view(),
                           "azure",
                           stdx::nullopt,
                           {{"Failed to resolve doesnotexist.invalid: generic server error"}});
    }

    // Call `client_encryption.createDataKey()` with "gcp" as the provider and the following
    // masterKey:
    // {
    //   "projectId": "devprod-drivers",
    //   "location": "global",
    //   "keyRing": "key-ring-csfle",
    //   "keyName": "key-name-csfle",
    //   "endpoint": "cloudkms.googleapis.com:443"
    // }
    // Expect this to succeed. Use the returned UUID of the key to explicitly encrypt and decrypt
    // the string "test" to validate it works. Call ``client_encryption_invalid.createDataKey()``
    // with the same masterKey.
    // Expect this to fail with a network exception indicating failure to resolve
    // "doesnotexist.invalid".
    SECTION("Test Case 8") {
        auto gcp_masterkey = document{} << "projectId"
                                        << "devprod-drivers"
                                        << "location"
                                        << "global"
                                        << "keyRing"
                                        << "key-ring-csfle"
                                        << "keyName"
                                        << "key-name-csfle"
                                        << "endpoint"
                                        << "cloudkms.googleapis.com:443" << finalize;
        _run_endpoint_test(&setup_client,
                           gcp_masterkey.view(),
                           "gcp",
                           stdx::nullopt,
                           {{"Failed to resolve doesnotexist.invalid: generic server error"}});
    }

    // Call `client_encryption.createDataKey()` with "gcp" as the provider and the following
    // masterKey:
    // {
    //   "projectId": "devprod-drivers",
    //   "location": "global",
    //   "keyRing": "key-ring-csfle",
    //   "keyName": "key-name-csfle",
    //   "endpoint": "doesnotexist.invalid:443"
    // }
    // Expect this to fail with an exception with a message containing the string: "Invalid KMS
    // response".
    SECTION("Test Case 9") {
        auto gcp_masterkey2 = document{} << "projectId"
                                         << "devprod-drivers"
                                         << "location"
                                         << "global"
                                         << "keyRing"
                                         << "key-ring-csfle"
                                         << "keyName"
                                         << "key-name-csfle"
                                         << "endpoint"
                                         << "doesnotexist.invalid:443" << finalize;
        _run_endpoint_test(&setup_client, gcp_masterkey2.view(), "gcp", {{"Invalid KMS response"}});
    }

    // Call `client_encryption.createDataKey()` with "kmip" as the provider and the following
    // masterKey:
    // {
    //   "keyId": "1"
    // }
    // Expect this to succeed. Use the returned UUID of the key to explicitly encrypt and decrypt
    // the string "test" to validate it works. Call client_encryption_invalid.createDataKey() with
    // the same masterKey. Expect this to fail with a network exception indicating failure to
    // resolve "doesnotexist.local".
    SECTION("Test Case 10") {
        auto kmip_masterkey = document{} << "keyId"
                                         << "1" << finalize;
        _run_endpoint_test(&setup_client,
                           kmip_masterkey.view(),
                           "kmip",
                           stdx::nullopt,
                           {{"Failed to resolve doesnotexist.local: generic server error"}});
    }

    // Call `client_encryption.createDataKey()` with "kmip" as the provider and the following
    // masterKey:
    // {
    //   "keyId": "1",
    //   "endpoint": "localhost:5698"
    // }
    // Expect this to succeed. Use the returned UUID of the key to explicitly encrypt and decrypt
    // the string "test" to validate it works.
    SECTION("Test Case 11") {
        auto kmip_masterkey = document{} << "keyId"
                                         << "1"
                                         << "endpoint"
                                         << "localhost:5698" << finalize;
        _run_endpoint_test(&setup_client, kmip_masterkey.view(), "kmip");
    }

    // Call `client_encryption.createDataKey()` with "kmip" as the provider and the following
    // masterKey:
    // {
    //   "keyId": "1",
    //   "endpoint": "doesnotexist.local:5698"
    // }
    // Expect this to fail with a network exception indicating failure to resolve
    // "doesnotexist.local".
    SECTION("Test Case 12") {
        auto kmip_masterkey = document{} << "keyId"
                                         << "1"
                                         << "endpoint"
                                         << "doesnotexist.local:5698" << finalize;
        _run_endpoint_test(&setup_client,
                           kmip_masterkey.view(),
                           "kmip",
                           {{"Failed to resolve doesnotexist.local: generic server error"}});
    }
}

void bypass_mongocrypt_via_shared_library(const std::string& shared_lib_path,
                                          bsoncxx::document::view_or_value external_schema) {
    // Via loading shared library
    // The following tests that loading crypt_shared bypasses spawning mongocryptd.
    //
    // Note
    //
    // IMPORTANT: This test requires the crypt_shared library be loaded. If the crypt_shared
    // library is not available, skip the test.
    //

    // 1. Create a MongoClient configured with auto encryption (referred to as client_encrypted)
    //
    // Configure the required options. Use the local KMS provider as follows:
    //
    // { "local": { "key": <base64 decoding of LOCAL_MASTERKEY> } }
    // Configure with the keyVaultNamespace set to keyvault.datakeys.
    //
    // Configure client_encrypted to use the schema external/external-schema.json for db.coll by
    // setting a schema map like: { "db.coll": <contents of external-schema.json>}
    //
    // Configure the following extraOptions:
    //
    // {
    //   "mongocryptdURI": "mongodb://localhost:27021/db?serverSelectionTimeoutMS=1000",
    //   "mongocryptdSpawnArgs": [
    //      "--pidfilepath=bypass-spawning-mongocryptd.pid",
    //      "--port=27021"
    //   ],
    //   "cryptSharedLibPath": "<path to shared library>",
    //   "cryptSharedLibRequired": true
    // }
    auto extra = make_document(
        kvp("mongocryptdURI", "mongodb://localhost:27021/db?serverSelectionTimeoutMS=1000"),
        kvp("mongocryptdSpawnArgs",
            make_array("--pidfilepath=bypass-spawning-mongocryptd.pid", "--port=27021")),
        kvp("cryptSharedLibPath", shared_lib_path),
        kvp("cryptSharedLibRequired", true));

    options::auto_encryption auto_encrypt_opts{};
    auto_encrypt_opts.kms_providers(_make_kms_doc());
    auto_encrypt_opts.tls_opts(_make_tls_opts());
    auto_encrypt_opts.key_vault_namespace({"keyvault", "datakeys"});
    auto_encrypt_opts.schema_map({external_schema.view()});
    auto_encrypt_opts.extra_options({extra.view()});

    options::client client_encrypted_opts;
    client_encrypted_opts.auto_encryption_opts(std::move(auto_encrypt_opts));

    mongocxx::client client_encrypted{
        uri{},
        test_util::add_test_server_api(client_encrypted_opts),
    };

    // 2. Use client_encrypted to insert the document {"unencrypted": "test"} into db.coll.
    // Expect this to succeed.
    auto coll = client_encrypted["db"]["coll"];
    coll.insert_one(make_document(kvp("unencrypted", "test")));

    // 3. Validate that mongocryptd was not spawned. Create a MongoClient to localhost:27021 (or
    // whatever was passed via --port) with serverSelectionTimeoutMS=1000. Run a handshake
    // command and ensure it fails with a server selection timeout.
    mongocxx::client ping_client{
        uri{"mongodb://localhost:27021/?serverSelectionTimeoutMS=1000"},
        test_util::add_test_server_api(),
    };
    REQUIRE_THROWS(ping_client["admin"].run_command(make_document(kvp("ping", 1))));

    // Note
    //
    // IMPORTANT: If crypt_shared is visible to the operating system's library search mechanism,
    // the expected server error generated by these mongocryptdBypassSpawn tests will not appear
    // because libmongocrypt will load the crypt_shared library instead of consulting
    // mongocryptd. For the following tests, it is required that libmongocrypt not load
    // crypt_shared. Refer to the client-side-encryption document for more information on
    // "disabling" crypt_shared.
}

TEST_CASE("Bypass spawning mongocryptd", "[client_side_encryption]") {
    instance::current();

    if (!mongocxx::test_util::should_run_client_side_encryption_test()) {
        return;
    }

    mongocxx::client setup_client{
        uri{},
        test_util::add_test_server_api(),
    };

    if (test_util::get_max_wire_version(setup_client) < 8) {
        // Automatic encryption requires wire version 8.
        WARN("Skipping - max wire version is < 8");
        return;
    }

    auto shared_lib_path = getenv("CRYPT_SHARED_LIB_PATH");

    auto external_schema_file = _doc_from_file("/external/external-schema.json");
    auto external_schema = document{} << "db.coll" << external_schema_file << finalize;

    SECTION("Via loading shared library") {
        if (shared_lib_path) {
            bypass_mongocrypt_via_shared_library(shared_lib_path, external_schema.view());
        }
    }

    if (shared_lib_path) {
        return;
    }

    // Via mongocryptdBypassSpawn

    // Create a MongoClient configured with auto encryption (referred to as client_encrypted)
    //
    // Configure the required options. Use the local KMS provider as follows:
    //     { "local": { "key": <base64 decoding of LOCAL_MASTERKEY> } }
    // Configure with the keyVaultNamespace set to keyvault.datakeys.
    //
    // Configure client_encrypted to use the schema external/external-schema.json for db.coll by
    // setting a schema map like: { "db.coll": <contents of external-schema.json>}
    options::client client_encrypted_opts;

    options::auto_encryption auto_encrypt_opts{};
    auto_encrypt_opts.kms_providers(_make_kms_doc());
    auto_encrypt_opts.tls_opts(_make_tls_opts());
    auto_encrypt_opts.key_vault_namespace({"keyvault", "datakeys"});
    auto_encrypt_opts.schema_map({external_schema.view()});

    // Configure the following extraOptions:
    // {
    //   "mongocryptdBypassSpawn": true
    //   "mongocryptdURI": "mongodb://localhost:27021/db?serverSelectionTimeoutMS=1000",
    //   "mongocryptdSpawnArgs": [ "--pidfilepath=bypass-spawning-mongocryptd.pid", "--port=27021"]
    // }
    auto extra = document{} << "mongocryptdBypassSpawn" << true << "mongocryptdURI"
                            << "mongodb://localhost:27021/db?serverSelectionTimeoutMS=1000"
                            << "mongocryptdSpawnArgs" << open_array
                            << "--pidfilepath=bypass-spawning-mongocryptd.pid"
                            << "--port=27021" << close_array << finalize;

    auto_encrypt_opts.extra_options({extra.view()});
    client_encrypted_opts.auto_encryption_opts(std::move(auto_encrypt_opts));

    mongocxx::client client_encrypted{uri{}, test_util::add_test_server_api(client_encrypted_opts)};

    // Use client_encrypted to insert the document {"encrypted": "test"} into db.coll.
    // Expect a server selection error propagated from the internal MongoClient failing to
    // connect to mongocryptd on port 27021.
    auto coll = client_encrypted["db"]["coll"];
    REQUIRE_THROWS(coll.insert_one(make_document(kvp("encrypted", "test"))));

    // Via bypassAutoEncryption

    // Create a MongoClient configured with auto encryption (referred to as client_encrypted)
    //
    // Configure the required options. Use the local KMS provider as follows:
    //
    // { "local": { "key": <base64 decoding of LOCAL_MASTERKEY> } }
    // Configure with the keyVaultNamespace set to keyvault.datakeys.
    options::client client_encrypted_opts2;
    options::auto_encryption auto_encrypt_opts2{};
    auto_encrypt_opts2.kms_providers(_make_kms_doc());
    auto_encrypt_opts2.tls_opts(_make_tls_opts());
    auto_encrypt_opts2.key_vault_namespace({"keyvault", "datakeys"});
    auto_encrypt_opts2.schema_map({external_schema.view()});

    // Configure with bypassAutoEncryption=true.
    auto_encrypt_opts2.bypass_auto_encryption(true);

    // Configure the following extraOptions:
    // {
    //   "mongocryptdSpawnArgs": [ "--pidfilepath=bypass-spawning-mongocryptd.pid", "--port=27021"]
    // }
    auto extra2 = document{} << "mongocryptdSpawnArgs" << open_array
                             << "--pidfilepath=bypass-spawning-mongocryptd.pid"
                             << "--port=27021" << close_array << finalize;

    auto_encrypt_opts2.extra_options({extra2.view()});
    client_encrypted_opts2.auto_encryption_opts(std::move(auto_encrypt_opts2));

    mongocxx::client client_encrypted2{uri{},
                                       test_util::add_test_server_api(client_encrypted_opts2)};

    // Use client_encrypted to insert the document {"unencrypted": "test"} into db.coll.
    // Expect this to succeed.
    auto coll2 = client_encrypted2["db"]["coll"];
    coll2.insert_one(make_document(kvp("unencrypted", "test")));

    // Validate that mongocryptd was not spawned. Create a MongoClient to localhost:27021
    // (or whatever was passed via --port) with serverSelectionTimeoutMS=1000. Run a ping
    // command and ensure it fails with a server selection timeout.
    options::client ping_client_opts;

    mongocxx::client ping_client{
        uri{"mongodb://localhost:27021/?serverSelectionTimeoutMS=1000"},
        test_util::add_test_server_api(),
    };
    REQUIRE_THROWS(ping_client["admin"].run_command(make_document(kvp("ping", 1))));
}

class kms_tls_expired_cert_matcher : public Catch::MatcherBase<mongocxx::exception> {
   public:
    bool match(const mongocxx::exception& exc) const override {
        return (Catch::Contains("certificate has expired") ||  // OpenSSL
                Catch::Contains("CSSMERR_TP_CERT_EXPIRED") ||  // Secure Transport
                Catch::Contains("certificate has expired") ||  // Secure Channel
                Catch::Contains("certificate has expired"))    // LibreSSL
            .match(exc.what());
    }

    std::string describe() const override {
        return "message should reference an expired certificate";
    }
};

TEST_CASE("KMS TLS expired certificate", "[client_side_encryption]") {
    instance::current();

    if (!mongocxx::test_util::should_run_client_side_encryption_test()) {
        return;
    }

    // Create a mongoclient without encryption.
    options::client client_opts;

    mongocxx::client setup_client{uri{}, test_util::add_test_server_api(client_opts)};

    // Support for detailed certificate verify failure messages required by this test are only
    // available in libmongoc 1.20.0 and newer (CDRIVER-3927).
    if (!mongoc_check_version(1, 20, 0)) {
        WARN("Skipping - libmongoc version is < 1.20.0 (CDRIVER-3927)");
        return;
    }

    // Required CA certificates may not be registered on system. See BUILD-14068.
    if (std::getenv("MONGOCXX_TEST_SKIP_KMS_TLS_TESTS")) {
        WARN("Skipping - KMS TLS tests disabled (BUILD-14068)");
        return;
    }

    if (test_util::get_max_wire_version(setup_client) < 8) {
        // Automatic encryption requires wire version 8.
        WARN("Skipping - max wire version is < 8");
        return;
    }

    options::client_encryption cse_opts;
    _add_cse_opts(&cse_opts, &setup_client);
    client_encryption client_encryption{std::move(cse_opts)};
    options::data_key data_key_opts;

    auto doc = make_document(
        kvp("region", "us-east-1"),
        kvp("key", "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"),
        kvp("endpoint", "127.0.0.1:9000"));
    data_key_opts.master_key(doc.view());

    REQUIRE_THROWS_MATCHES(client_encryption.create_data_key("aws", data_key_opts),
                           mongocxx::exception,
                           kms_tls_expired_cert_matcher());
}

class kms_tls_wrong_host_cert_matcher : public Catch::MatcherBase<mongocxx::exception> {
   public:
    bool match(const mongocxx::exception& exc) const override {
        return (Catch::Contains("IP address mismatch") ||                 // OpenSSL
                Catch::Contains("Host name mismatch") ||                  // Secure Transport
                Catch::Contains("hostname doesn't match certificate") ||  // Secure Channel
                Catch::Contains("not present in server certificate"))     // LibreSSL
            .match(exc.what());
    }

    std::string describe() const override {
        return "message should reference an incorrect or unexpected host";
    }
};

TEST_CASE("KMS TLS wrong host certificate", "[client_side_encryption]") {
    instance::current();

    if (!mongocxx::test_util::should_run_client_side_encryption_test()) {
        return;
    }

    // Create a mongoclient without encryption.
    options::client client_opts;

    mongocxx::client setup_client{uri{}, test_util::add_test_server_api(client_opts)};

    // Support for detailed certificate verify failure messages required by this test are only
    // available in libmongoc 1.20.0 and newer (CDRIVER-3927).
    if (!mongoc_check_version(1, 20, 0)) {
        WARN("Skipping - libmongoc version is < 1.20.0 (CDRIVER-3927)");
        return;
    }

    // Required CA certificates may not be registered on system. See BUILD-14068.
    if (std::getenv("MONGOCXX_TEST_SKIP_KMS_TLS_TESTS")) {
        WARN("Skipping - KMS TLS tests disabled (BUILD-14068)");
        return;
    }

    if (test_util::get_max_wire_version(setup_client) < 8) {
        // Automatic encryption requires wire version 8.
        WARN("Skipping - max wire version is < 8");
        return;
    }

    options::client_encryption cse_opts;
    _add_cse_opts(&cse_opts, &setup_client);
    client_encryption client_encryption{std::move(cse_opts)};
    options::data_key data_key_opts;

    auto doc = make_document(
        kvp("region", "us-east-1"),
        kvp("key", "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"),
        kvp("endpoint", "127.0.0.1:9001"));
    data_key_opts.master_key(doc.view());

    REQUIRE_THROWS_MATCHES(client_encryption.create_data_key("aws", data_key_opts),
                           mongocxx::exception,
                           kms_tls_wrong_host_cert_matcher());
}

bsoncxx::document::value make_kms_providers_with_custom_endpoints(stdx::string_view azure,
                                                                  stdx::string_view gcp,
                                                                  stdx::string_view kmip) {
    bsoncxx::builder::basic::document kms_doc;

    kms_doc.append(kvp("aws", [&](sub_document subdoc) {
        subdoc.append(kvp("secretAccessKey",
                          test_util::getenv_or_fail("MONGOCXX_TEST_AWS_SECRET_ACCESS_KEY")));
        subdoc.append(
            kvp("accessKeyId", test_util::getenv_or_fail("MONGOCXX_TEST_AWS_ACCESS_KEY_ID")));
    }));

    kms_doc.append(kvp("azure", [&](sub_document subdoc) {
        subdoc.append(kvp("tenantId", test_util::getenv_or_fail("MONGOCXX_TEST_AZURE_TENANT_ID")));
        subdoc.append(kvp("clientId", test_util::getenv_or_fail("MONGOCXX_TEST_AZURE_CLIENT_ID")));
        subdoc.append(
            kvp("clientSecret", test_util::getenv_or_fail("MONGOCXX_TEST_AZURE_CLIENT_SECRET")));
        subdoc.append(kvp("identityPlatformEndpoint", azure));
    }));

    kms_doc.append(kvp("gcp", [&](sub_document subdoc) {
        subdoc.append(kvp("email", test_util::getenv_or_fail("MONGOCXX_TEST_GCP_EMAIL")));
        subdoc.append(kvp("privateKey", test_util::getenv_or_fail("MONGOCXX_TEST_GCP_PRIVATEKEY")));
        subdoc.append(kvp("endpoint", gcp));
    }));

    kms_doc.append(kvp("kmip", [&](sub_document subdoc) { subdoc.append(kvp("endpoint", kmip)); }));

    return kms_doc.extract();
}

enum struct with_certs { none, ca_only, cert_only, both };

bsoncxx::document::value make_tls_opts_with_certs(with_certs with) {
    bsoncxx::builder::basic::document tls_opts;

    stdx::string_view providers[] = {"aws", "azure", "gcp", "kmip"};

    for (const auto& provider : providers) {
        tls_opts.append(kvp(provider, [&](sub_document subdoc) {
            if (with == with_certs::ca_only || with == with_certs::both) {
                subdoc.append(
                    kvp("tlsCAFile", test_util::getenv_or_fail("MONGOCXX_TEST_CSFLE_TLS_CA_FILE")));
            }

            if (with == with_certs::cert_only || with == with_certs::both) {
                subdoc.append(
                    kvp("tlsCertificateKeyFile",
                        test_util::getenv_or_fail("MONGOCXX_TEST_CSFLE_TLS_CERTIFICATE_KEY_FILE")));
            }
        }));
    }

    return tls_opts.extract();
}

client_encryption make_prose_test_11_ce(mongocxx::client* client,
                                        stdx::string_view azure,
                                        stdx::string_view gcp,
                                        stdx::string_view kmip,
                                        with_certs with) {
    options::client_encryption cse_opts;
    cse_opts.key_vault_client(client);
    cse_opts.key_vault_namespace({"keyvault", "datakeys"});
    cse_opts.kms_providers(make_kms_providers_with_custom_endpoints(azure, gcp, kmip));
    cse_opts.tls_opts(make_tls_opts_with_certs(with));
    return client_encryption(std::move(cse_opts));
}

// CDRIVER-4181: may fail due to unexpected invalid hostname errors if C Driver was built with VS
// 2015 and uses Secure Channel (ENABLE_SSL=WINDOWS).
TEST_CASE("KMS TLS Options Tests", "[client_side_encryption][!mayfail]") {
    instance::current();

    if (!mongocxx::test_util::should_run_client_side_encryption_test()) {
        return;
    }

    auto setup_client = client(uri(), test_util::add_test_server_api());

    // Support for detailed certificate verify failure messages required by this test are only
    // available in libmongoc 1.20.0 and newer (CDRIVER-3927).
    if (!mongoc_check_version(1, 20, 0)) {
        WARN("Skipping - libmongoc version is < 1.20.0 (CDRIVER-3927)");
        return;
    }

    // Required CA certificates may not be registered on system. See BUILD-14068.
    if (std::getenv("MONGOCXX_TEST_SKIP_KMS_TLS_TESTS")) {
        WARN("Skipping - KMS TLS tests disabled (BUILD-14068)");
        return;
    }

    if (test_util::get_max_wire_version(setup_client) < 8) {
        // Automatic encryption requires wire version 8.
        WARN("Skipping - max wire version is < 8");
        return;
    }

    auto client_encryption_no_client_cert = make_prose_test_11_ce(
        &setup_client, "127.0.0.1:9002", "127.0.0.1:9002", "127.0.0.1:5698", with_certs::ca_only);
    auto client_encryption_with_tls = make_prose_test_11_ce(
        &setup_client, "127.0.0.1:9002", "127.0.0.1:9002", "127.0.0.1:5698", with_certs::both);
    auto client_encryption_expired = make_prose_test_11_ce(
        &setup_client, "127.0.0.1:9000", "127.0.0.1:9000", "127.0.0.1:9000", with_certs::ca_only);
    auto client_encryption_invalid_hostname = make_prose_test_11_ce(
        &setup_client, "127.0.0.1:9001", "127.0.0.1:9001", "127.0.0.1:9001", with_certs::ca_only);

    const auto expired_cert_matcher = Catch::Contains("expired", Catch::CaseSensitive::No);
    const auto invalid_hostname_matcher = Catch::Matches(
        // Content of error message may vary depending on the SSL library being used.
        ".*(mismatch|doesn't match|not present).*",
        Catch::CaseSensitive::No);

    SECTION("Case 1 - AWS") {
        // Expect an error indicating TLS handshake failed.
        // Note: The remote server may disconnect during the TLS handshake, causing miscellaneous
        // errors instead of a neat handshake failure. Just assert that *an* error occurred.
        CHECK_THROWS_AS(
            client_encryption_no_client_cert.create_data_key(
                "aws",
                options::data_key().master_key(
                    document()
                    << "region"
                    << "us-east-1"
                    << "key"
                    << "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
                    << "endpoint"
                    << "127.0.0.1:9002" << finalize)),
            mongocxx::exception);

        // Expect an error from libmongocrypt with a message containing the string: "parse error".
        // This implies TLS handshake succeeded.
        CHECK_THROWS_WITH(
            client_encryption_with_tls.create_data_key(
                "aws",
                options::data_key().master_key(
                    document()
                    << "region"
                    << "us-east-1"
                    << "key"
                    << "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
                    << "endpoint"
                    << "127.0.0.1:9002" << finalize)),
            Catch::Contains("parse error", Catch::CaseSensitive::No));

        // Expect an error indicating TLS handshake failed due to an expired certificate.
        CHECK_THROWS_WITH(
            client_encryption_with_tls.create_data_key(
                "aws",
                options::data_key().master_key(
                    document()
                    << "region"
                    << "us-east-1"
                    << "key"
                    << "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
                    << "endpoint"
                    << "127.0.0.1:9000" << finalize)),
            expired_cert_matcher);

        // Expect an error indicating TLS handshake failed due to an invalid hostname.
        CHECK_THROWS_WITH(
            client_encryption_with_tls.create_data_key(
                "aws",
                options::data_key().master_key(
                    document()
                    << "region"
                    << "us-east-1"
                    << "key"
                    << "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"
                    << "endpoint"
                    << "127.0.0.1:9001" << finalize)),
            invalid_hostname_matcher);
    }

    SECTION("Case 2 - Azure") {
        options::data_key opts;

        opts.master_key(document() << "keyVaultEndpoint"
                                   << "doesnotexist.local"
                                   << "keyName"
                                   << "foo" << finalize);

        // Expect an error indicating TLS handshake failed.
        // Note: The remote server may disconnect during the TLS handshake, causing miscellaneous
        // errors instead of a neat handshake failure. Just assert that *an* error occurred.
        CHECK_THROWS_AS(client_encryption_no_client_cert.create_data_key("azure", opts),
                        mongocxx::exception);

        // Expect an error from libmongocrypt with a message containing the string: "HTTP
        // status=404". This implies TLS handshake succeeded.
        CHECK_THROWS_WITH(client_encryption_with_tls.create_data_key("azure", opts),
                          Catch::Contains("HTTP status=404", Catch::CaseSensitive::No));

        // Expect an error indicating TLS handshake failed due to an expired certificate.
        CHECK_THROWS_WITH(client_encryption_expired.create_data_key("azure", opts),
                          expired_cert_matcher);

        // Expect an error indicating TLS handshake failed due to an invalid hostname.
        CHECK_THROWS_WITH(client_encryption_invalid_hostname.create_data_key("azure", opts),
                          invalid_hostname_matcher);
    }

    SECTION("Case 3 - GCP") {
        options::data_key opts;

        opts.master_key(document() << "projectId"
                                   << "foo"
                                   << "location"
                                   << "bar"
                                   << "keyRing"
                                   << "baz"
                                   << "keyName"
                                   << "foo" << finalize);

        // Expect an error indicating TLS handshake failed.
        // Note: The remote server may disconnect during the TLS handshake, causing miscellaneous
        // errors instead of a neat handshake failure. Just assert that *an* error occurred.
        CHECK_THROWS_AS(client_encryption_no_client_cert.create_data_key("gcp", opts),
                        mongocxx::exception);

        // Expect an error from libmongocrypt with a message containing the string: "HTTP
        // status=404". This implies TLS handshake succeeded.
        CHECK_THROWS_WITH(client_encryption_with_tls.create_data_key("gcp", opts),
                          Catch::Contains("HTTP status=404", Catch::CaseSensitive::No));

        // Expect an error indicating TLS handshake failed due to an expired certificate.
        CHECK_THROWS_WITH(client_encryption_expired.create_data_key("gcp", opts),
                          expired_cert_matcher);

        // Expect an error indicating TLS handshake failed due to an invalid hostname.
        CHECK_THROWS_WITH(client_encryption_invalid_hostname.create_data_key("gcp", opts),
                          invalid_hostname_matcher);
    }

    SECTION("Case 4 - KMIP") {
        options::data_key opts;

        opts.master_key({});

        // Expect an error indicating TLS handshake failed.
        // Note: The remote server may disconnect during the TLS handshake, causing miscellaneous
        // errors instead of a neat handshake failure. Just assert that *an* error occurred.
        CHECK_THROWS_AS(client_encryption_no_client_cert.create_data_key("kmip", opts),
                        mongocxx::exception);

        // Expect success.
        CHECK_NOTHROW(client_encryption_with_tls.create_data_key("kmip", opts));

        // Expect an error indicating TLS handshake failed due to an expired certificate.
        CHECK_THROWS_WITH(client_encryption_expired.create_data_key("kmip", opts),
                          expired_cert_matcher);

        // Expect an error indicating TLS handshake failed due to an invalid hostname.
        CHECK_THROWS_WITH(client_encryption_invalid_hostname.create_data_key("kmip", opts),
                          invalid_hostname_matcher);
    }
}

// https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.rst#test-setup
std::tuple<mongocxx::client_encryption, mongocxx::client> _setup_explicit_encryption(
    bsoncxx::document::view key1_document, mongocxx::client* key_vault_client) {
    mongocxx::client client{
        uri{},
        test_util::add_test_server_api(),
    };

    // Load the file encryptedFields.json as encryptedFields.
    auto encrypted_fields = _doc_from_file("/explicit-encryption/encryptedFields.json");

    // Drop and create the collection db.explicit_encryption using
    // encryptedFields as an option. See FLE 2 CreateCollection() and
    // Collection.Drop().
    {
        write_concern wc_majority;
        wc_majority.acknowledge_level(write_concern::level::k_majority);

        auto coll = client["db"]["explicit_encryption"];
        auto drop_doc = make_document(kvp("encryptedFields", encrypted_fields));
        coll.drop(wc_majority, drop_doc.view());

        client["db"].create_collection("explicit_encryption", drop_doc.view(), wc_majority);
    }

    // Drop and create the collection keyvault.datakeys.
    {
        write_concern wc_majority;
        wc_majority.acknowledge_level(write_concern::level::k_majority);

        auto coll = client["keyvault"]["datakeys"];
        coll.drop(wc_majority);

        const auto empty_doc = make_document();
        client["keyvault"].create_collection("datakeys", empty_doc.view(), wc_majority);
    }

    // Insert key1Document in keyvault.datakeys with majority write concern.
    {
        write_concern wc_majority;
        wc_majority.acknowledge_level(write_concern::level::k_majority);

        options::insert insert_opts;
        insert_opts.write_concern(std::move(wc_majority));

        client["keyvault"]["datakeys"].insert_one(key1_document, insert_opts);
    }

    // Create a MongoClient named keyVaultClient.
    // Create a ClientEncryption object named clientEncryption with these
    // options:
    //
    // ClientEncryptionOpts {
    //    keyVaultClient: <keyVaultClient>;
    //    keyVaultNamespace: "keyvault.datakeys";
    //    kmsProviders: { "local": { "key": <base64 decoding of LOCAL_MASTERKEY> } }
    // }
    options::client_encryption ce_opts;
    ce_opts.key_vault_client(key_vault_client);
    ce_opts.key_vault_namespace({"keyvault", "datakeys"});
    ce_opts.kms_providers(_make_kms_doc(false));
    client_encryption client_encryption(std::move(ce_opts));

    // Create a MongoClient named encryptedClient with these AutoEncryptionOpts:
    //
    // AutoEncryptionOpts {
    //    keyVaultNamespace: "keyvault.datakeys";
    //    kmsProviders: { "local": { "key": <base64 decoding of LOCAL_MASTERKEY> } }
    //    bypassQueryAnalysis: true
    // }
    options::auto_encryption auto_encrypt_opts{};
    auto_encrypt_opts.key_vault_namespace({"keyvault", "datakeys"});
    auto_encrypt_opts.kms_providers(_make_kms_doc(false));
    auto_encrypt_opts.bypass_query_analysis(true);
    options::client encrypted_client_opts;
    encrypted_client_opts.auto_encryption_opts(std::move(auto_encrypt_opts));
    mongocxx::client encrypted_client{
        uri{},
        test_util::add_test_server_api(encrypted_client_opts),
    };

    return std::make_tuple(std::move(client_encryption), std::move(encrypted_client));
}

// https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.rst
TEST_CASE("Explicit Encryption", "[client_side_encryption]") {
    instance::current();

    if (!mongocxx::test_util::should_run_client_side_encryption_test()) {
        return;
    }

    mongocxx::client conn{
        mongocxx::uri{},
        test_util::add_test_server_api(),
    };

    if (!test_util::newer_than(conn, "7.0")) {
        WARN("Skipping - MongoDB server 7.0 or newer required");
        return;
    }

    if (test_util::get_topology(conn) == "single") {
        WARN("Skipping - must not run against a standalone server");
        return;
    }

    // Load the file key1-document.json as key1Document.
    auto key1_document = _doc_from_file("/explicit-encryption/key1-document.json");

    // Read the "_id" field of key1Document as key1ID.
    auto key1_id = key1_document["_id"].get_value();

    std::string plain_text_indexed = "encrypted indexed value";
    bsoncxx::types::bson_value::value plain_text_indexed_value(plain_text_indexed);

    std::string plain_text_unindexed = "encrypted unindexed value";
    bsoncxx::types::bson_value::value plain_text_unindexed_value(plain_text_unindexed);

    mongocxx::client key_vault_client{
        uri{},
        test_util::add_test_server_api(),
    };
    auto tpl = _setup_explicit_encryption(key1_document, &key_vault_client);

    auto client_encryption = std::move(std::get<0>(tpl));
    auto encrypted_client = std::move(std::get<1>(tpl));

    SECTION("Case 1: can insert encrypted indexed and find") {
        // Use clientEncryption to encrypt the value "encrypted indexed value" with these
        // EncryptOpts:
        //
        // class EncryptOpts {
        //    keyId : <key1ID>
        //    algorithm: "Indexed",
        //    contentionFactor: 0
        // }
        //
        // Store the result in insertPayload.
        {
            options::encrypt encrypt_opts;
            encrypt_opts.key_id(key1_id);
            encrypt_opts.algorithm(options::encrypt::encryption_algorithm::k_indexed);
            encrypt_opts.contention_factor(0);
            auto insert_payload = client_encryption.encrypt(plain_text_indexed_value, encrypt_opts);

            // Use encryptedClient to insert the document { "encryptedIndexed": <insertPayload> }
            // into db.explicit_encryption.
            auto doc = make_document(kvp("encryptedIndexed", insert_payload));
            encrypted_client["db"]["explicit_encryption"].insert_one(doc.view());
        }

        // Use clientEncryption to encrypt the value "encrypted indexed value" with these
        // EncryptOpts:
        //
        // class EncryptOpts {
        //    keyId : <key1ID>
        //    algorithm: "Indexed",
        //    queryType: "equality",
        //    contentionFactor: 0
        // }
        // Store the result in findPayload.
        {
            options::encrypt encrypt_opts;
            encrypt_opts.key_id(key1_id);
            encrypt_opts.algorithm(options::encrypt::encryption_algorithm::k_indexed);
            encrypt_opts.query_type(options::encrypt::encryption_query_type::k_equality);
            encrypt_opts.contention_factor(0);
            auto find_payload = client_encryption.encrypt(plain_text_indexed_value, encrypt_opts);

            // Use encryptedClient to run a "find" operation on the
            // db.explicit_encryption collection with the filter
            // { "encryptedIndexed": <findPayload> }.
            auto find_filter = make_document(kvp("encryptedIndexed", find_payload));
            auto found = encrypted_client["db"]["explicit_encryption"].find(find_filter.view());
            size_t count = 0;
            for (const auto& it : found) {
                count++;
                auto doc = it.find("encryptedIndexed")->get_string().value;

                // Assert one document is returned containing the field { "encryptedIndexed":
                // "encrypted indexed value" }.
                REQUIRE(doc == plain_text_indexed_value);
            }

            // Assert one document is returned containing the field { "encryptedIndexed": "encrypted
            // indexed value" }.
            REQUIRE(count == 1);
        }
    }

    SECTION("Case 2: can insert encrypted indexed and find with non-zero contention") {
        // Use clientEncryption to encrypt the value "encrypted indexed value" with these
        // EncryptOpts:
        //
        // class EncryptOpts {
        //    keyId : <key1ID>
        //    algorithm: "Indexed",
        //    contentionFactor: 10
        // }
        //
        // Store the result in insertPayload.
        for (size_t i = 0; i < 10; i++) {
            options::encrypt encrypt_opts;
            encrypt_opts.key_id(key1_id);
            encrypt_opts.algorithm(options::encrypt::encryption_algorithm::k_indexed);
            encrypt_opts.contention_factor(10);
            auto insert_payload = client_encryption.encrypt(plain_text_indexed_value, encrypt_opts);

            // Use encryptedClient to insert the document { "encryptedIndexed": <insertPayload> }
            // into db.explicit_encryption.
            auto doc = make_document(kvp("encryptedIndexed", insert_payload));
            encrypted_client["db"]["explicit_encryption"].insert_one(doc.view());

            // Repeat the above steps 10 times to insert 10 total documents.
            // The insertPayload must be regenerated each iteration.
        }

        // Use clientEncryption to encrypt the value "encrypted indexed value" with these
        // EncryptOpts:
        //
        // class EncryptOpts {
        //    keyId : <key1ID>
        //    algorithm: "Indexed",
        //    queryType: "equality",
        //    contentionFactor: 0
        // }
        //
        // Store the result in findPayload.
        {
            options::encrypt encrypt_opts;
            encrypt_opts.key_id(key1_id);
            encrypt_opts.algorithm(options::encrypt::encryption_algorithm::k_indexed);
            encrypt_opts.query_type(options::encrypt::encryption_query_type::k_equality);
            encrypt_opts.contention_factor(0);
            auto find_payload = client_encryption.encrypt(plain_text_indexed_value, encrypt_opts);

            // Use encryptedClient to run a "find" operation on the db.explicit_encryption
            // collection with the filter { "encryptedIndexed": <findPayload> }.
            auto find_filter = make_document(kvp("encryptedIndexed", find_payload));
            auto found = encrypted_client["db"]["explicit_encryption"].find(find_filter.view());
            size_t count = 0;
            for (const auto& it : found) {
                count++;
                auto doc = it.find("encryptedIndexed")->get_string().value;

                // Assert less than 10 documents are returned. 0 documents may be returned. Assert
                // each returned document contains the field { "encryptedIndexed": "encrypted
                // indexed value" }.
                REQUIRE(doc == plain_text_indexed_value);
            }

            // Assert less than 10 documents are returned. 0 documents may be returned. Assert each
            // returned document contains the field { "encryptedIndexed": "encrypted indexed value"
            // }.
            REQUIRE(count < 10);
        }

        // Use clientEncryption to encrypt the value "encrypted indexed value" with these
        // EncryptOpts:
        //
        // class EncryptOpts {
        //    keyId : <key1ID>
        //    algorithm: "Indexed",
        //    queryType: "equality",
        //    contentionFactor: 10
        // }
        //
        // Store the result in findPayload2.
        {
            options::encrypt encrypt_opts;
            encrypt_opts.key_id(key1_id);
            encrypt_opts.algorithm(options::encrypt::encryption_algorithm::k_indexed);
            encrypt_opts.query_type(options::encrypt::encryption_query_type::k_equality);
            encrypt_opts.contention_factor(10);
            auto find_payload2 = client_encryption.encrypt(plain_text_indexed_value, encrypt_opts);

            // Use encryptedClient to run a "find" operation on the db.explicit_encryption
            // collection with the filter { "encryptedIndexed": <findPayload2> }.
            auto find_filter = make_document(kvp("encryptedIndexed", find_payload2));
            auto found = encrypted_client["db"]["explicit_encryption"].find(find_filter.view());
            size_t count = 0;
            for (const auto& it : found) {
                count++;
                auto doc = it.find("encryptedIndexed")->get_string().value;

                // Assert 10 documents are returned. Assert each returned document contains the
                // field { "encryptedIndexed": "encrypted indexed value" }.
                REQUIRE(doc == plain_text_indexed_value);
            }

            // Assert 10 documents are returned. Assert each returned document contains the field {
            // "encryptedIndexed": "encrypted indexed value" }.
            REQUIRE(count == 10);
        }
    }

    SECTION("Case 3: can insert encrypted unindexed") {
        // Use clientEncryption to encrypt the value "encrypted unindexed value" with these
        // EncryptOpts:
        //
        // class EncryptOpts {
        //   keyId : <key1ID>
        //   algorithm: "Unindexed"
        //}
        //
        // Store the result in insertPayload.
        {
            options::encrypt encrypt_opts;
            encrypt_opts.key_id(key1_id);
            encrypt_opts.algorithm(options::encrypt::encryption_algorithm::k_unindexed);

            auto insert_payload =
                client_encryption.encrypt(plain_text_unindexed_value, encrypt_opts);

            // Use encryptedClient to insert the document { "_id": 1, "encryptedUnindexed":
            // <insertPayload> } into db.explicit_encryption.
            auto doc = make_document(kvp("_id", 1), kvp("encryptedUnindexed", insert_payload));
            encrypted_client["db"]["explicit_encryption"].insert_one(doc.view());

            // Use encryptedClient to run a "find" operation on the db.explicit_encryption
            // collection with the filter { "_id": 1 }.
            auto find_filter = make_document(kvp("_id", 1));
            auto found = encrypted_client["db"]["explicit_encryption"].find(find_filter.view());
            size_t count = 0;
            for (const auto& it : found) {
                count++;
                auto doc = it.find("encryptedUnindexed")->get_string().value;

                // Assert one document is returned containing the field { "encryptedUnindexed":
                // "encrypted unindexed value" }.
                REQUIRE(doc == plain_text_unindexed_value);
            }

            // Assert one document is returned containing the field { "encryptedUnindexed":
            // "encrypted unindexed value" }.
            REQUIRE(count == 1);
        }
    }

    SECTION("Case 4: can roundtrip encrypted indexed") {
        // Use clientEncryption to encrypt the value "encrypted indexed value" with these
        // EncryptOpts:
        //
        // class EncryptOpts {
        //    keyId : <key1ID>
        //    algorithm: "Indexed",
        //    contentionFactor: 0
        // }
        //
        // Store the result in payload.
        {
            options::encrypt encrypt_opts;
            encrypt_opts.key_id(key1_id);
            encrypt_opts.algorithm(options::encrypt::encryption_algorithm::k_indexed);
            encrypt_opts.contention_factor(0);

            auto payload = client_encryption.encrypt(plain_text_indexed_value, encrypt_opts);

            // Use clientEncryption to decrypt payload.
            auto plain_text_value = client_encryption.decrypt(payload);
            auto plain_text = plain_text_value.view().get_string().value;

            // Assert the returned value equals "encrypted indexed value".
            REQUIRE(plain_text == plain_text_indexed_value);
        }
    }

    SECTION("Case 5: can roundtrip encrypted unindexed") {
        // Use clientEncryption to encrypt the value "encrypted unindexed value" with these
        // EncryptOpts:
        //
        // class EncryptOpts {
        //    keyId : <key1ID>
        //    algorithm: "Unindexed",
        // }
        //
        // Store the result in payload.
        {
            options::encrypt encrypt_opts;
            encrypt_opts.key_id(key1_id);
            encrypt_opts.algorithm(options::encrypt::encryption_algorithm::k_unindexed);
            auto payload = client_encryption.encrypt(plain_text_unindexed_value, encrypt_opts);

            // Use clientEncryption to decrypt payload.
            auto plain_text_value = client_encryption.decrypt(payload);
            auto plain_text = plain_text_value.view().get_string().value;

            // Assert the returned value equals "encrypted unindexed value".
            REQUIRE(plain_text == plain_text_unindexed_value);
        }
    }
}

TEST_CASE("Create Encrypted Collection", "[client_side_encryption]") {
    instance::current();
    mongocxx::client conn{mongocxx::uri{}, test_util::add_test_server_api()};

    if (!mongocxx::test_util::should_run_client_side_encryption_test()) {
        return;
    }

    if (!test_util::newer_than(conn, "7.0")) {
        WARN("Explicit Encryption tests require MongoDB server 7.0+.");
        return;
    }

    if (test_util::get_topology(conn) == "single") {
        WARN("Explicit Encryption tests must not run against a standalone.");
        return;
    }

    conn.database("keyvault").collection("datakeys").drop();

    struct which {
        std::string kms_provider;
        stdx::optional<bsoncxx::document::value> master_key;
    };

    which w = GENERATE(Catch::Generators::values<which>({
        {"aws",
         make_document(
             kvp("region", "us-east-1"),
             kvp("key",
                 "arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0"))},
        // When testing 'local', use master_key of 'null'
        {"local", stdx::nullopt},
    }));

    options::client_encryption cse_opts;
    _add_cse_opts(&cse_opts, &conn, true);
    client_encryption cse{std::move(cse_opts)};

    auto db = conn.database("cec-test-db");
    db.drop();
    auto fin_options = make_document();

    DYNAMIC_SECTION("KMS Provider - " << w.kms_provider) {
        SECTION("Case 1: Simple Creation and Validation") {
            const auto create_opts = make_document(
                kvp("encryptedFields",
                    make_document(
                        kvp("fields",
                            make_array(make_document(kvp("path", "ssn"),
                                                     kvp("bsonType", "string"),
                                                     kvp("keyId", bsoncxx::types::b_null{})))))));

            auto coll = cse.create_encrypted_collection(
                db,
                "testing1",
                create_opts,
                fin_options,
                w.kms_provider,
                w.master_key ? stdx::make_optional(w.master_key->view()) : stdx::nullopt);
            CAPTURE(fin_options, coll);
            try {
                coll.insert_one(make_document(kvp("ssn", "123-45-6789")));
                FAIL_CHECK("Insert should have failed");
            } catch (const mongocxx::operation_exception& e) {
                CHECK(e.code().value() == 121);  // VALIDATION_ERROR
            }
        }

        SECTION("Case 2: Missing 'encryptedFields'") {
            const auto create_opts = make_document();
            try {
                auto coll = cse.create_encrypted_collection(
                    db,
                    "testing1",
                    create_opts,
                    fin_options,
                    w.kms_provider,
                    w.master_key ? stdx::make_optional(w.master_key->view()) : stdx::nullopt);
                CAPTURE(fin_options, coll);
                FAIL_CHECK("Did not throw");
            } catch (const mongocxx::operation_exception& e) {
                CHECK(e.code().value() == 22);  // INVALID_ARG
            }
        }

        SECTION("Case 3: Invalid keyId") {
            const auto create_opts = make_document(kvp(
                "encryptedFields",
                make_document(kvp(
                    "fields",
                    make_array(make_document(
                        kvp("path", "ssn"), kvp("bsonType", "string"), kvp("keyId", false)))))));

            try {
                auto coll = cse.create_encrypted_collection(
                    db,
                    "testing1",
                    create_opts,
                    fin_options,
                    w.kms_provider,
                    w.master_key ? stdx::make_optional(w.master_key->view()) : stdx::nullopt);
                CAPTURE(fin_options, coll);
                FAIL_CHECK("Did not throw");
            } catch (const mongocxx::operation_exception& e) {
                CHECK(e.code().value() == 14);  // INVALID_REPLY
            }
        }

        SECTION("Case 4: Insert encrypted value") {
            const auto create_opts = make_document(
                kvp("encryptedFields",
                    make_document(
                        kvp("fields",
                            make_array(make_document(kvp("path", "ssn"),
                                                     kvp("bsonType", "string"),
                                                     kvp("keyId", bsoncxx::types::b_null{})))))));

            auto coll = cse.create_encrypted_collection(
                db,
                "testing1",
                create_opts,
                fin_options,
                w.kms_provider,
                w.master_key ? stdx::make_optional(w.master_key->view()) : stdx::nullopt);
            CAPTURE(fin_options, coll);

            bsoncxx::types::b_string ssn{"123-45-6789"};
            auto key = fin_options["encryptedFields"]["fields"][0]["keyId"];
            options::encrypt enc;
            enc.key_id(key.get_value());
            enc.algorithm(options::encrypt::encryption_algorithm::k_unindexed);
            auto encrypted = cse.encrypt(bsoncxx::types::bson_value::view(ssn), enc);
            CHECK_NOTHROW(coll.insert_one(make_document(kvp("ssn", encrypted))));
        }
    }
}

TEST_CASE("Unique Index on keyAltNames", "[client_side_encryption]") {
    instance::current();

    if (!mongocxx::test_util::should_run_client_side_encryption_test()) {
        return;
    }

    if (!test_util::newer_than(uri{}, "4.2")) {
        WARN("Skipping - requires MongoDB server 4.2+");
        return;
    }

    // 1. Create a MongoClient object (referred to as client).
    mongocxx::client client{mongocxx::uri{}, test_util::add_test_server_api()};

    // 2. Using client, drop the collection keyvault.datakeys.
    client["keyvault"]["datakeys"].drop();

    // 3. Using client, create a unique index on keyAltNames with a partial index filter for only
    // documents where keyAltNames exists using writeConcern "majority". The command should be
    // equivalent to:
    //
    // db.runCommand(
    //   {
    //      createIndexes: "datakeys",
    //      indexes: [
    //        {
    //          name: "keyAltNames_1",
    //          key: { "keyAltNames": 1 },
    //          unique: true,
    //          partialFilterExpression: { keyAltNames: { $exists: true } }
    //        }
    //      ],
    //      writeConcern: { w: "majority" }
    //   }
    // )
    auto db = client["keyvault"];
    db.run_command(make_document(
        kvp("createIndexes", "datakeys"),
        kvp("indexes",
            make_array(make_document(
                kvp("name", "keyAltNames_1"),
                kvp("key", make_document(kvp("keyAltNames", 1))),
                kvp("unique", true),
                kvp("partialFilterExpression",
                    make_document(kvp("keyAltNames", make_document(kvp("$exists", true)))))))),
        kvp("writeConcern", make_document(kvp("w", "majority")))));

    // 4. Create a ClientEncryption object (referred to as client_encryption) with client set as the
    // keyVaultClient.
    options::client_encryption ce_opts;
    ce_opts.key_vault_client(&client);
    ce_opts.key_vault_namespace({"keyvault", "datakeys"});
    ce_opts.kms_providers(_make_kms_doc(false));
    client_encryption client_encryption(std::move(ce_opts));

    // 5. Using client_encryption, create a data key with a local KMS provider and the keyAltName
    // "def".
    mongocxx::options::data_key dk_opts;
    dk_opts.key_alt_names({"def"});
    std::string provider = "local";
    auto existing_key = client_encryption.create_data_key(provider, dk_opts);

    SECTION("Case 1: createKey()") {
        // 1. Use client_encryption to create a new local data key with a keyAltName "abc" and
        // assert the operation does not fail.
        {
            mongocxx::options::data_key dk_opts;
            dk_opts.key_alt_names({"abc"});
            std::string provider = "local";
            client_encryption.create_data_key(provider, dk_opts);
        }

        // 2. Repeat Step 1 and assert the operation fails due to a duplicate key server error
        // (error code 11000).
        {
            mongocxx::options::data_key dk_opts;
            dk_opts.key_alt_names({"abc"});
            std::string provider = "local";
            bool exception_thrown = false;
            try {
                client_encryption.create_data_key(provider, dk_opts);
            } catch (mongocxx::operation_exception& e) {
                REQUIRE(std::strstr(
                    e.what(),
                    "E11000 duplicate key error collection: keyvault.datakeys index: keyAltNames_1 "
                    "dup key: { keyAltNames: \"abc\" }: generic server error"));
                exception_thrown = true;
            }
            REQUIRE(exception_thrown);
        }

        // 3. Use client_encryption to create a new local data key with a keyAltName "def" and
        // assert the operation fails due to a duplicate key server error (error code 11000).
        {
            mongocxx::options::data_key dk_opts;
            dk_opts.key_alt_names({"def"});
            std::string provider = "local";
            bool exception_thrown = false;
            try {
                client_encryption.create_data_key(provider, dk_opts);
            } catch (mongocxx::operation_exception& e) {
                REQUIRE(std::strstr(
                    e.what(),
                    "E11000 duplicate key error collection: keyvault.datakeys index: keyAltNames_1 "
                    "dup key: { keyAltNames: \"def\" }: generic server error"));
                exception_thrown = true;
            }
            REQUIRE(exception_thrown);
        }
    }

    SECTION("Case 2: addKeyAltName()") {
        // 1. Use client_encryption to create a new local data key and assert the operation does not
        // fail.
        auto key_doc = client_encryption.create_data_key("local");

        // 2. Use client_encryption to add a keyAltName "abc" to the key created in Step 1 and
        // assert the operation does not fail.
        client_encryption.add_key_alt_name(key_doc.view(), "abc");

        // 3. Repeat Step 2, assert the operation does not fail, and assert the returned key
        // document contains the keyAltName "abc" added in Step 2.
        {
            auto alt_key = client_encryption.add_key_alt_name(key_doc.view(), "abc");
            REQUIRE(std::string(alt_key.value()["keyAltNames"][0].get_string().value) == "abc");
        }

        // 4. Use client_encryption to add a keyAltName "def" to the key created in Step 1 and
        // assert the operation fails due to a duplicate key server error (error code 11000).
        {
            bool exception_thrown = false;
            try {
                client_encryption.add_key_alt_name(key_doc.view(), "def");
            } catch (mongocxx::operation_exception& e) {
                REQUIRE(std::strstr(
                    e.what(),
                    "E11000 duplicate key error collection: keyvault.datakeys index: keyAltNames_1 "
                    "dup key: { keyAltNames: \"def\" }: generic server error"));
                exception_thrown = true;
            }
            REQUIRE(exception_thrown);
        }

        // 5. Use client_encryption to add a keyAltName "def" to the existing key, assert the
        // operation does not fail, and assert the returned key document contains the keyAltName
        // "def" added during Setup.
        {
            auto alt_key = client_encryption.add_key_alt_name(existing_key.view(), "def");
            REQUIRE(std::string(alt_key.value()["keyAltNames"][0].get_string().value) == "def");
        }
    }
}

TEST_CASE("Custom Key Material Test", "[client_side_encryption]") {
    instance::current();

    if (!mongocxx::test_util::should_run_client_side_encryption_test()) {
        WARN("Skipping - Client Side Encryption is required");
        return;
    }

    if (!test_util::newer_than(uri{}, "4.2")) {
        WARN("Skipping - MongoDB server 4.2 or newer required");
        return;
    }

    // 1. Create a MongoClient object (referred to as client).
    mongocxx::client client{mongocxx::uri{}, test_util::add_test_server_api()};

    // 2. Using client, drop the collection keyvault.datakeys.
    client["keyvault"]["datakeys"].drop();

    // 3. Create a ClientEncryption object (referred to as client_encryption) with client set as the
    // keyVaultClient.
    options::client_encryption ce_opts;
    ce_opts.key_vault_client(&client);
    ce_opts.key_vault_namespace({"keyvault", "datakeys"});
    ce_opts.kms_providers(_make_kms_doc(false));
    client_encryption client_encryption(std::move(ce_opts));

    // 4. Using client_encryption, create a data key with a local KMS provider and the following
    // custom key material (given as base64):
    // xPTAjBRG5JiPm+d3fj6XLi2q5DMXUS/f1f+SMAlhhwkhDRL0kr8r9GDLIGTAGlvC+HVjSIgdL+RKwZCvpXSyxTICWSXTUYsWYPyu3IoHbuBZdmw2faM3WhcRIgbMReU5
    mongocxx::options::data_key dk_opts;
    std::vector<uint8_t> key_material{
        0xc4, 0xf4, 0xc0, 0x8c, 0x14, 0x46, 0xe4, 0x98, 0x8f, 0x9b, 0xe7, 0x77, 0x7e, 0x3e,
        0x97, 0x2e, 0x2d, 0xaa, 0xe4, 0x33, 0x17, 0x51, 0x2f, 0xdf, 0xd5, 0xff, 0x92, 0x30,
        0x9,  0x61, 0x87, 0x9,  0x21, 0xd,  0x12, 0xf4, 0x92, 0xbf, 0x2b, 0xf4, 0x60, 0xcb,
        0x20, 0x64, 0xc0, 0x1a, 0x5b, 0xc2, 0xf8, 0x75, 0x63, 0x48, 0x88, 0x1d, 0x2f, 0xe4,
        0x4a, 0xc1, 0x90, 0xaf, 0xa5, 0x74, 0xb2, 0xc5, 0x32, 0x2,  0x59, 0x25, 0xd3, 0x51,
        0x8b, 0x16, 0x60, 0xfc, 0xae, 0xdc, 0x8a, 0x7,  0x6e, 0xe0, 0x59, 0x76, 0x6c, 0x36,
        0x7d, 0xa3, 0x37, 0x5a, 0x17, 0x11, 0x22, 0x6,  0xcc, 0x45, 0xe5, 0x39};
    dk_opts.key_material(key_material);
    auto key = client_encryption.create_data_key("local", dk_opts);
    auto key_id = key.view().get_binary();

    // 5. Find the resulting key document in keyvault.datakeys, save a copy of the key document,
    // then remove the key document from the collection.
    auto cursor = client["keyvault"]["datakeys"].find(make_document(kvp("_id", key_id)));
    const auto doc = *cursor.begin();
    client["keyvault"]["datakeys"].delete_one(make_document(kvp("_id", key_id)));

    // 6. Replace the _id field in the copied key document with a UUID with base64 value
    // AAAAAAAAAAAAAAAAAAAAAA== (16 bytes all equal to 0x00) and insert the modified key document
    // into keyvault.datakeys with majority write concern.
    std::vector<uint8_t> id = {
        0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0};
    bsoncxx::types::b_binary id_bin{
        bsoncxx::binary_sub_type::k_uuid, (uint32_t)id.size(), id.data()};
    auto key_doc = make_document(kvp("_id", id_bin));

    mongocxx::libbson::scoped_bson_t bson_doc;
    bson_doc.init_from_static(doc);
    mongocxx::libbson::scoped_bson_t doc_without_id;
    bson_copy_to_excluding_noinit(bson_doc.bson(), doc_without_id.bson_for_init(), "_id", NULL);

    bsoncxx::document::value new_doc(doc_without_id.steal());

    bsoncxx::builder::basic::document builder;
    builder.append(concatenate(key_doc.view()));
    builder.append(concatenate(new_doc.view()));
    auto doc_with_new_key = builder.extract();

    write_concern wc_majority;
    wc_majority.acknowledge_level(write_concern::level::k_majority);

    options::insert insert_opts;
    insert_opts.write_concern(std::move(wc_majority));

    client["keyvault"]["datakeys"].insert_one(doc_with_new_key.view(), insert_opts);

    // 7. Using client_encryption, encrypt the string "test" with the modified data key using the
    // AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic algorithm and assert the resulting value is equal
    // to the following (given as base64):
    // AQAAAAAAAAAAAAAAAAAAAAACz0ZOLuuhEYi807ZXTdhbqhLaS2/t9wLifJnnNYwiw79d75QYIZ6M/aYC1h9nCzCjZ7pGUpAuNnkUhnIXM3PjrA==
    options::encrypt encrypt_opts{};
    encrypt_opts.key_id(key_doc.view()["_id"].get_value());
    encrypt_opts.algorithm(options::encrypt::encryption_algorithm::k_deterministic);

    auto to_encrypt = make_value("test");

    auto encrypted = client_encryption.encrypt(to_encrypt.view(), encrypt_opts);
    std::vector<uint8_t> expected = {
        0x1,  0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0x0,  0x0,
        0x0,  0x0,  0x0,  0x2,  0xcf, 0x46, 0x4e, 0x2e, 0xeb, 0xa1, 0x11, 0x88, 0xbc, 0xd3,
        0xb6, 0x57, 0x4d, 0xd8, 0x5b, 0xaa, 0x12, 0xda, 0x4b, 0x6f, 0xed, 0xf7, 0x2,  0xe2,
        0x7c, 0x99, 0xe7, 0x35, 0x8c, 0x22, 0xc3, 0xbf, 0x5d, 0xef, 0x94, 0x18, 0x21, 0x9e,
        0x8c, 0xfd, 0xa6, 0x2,  0xd6, 0x1f, 0x67, 0xb,  0x30, 0xa3, 0x67, 0xba, 0x46, 0x52,
        0x90, 0x2e, 0x36, 0x79, 0x14, 0x86, 0x72, 0x17, 0x33, 0x73, 0xe3, 0xac};

    auto encrypted_as_binary = encrypted.view().get_binary();
    REQUIRE(expected.size() == encrypted_as_binary.size);
    REQUIRE(std::memcmp(expected.data(), encrypted_as_binary.bytes, expected.size()) == 0);
}

enum struct RangeFieldType : int {
    DecimalNoPrecision,
    DecimalPrecision,
    DoubleNoPrecision,
    DoublePrecision,
    Date,
    Int,
    Long,
};

std::string to_type_str(RangeFieldType field_type) {
    switch (field_type) {
        case RangeFieldType::DecimalNoPrecision:
            return "DecimalNoPrecision";
        case RangeFieldType::DecimalPrecision:
            return "DecimalPrecision";
        case RangeFieldType::DoubleNoPrecision:
            return "DoubleNoPrecision";
        case RangeFieldType::DoublePrecision:
            return "DoublePrecision";
        case RangeFieldType::Date:
            return "Date";
        case RangeFieldType::Int:
            return "Int";
        case RangeFieldType::Long:
            return "Long";
    };

    FAIL("unexpected field type " << static_cast<int>(field_type));
    MONGOCXX_UNREACHABLE;
}

bsoncxx::types::bson_value::value to_field_value(int test_value, RangeFieldType field_type) {
    switch (field_type) {
        case RangeFieldType::DecimalNoPrecision:
        case RangeFieldType::DecimalPrecision:
            return {bsoncxx::decimal128(std::to_string(test_value))};
        case RangeFieldType::DoubleNoPrecision:
        case RangeFieldType::DoublePrecision:
            return {static_cast<double>(test_value)};
        case RangeFieldType::Date:
            return {std::chrono::milliseconds(test_value)};
        case RangeFieldType::Int:
            return {test_value};
        case RangeFieldType::Long:
            return {std::int64_t{test_value}};
    }

    FAIL("unexpected field type " << static_cast<int>(field_type));
    MONGOCXX_UNREACHABLE;
}

options::range to_range_opts(RangeFieldType field_type) {
    using namespace bsoncxx::types;

    switch (field_type) {
        case RangeFieldType::DecimalNoPrecision:
            return options::range().sparsity(1);
        case RangeFieldType::DecimalPrecision:
            return options::range()
                .min(make_value(b_decimal128{bsoncxx::decimal128(std::to_string(0))}))
                .max(make_value(b_decimal128{bsoncxx::decimal128(std::to_string(200))}))
                .sparsity(1)
                .precision(2);
        case RangeFieldType::DoubleNoPrecision:
            return options::range().sparsity(1);
        case RangeFieldType::DoublePrecision:
            return options::range()
                .min(make_value(b_double{0.0}))
                .max(make_value(b_double{200.0}))
                .sparsity(1)
                .precision(2);
        case RangeFieldType::Date:
            return options::range()
                .min(make_value(b_date{std::chrono::milliseconds(0)}))
                .max(make_value(b_date{std::chrono::milliseconds(200)}))
                .sparsity(1);
        case RangeFieldType::Int:
            return options::range()
                .min(make_value(b_int32{0}))
                .max(make_value(b_int32{200}))
                .sparsity(1);
        case RangeFieldType::Long:
            return options::range()
                .min(make_value(b_int64{0}))
                .max(make_value(b_int64{200}))
                .sparsity(1);
    }

    FAIL("unexpected field type " << static_cast<int>(field_type));
    MONGOCXX_UNREACHABLE;
}

struct field_type_values {
    bsoncxx::types::bson_value::value v0;
    bsoncxx::types::bson_value::value v6;
    bsoncxx::types::bson_value::value v30;
    bsoncxx::types::bson_value::value v200;

    explicit field_type_values(RangeFieldType field_type)
        : v0(to_field_value(0, field_type)),
          v6(to_field_value(6, field_type)),
          v30(to_field_value(30, field_type)),
          v200(to_field_value(200, field_type)) {}
};

struct range_explicit_encryption_objects {
    options::range range_opts;
    bsoncxx::document::value key1_document = make_document();
    bsoncxx::types::bson_value::view key1_id;
    std::unique_ptr<mongocxx::client> key_vault_client_ptr;
    std::unique_ptr<mongocxx::client_encryption> client_encryption_ptr;
    std::unique_ptr<mongocxx::client> encrypted_client_ptr;
    std::string field_name;
    std::unique_ptr<field_type_values> field_values_ptr;
};

range_explicit_encryption_objects range_explicit_encryption_setup(const std::string& type_str,
                                                                  RangeFieldType field_type) {
    range_explicit_encryption_objects res;

    // Load the file for the specific data type being tested `range-encryptedFields-<type>.json`.
    const auto encrypted_fields =
        _doc_from_file("/explicit-encryption/range-encryptedFields-" + type_str + ".json");
    const auto collection_options = make_document(kvp("encryptedFields", encrypted_fields));

    // Load the file key1-document.json as `key1Document`.
    auto& key1_document =
        (res.key1_document = _doc_from_file("/explicit-encryption/key1-document.json"));

    // Read the "_id" field of key1Document as `key1ID`.
    const auto& key1_id = (res.key1_id = key1_document["_id"].get_value());

    const auto wc_majority = []() -> mongocxx::write_concern {
        write_concern res;
        res.acknowledge_level(write_concern::level::k_majority);
        return res;
    }();

    const auto rc_majority = []() -> mongocxx::read_concern {
        read_concern res;
        res.acknowledge_level(read_concern::level::k_majority);
        return res;
    }();

    const auto empty_doc = make_document();

    auto client = mongocxx::client(uri(), test_util::add_test_server_api());

    // Drop and create the collection `db.explicit_encryption` using `encryptedFields` as an option.
    {
        auto db = client["db"];
        db["explicit_encryption"].drop();
        db.create_collection("explicit_encryption", collection_options.view());
    }

    {
        auto keyvault = client["keyvault"];

        // Drop and create the collection `keyvault.datakeys`.
        keyvault["datakeys"].drop();
        auto datakeys = keyvault.create_collection("datakeys");

        // Insert `key1Document` in `keyvault.datakeys` with majority write concern.
        datakeys.insert_one(key1_document.view(), options::insert().write_concern(wc_majority));
    }

    const auto kms_providers = _make_kms_doc(false);

    using bsoncxx::stdx::make_unique;

    // Create a MongoClient named `keyVaultClient`.
    auto& key_vault_client = *(res.key_vault_client_ptr = make_unique<mongocxx::client>(
                                   uri(), test_util::add_test_server_api()));

    // Create a ClientEncryption object named `clientEncryption` with these options:
    //   ClientEncryptionOpts {
    //      keyVaultClient: <keyVaultClient>;
    //      keyVaultNamespace: "keyvault.datakeys";
    //      kmsProviders: { "local": { "key": <base64 decoding of LOCAL_MASTERKEY> } }
    //   }
    auto& client_encryption =
        *(res.client_encryption_ptr = make_unique<mongocxx::client_encryption>(
              options::client_encryption()
                  .key_vault_client(&key_vault_client)
                  .key_vault_namespace({"keyvault", "datakeys"})
                  .kms_providers(kms_providers.view())));

    // Create a MongoClient named `encryptedClient` with these `AutoEncryptionOpts`:
    //   AutoEncryptionOpts {
    //      keyVaultNamespace: "keyvault.datakeys";
    //      kmsProviders: { "local": { "key": <base64 decoding of LOCAL_MASTERKEY> } }
    //      bypassQueryAnalysis: true
    //   }
    auto& encrypted_client = *(res.encrypted_client_ptr = make_unique<mongocxx::client>(
                                   uri(),
                                   test_util::add_test_server_api().auto_encryption_opts(
                                       options::auto_encryption()
                                           .key_vault_namespace({"keyvault", "datakeys"})
                                           .kms_providers(kms_providers.view())
                                           .bypass_query_analysis(true))));

    // Ensure the type matches with the type of the encrypted field.
    const auto& field_values = *(res.field_values_ptr = make_unique<field_type_values>(field_type));
    const auto& field_name = (res.field_name = "encrypted" + type_str);
    const auto& range_opts = (res.range_opts = to_range_opts(field_type));

    // Encrypt these values with the matching `RangeOpts` listed in Test Setup: RangeOpts and these
    // `EncryptOpts`:
    //   class EncryptOpts {
    //      keyId : <key1ID>:
    //      algorithm: "RangePreview",
    //      contentionFactor: 0
    //   }
    const auto encrypt_opts =
        options::encrypt()
            .range_opts(range_opts)
            .key_id(key1_id)
            .algorithm(options::encrypt::encryption_algorithm::k_range_preview)
            .contention_factor(0);

    // Use `clientEncryption` to encrypt these values: 0, 6, 30, and 200.
    const auto encrypted_v0 = client_encryption.encrypt(field_values.v0, encrypt_opts);
    const auto encrypted_v6 = client_encryption.encrypt(field_values.v6, encrypt_opts);
    const auto encrypted_v30 = client_encryption.encrypt(field_values.v30, encrypt_opts);
    const auto encrypted_v200 = client_encryption.encrypt(field_values.v200, encrypt_opts);

    auto explicit_encryption = encrypted_client["db"]["explicit_encryption"];

    // Use `encryptedClient` to insert these documents into
    // `db.explicit_encryption`:
    //   { "encrypted<Type>": <encrypted 0>, _id: 0 }
    //   { "encrypted<Type>": <encrypted 6>, _id: 1 }
    //   { "encrypted<Type>": <encrypted 30>, _id: 2 }
    //   { "encrypted<Type>": <encrypted 200>, _id: 3 }
    explicit_encryption.insert_one(make_document(kvp(field_name, encrypted_v0), kvp("_id", 0)));
    explicit_encryption.insert_one(make_document(kvp(field_name, encrypted_v6), kvp("_id", 1)));
    explicit_encryption.insert_one(make_document(kvp(field_name, encrypted_v30), kvp("_id", 2)));
    explicit_encryption.insert_one(make_document(kvp(field_name, encrypted_v200), kvp("_id", 3)));

    return res;
}

// Prose Test 22
TEST_CASE("Range Explicit Encryption", "[client_side_encryption]") {
    instance::current();

    if (!mongocxx::test_util::should_run_client_side_encryption_test()) {
        return;
    }

    // Tests for `DecimalNoPrecision` must only run against a replica set.
    auto is_replica_set = false;

    {
        auto client = mongocxx::client(mongocxx::uri(), test_util::add_test_server_api());

        if (!test_util::newer_than(client, "7.0")) {
            WARN("Skipping - MongoDB server 7.0 or newer required");
            return;
        }

        if (test_util::newer_than(client, "8.0")) {
            WARN(
                "Skipping - test is skipped on MongoDB server 8.0 or newer pending updates for "
                "DRIVERS-2776");
            return;
        }

        if (test_util::get_topology(client) == "single") {
            WARN("Skipping - must not run against a standalone server");
            return;
        }

        is_replica_set = test_util::get_topology(client) == "replicaset";
    }

    const RangeFieldType field_types[] = {
        RangeFieldType::DecimalNoPrecision,
        RangeFieldType::DecimalPrecision,
        RangeFieldType::DoubleNoPrecision,
        RangeFieldType::DoublePrecision,
        RangeFieldType::Date,
        RangeFieldType::Int,
        RangeFieldType::Long,
    };

    for (const auto& field_type : field_types) {
        const auto type_str = to_type_str(field_type);

        DYNAMIC_SECTION("Field Type - " << type_str) {
            if (field_type == RangeFieldType::DecimalNoPrecision && !is_replica_set) {
                WARN("Skipping - must only run against a replica set");
                continue;
            }

            auto test_objects = range_explicit_encryption_setup(type_str, field_type);

            REQUIRE(test_objects.client_encryption_ptr);
            REQUIRE(test_objects.encrypted_client_ptr);
            REQUIRE(test_objects.field_values_ptr);

            const auto& range_opts = test_objects.range_opts;
            const auto& key1_id = test_objects.key1_id;
            auto& client_encryption = *test_objects.client_encryption_ptr;
            auto& encrypted_client = *test_objects.encrypted_client_ptr;
            const auto& field_name = test_objects.field_name;
            const auto& field_values = *test_objects.field_values_ptr;

            auto explicit_encryption = encrypted_client["db"]["explicit_encryption"];

            SECTION("Case 1: can decrypt a payload") {
                // Use `clientEncryption.encrypt()` to encrypt the value 6.
                const auto& original = field_values.v6;

                // Encrypt with the matching `RangeOpts` listed in Test Setup: RangeOpts and these
                // `EncryptOpts`:
                //   class EncryptOpts {
                //      keyId : <key1ID>
                //      algorithm: "RangePreview",
                //      contentionFactor: 0
                //   }
                // Store the result in insertPayload.
                const auto insert_payload = client_encryption.encrypt(
                    original.view(),
                    options::encrypt()
                        .range_opts(range_opts)
                        .key_id(key1_id)
                        .algorithm(options::encrypt::encryption_algorithm::k_range_preview)
                        .contention_factor(0));

                // Use `clientEncryption` to decrypt `insertPayload`.
                const auto result = client_encryption.decrypt(insert_payload);

                // Assert the returned value equals 6.
                REQUIRE(result == original);
            }

            SECTION("Case 2: can find encrypted range and return the maximum") {
                // Use clientEncryption.encryptExpression() to encrypt this query:
                //   {"$and": [{"encrypted<Type>": {"$gte": 6}}, {"encrypted<Type>": {"$lte":
                //   200}}]}
                const auto query = make_document(kvp(
                    "$and",
                    make_array(
                        make_document(kvp(field_name, make_document(kvp("$gte", field_values.v6)))),
                        make_document(
                            kvp(field_name, make_document(kvp("$lte", field_values.v200)))))));

                // Use the matching `RangeOpts` listed in Test Setup: RangeOpts and these
                // `EncryptOpts` to encrypt the query:
                //   class EncryptOpts {
                //      keyId : <key1ID>
                //      algorithm: "RangePreview",
                //      queryType: "rangePreview",
                //      contentionFactor: 0
                //   }
                // Store the result in `findPayload`.
                const auto find_payload = client_encryption.encrypt_expression(
                    query.view(),
                    options::encrypt()
                        .range_opts(range_opts)
                        .key_id(key1_id)
                        .algorithm(options::encrypt::encryption_algorithm::k_range_preview)
                        .query_type(options::encrypt::encryption_query_type::k_range_preview)
                        .contention_factor(0));

                // Use encryptedClient to run a "find" operation on the `db.explicit_encryption`
                // collection with the filter findPayload and sort the results by _id.
                auto cursor = explicit_encryption.find(
                    find_payload.view(),
                    options::find()
                        .sort(make_document(kvp("_id", 1)))
                        .projection(make_document(kvp("_id", 0), kvp(field_name, 1))));

                // Assert these three documents are returned:
                //  - { "encrypted<Type>": 6 }
                //  - { "encrypted<Type>": 30 }
                //  - { "encrypted<Type>": 200 }
                const auto expected = std::vector<bsoncxx::document::value>({
                    make_document(kvp(field_name, field_values.v6)),
                    make_document(kvp(field_name, field_values.v30)),
                    make_document(kvp(field_name, field_values.v200)),
                });

                const auto actual =
                    std::vector<bsoncxx::document::value>(cursor.begin(), cursor.end());

                REQUIRE(actual == expected);
            }

            SECTION("Case 3: can find encrypted range and return the minimum") {
                // Use `clientEncryption.encryptExpression()` to encrypt this query:
                //   {"$and": [{"encrypted<Type>": {"$gte": 0}}, {"encrypted<Type>": {"$lte": 6}}]}
                const auto query = make_document(kvp(
                    "$and",
                    make_array(
                        make_document(kvp(field_name, make_document(kvp("$gte", field_values.v0)))),
                        make_document(
                            kvp(field_name, make_document(kvp("$lte", field_values.v6)))))));

                // Use the matching `RangeOpts` listed in Test Setup: RangeOpts and these
                // `EncryptOpts` to encrypt the query:
                //   class EncryptOpts {
                //      keyId : <key1ID>
                //      algorithm: "RangePreview",
                //      queryType: "rangePreview",
                //      contentionFactor: 0
                //   }
                // Store the result in `findPayload`.
                const auto find_payload = client_encryption.encrypt_expression(
                    query.view(),
                    options::encrypt()
                        .range_opts(range_opts)
                        .key_id(key1_id)
                        .algorithm(options::encrypt::encryption_algorithm::k_range_preview)
                        .query_type(options::encrypt::encryption_query_type::k_range_preview)
                        .contention_factor(0));

                // Use `encryptedClient` to run a "find" operation on the `db.explicit_encryption`
                // collection with the filter `findPayload` and sort the results by `_id`.
                auto cursor = explicit_encryption.find(
                    find_payload.view(),
                    options::find()
                        .sort(make_document(kvp("_id", 1)))
                        .projection(make_document(kvp("_id", 0), kvp(field_name, 1))));

                // Assert these two documents are returned:
                //  - { "encrypted<Type>": 0 }
                //  - { "encrypted<Type>": 6 }
                const auto expected = std::vector<bsoncxx::document::value>({
                    make_document(kvp(field_name, field_values.v0)),
                    make_document(kvp(field_name, field_values.v6)),
                });

                const auto actual =
                    std::vector<bsoncxx::document::value>(cursor.begin(), cursor.end());

                REQUIRE(actual == expected);
            }

            SECTION("Case 4: can find encrypted range with an open range query") {
                // Use clientEncryption.encryptExpression() to encrypt this query:
                //   {"$and": [{"encrypted<Type>": {"$gt": 30}}]}
                const auto query = make_document(
                    kvp("$and",
                        make_array(make_document(
                            kvp(field_name, make_document(kvp("$gt", field_values.v30)))))));

                // Use the matching `RangeOpts` listed in Test Setup: RangeOpts and these
                // `EncryptOpts` to encrypt the query:
                //   class EncryptOpts {
                //      keyId : <key1ID>
                //      algorithm: "RangePreview",
                //      queryType: "rangePreview",
                //      contentionFactor: 0
                //   }
                // Store the result in `findPayload`.
                const auto find_payload = client_encryption.encrypt_expression(
                    query.view(),
                    options::encrypt()
                        .range_opts(range_opts)
                        .key_id(key1_id)
                        .algorithm(options::encrypt::encryption_algorithm::k_range_preview)
                        .query_type(options::encrypt::encryption_query_type::k_range_preview)
                        .contention_factor(0));

                // Use encryptedClient to run a "find" operation on the `db.explicit_encryption`
                // collection with the filter findPayload and sort the results by _id.
                auto cursor = explicit_encryption.find(
                    find_payload.view(),
                    options::find()
                        .sort(make_document(kvp("_id", 1)))
                        .projection(make_document(kvp("_id", 0), kvp(field_name, 1))));

                // Assert that only this document is returned:
                //  - { "encrypted<Type>": 200 }
                const auto expected = std::vector<bsoncxx::document::value>({
                    make_document(kvp(field_name, field_values.v200)),
                });

                const auto actual =
                    std::vector<bsoncxx::document::value>(cursor.begin(), cursor.end());

                REQUIRE(actual == expected);
            }

            SECTION("Case 5: can run an aggregation expression inside $expr") {
                // Use clientEncryption.encryptExpression() to encrypt this query:
                //   {'$and': [ { '$lt': [ '$encrypted<Type>', 30 ] } ] } }
                const auto query = make_document(
                    kvp("$and",
                        make_array(make_document(
                            kvp(field_name, make_document(kvp("$lt", field_values.v30)))))));

                // Use the matching `RangeOpts` listed in Test Setup: RangeOpts and these
                // `EncryptOpts` to encrypt the query:
                //   class EncryptOpts {
                //      keyId : <key1ID>
                //      algorithm: "RangePreview",
                //      queryType: "rangePreview",
                //      contentionFactor: 0
                //   }
                // Store the result in `findPayload`.
                const auto find_payload = client_encryption.encrypt_expression(
                    query.view(),
                    options::encrypt()
                        .range_opts(range_opts)
                        .key_id(key1_id)
                        .algorithm(options::encrypt::encryption_algorithm::k_range_preview)
                        .query_type(options::encrypt::encryption_query_type::k_range_preview)
                        .contention_factor(0));

                // Use encryptedClient to run a "find" operation on the `db.explicit_encryption`
                // collection with the filter findPayload and sort the results by _id.
                auto cursor = explicit_encryption.find(
                    find_payload.view(),
                    options::find()
                        .sort(make_document(kvp("_id", 1)))
                        .projection(make_document(kvp("_id", 0), kvp(field_name, 1))));

                // Assert these two documents are returned:
                //  - { "encrypted<Type>": 0 }
                //  - { "encrypted<Type>": 6 }
                const auto expected = std::vector<bsoncxx::document::value>({
                    make_document(kvp(field_name, field_values.v0)),
                    make_document(kvp(field_name, field_values.v6)),
                });

                const auto actual =
                    std::vector<bsoncxx::document::value>(cursor.begin(), cursor.end());

                REQUIRE(actual == expected);
            }

            switch (field_type) {
                case RangeFieldType::DoubleNoPrecision:
                case RangeFieldType::DecimalNoPrecision:
                    // This test case should be skipped if the encrypted field is
                    // `encryptedDoubleNoPrecision` or `encryptedDecimalNoPrecision`.
                    break;
                default: {
                    SECTION("Case 6: encrypting a document greater than the maximum errors") {
                        const auto original = to_field_value(201, field_type);

                        // Use clientEncryption.encrypt() to try to encrypt the value 201 with the
                        // matching RangeOpts listed in Test Setup: RangeOpts and these EncryptOpts:
                        //   class EncryptOpts {
                        //      keyId : <key1ID>
                        //      algorithm: "RangePreview",
                        //      contentionFactor: 0
                        //   }
                        // The error should be raised because 201 is greater than the maximum value
                        // in RangeOpts. Assert that an error was raised.
                        REQUIRE_THROWS_WITH(
                            client_encryption.encrypt(
                                original.view(),
                                options::encrypt()
                                    .range_opts(range_opts)
                                    .key_id(key1_id)
                                    .algorithm(
                                        options::encrypt::encryption_algorithm::k_range_preview)
                                    .contention_factor(0)),
                            Catch::Contains(
                                "Value must be greater than or equal to the minimum value and "
                                "less than or equal to the maximum value"));
                    }
                    break;
                }
            }

            switch (field_type) {
                case RangeFieldType::DoubleNoPrecision:
                case RangeFieldType::DecimalNoPrecision:
                    // This test case should be skipped if the encrypted field is
                    // `encryptedDoubleNoPrecision`.
                    break;
                default: {
                    SECTION("Case 7: encrypting a document of a different type errors") {
                        // For all the tests below use these EncryptOpts:
                        //   class EncryptOpts {
                        //      keyId : <key1ID>
                        //      algorithm: "RangePreview",
                        //      contentionFactor: 0
                        //   }
                        const auto encrypt_opts =
                            options::encrypt()
                                .range_opts(range_opts)
                                .key_id(key1_id)
                                .algorithm(options::encrypt::encryption_algorithm::k_range_preview)
                                .contention_factor(0);

                        // If the encrypted field is encryptedInt encrypt:
                        //   { "encryptedInt": { "$numberDouble": "6" } }
                        // Otherwise, encrypt:
                        //   { "encrypted<Type>": { "$numberInt": "6" } }
                        const auto value = field_type == RangeFieldType::Int
                                               ? to_field_value(6, RangeFieldType::DoublePrecision)
                                               : to_field_value(6, RangeFieldType::Int);

                        // Assert an error was raised.
                        REQUIRE_THROWS_WITH(
                            client_encryption.encrypt(value.view(), encrypt_opts),
                            Catch::Contains("expected matching 'min' and value type"));
                    }
                    break;
                }
            }

            switch (field_type) {
                case RangeFieldType::DoublePrecision:
                case RangeFieldType::DoubleNoPrecision:
                case RangeFieldType::DecimalPrecision:
                case RangeFieldType::DecimalNoPrecision:
                    // This test case should be skipped if the encrypted field is
                    // `encryptedDoublePrecision` or `encryptedDoubleNoPrecision` or
                    // `encryptedDecimalPrecision` or `encryptedDecimalNoPrecision`.
                    break;
                default: {
                    SECTION("Case 8: setting precision errors if the type is not a double") {
                        // Use `clientEncryption.encrypt()` to try to encrypt the value 6 with these
                        // `EncryptOpts` and these `RangeOpts`:
                        //   class EncryptOpts {
                        //      keyId : <key1ID>
                        //      algorithm: "RangePreview",
                        //      contentionFactor: 0
                        //   }
                        //
                        //   class RangeOpts {
                        //      min: 0,
                        //      max: 200,
                        //      sparsity: 1,
                        //      precision: 2,
                        //   }
                        // Assert an error was raised.
                        REQUIRE_THROWS_WITH(
                            client_encryption.encrypt(
                                field_values.v6,
                                options::encrypt()
                                    .range_opts(options::range()
                                                    .min(to_field_value(0, field_type))
                                                    .max(to_field_value(200, field_type))
                                                    .sparsity(1)
                                                    .precision(2))
                                    .key_id(key1_id)
                                    .algorithm(
                                        options::encrypt::encryption_algorithm::k_range_preview)
                                    .contention_factor(0)),
                            Catch::Contains(
                                "expected 'precision' to be set with double or decimal128 index"));
                    }
                } break;
            }
        }
    }
}

TEST_CASE("16. Rewrap. Case 2: RewrapManyDataKeyOpts.provider is not optional",
          "[client_side_encryption]") {
    instance::current();

    if (!mongocxx::test_util::should_run_client_side_encryption_test()) {
        return;
    }

    auto keyvault_client = mongocxx::client(mongocxx::uri(), test_util::add_test_server_api());
    auto ce_opts = mongocxx::options::client_encryption();
    ce_opts.key_vault_client(&keyvault_client);
    ce_opts.key_vault_namespace({"keyvault", "datakeys"});
    ce_opts.kms_providers(_make_kms_doc(true /* include_external */));

    auto clientEncryption = mongocxx::client_encryption(ce_opts);
    REQUIRE_THROWS_WITH(
        clientEncryption.rewrap_many_datakey(
            make_document(), mongocxx::options::rewrap_many_datakey().master_key(make_document())),
        Catch::Contains("expected 'provider' to be set to identify type of 'master_key'"));
}

}  // namespace
