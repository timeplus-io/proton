// Copyright 2020-present MongoDB Inc.
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
#include <iostream>
#include <random>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types/bson_value/make_value.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/client_encryption.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/auto_encryption.hpp>
#include <mongocxx/options/client.hpp>
#include <mongocxx/options/data_key.hpp>
#include <mongocxx/options/encrypt.hpp>

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

using bsoncxx::builder::stream::close_array;
using bsoncxx::builder::stream::close_document;
using bsoncxx::builder::stream::document;
using bsoncxx::builder::stream::finalize;
using bsoncxx::builder::stream::open_array;
using bsoncxx::builder::stream::open_document;

using bsoncxx::types::bson_value::make_value;

using namespace mongocxx;

const int kKeyLength = 96;

int main(int, char**) {
    instance inst{};

    // This must be the same master key that was used to create
    // the encryption key; here, we use a random key as a placeholder.
    char key_storage[kKeyLength];
    std::generate_n(key_storage, kKeyLength, std::rand);
    bsoncxx::types::b_binary local_master_key{
        bsoncxx::binary_sub_type::k_binary, kKeyLength, (const uint8_t*)&key_storage};

    auto kms_providers = document{} << "local" << open_document << "key" << local_master_key
                                    << close_document << finalize;

    class client key_vault_client {
        uri {}
    };
    auto key_vault = key_vault_client["keyvault"]["datakeys"];

    // Ensure that two data keys cannot share the same keyAltName by adding
    // a unique index on this field.
    key_vault.drop();

    mongocxx::options::index index_options{};
    index_options.unique(true);
    auto expression = document{} << "keyAltNames" << open_document << "$exists" << true
                                 << close_document << finalize;
    index_options.partial_filter_expression(expression.view());
    key_vault.create_index(make_document(kvp("keyAltNames", 1)), index_options);

    // Set up our client_encryption
    options::client_encryption client_encryption_opts{};
    client_encryption_opts.key_vault_namespace({"keyvault", "datakeys"});
    client_encryption_opts.kms_providers(kms_providers.view());
    client_encryption_opts.key_vault_client(&key_vault_client);

    class client_encryption client_encryption(std::move(client_encryption_opts));

    // Create a new data key for the encryptedField.
    auto data_key_id = client_encryption.create_data_key("local");

    // Create a new json schema for the encryptedField.
    auto json_schema = document{} << "properties" << open_document << "encryptedField"
                                  << open_document << "encrypt" << open_document << "keyId"
                                  << open_array << data_key_id << close_array << "bsonType"
                                  << "string"
                                  << "algorithm"
                                  << "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic" << close_document
                                  << close_document << close_document << "bsonType"
                                  << "object" << finalize;

    // Set up auto encryption opts.
    options::auto_encryption auto_encrypt_opts{};
    auto_encrypt_opts.kms_providers(kms_providers.view());
    auto_encrypt_opts.key_vault_namespace({"keyvault", "datakeys"});

    // Create a client with auto encryption enabled.
    options::client client_opts{};
    client_opts.auto_encryption_opts(std::move(auto_encrypt_opts));
    class client client {
        uri{}, std::move(client_opts)
    };

    auto db = client["test"];

    {
        // Clear old data.
        auto coll = db["coll"];
        coll.drop();
    }

    // Create the collection with the encryption JSON Schema.
    auto cmd = document{} << "create"
                          << "coll"
                          << "validator" << open_document << "$jsonSchema" << json_schema.view()
                          << close_document << finalize;
    db.run_command(cmd.view());

    auto coll = db["coll"];

    auto to_insert = make_document(kvp("encryptedField", "123456789"));
    coll.insert_one(to_insert.view());

    auto res = coll.find_one({});

    std::cout << "Decrypted document: " << bsoncxx::to_json(*res) << std::endl;

    class client unencrypted_client {
        uri {}
    };
    res = unencrypted_client["test"]["coll"].find_one({});

    std::cout << "Encrypted document: " << bsoncxx::to_json(*res) << std::endl;

    try {
        // Inserting via the unencrypted client fails, because the server
        // requires that the encryptedField actually be encrypted.
        unencrypted_client["test"]["coll"].insert_one(to_insert.view());
    } catch (const std::exception& e) {
        std::cout << "Unencrypted insert failed: " << e.what() << std::endl;
    }
}
