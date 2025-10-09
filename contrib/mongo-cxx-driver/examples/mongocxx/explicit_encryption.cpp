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

using bsoncxx::builder::stream::close_document;
using bsoncxx::builder::stream::document;
using bsoncxx::builder::stream::finalize;
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

    // Create an unencrypted mongocxx::client.
    class client client {
        uri {}
    };
    auto coll = client["test"]["coll"];

    // Clear old data.
    coll.drop();

    // Set up the key vault for this example.
    auto key_vault = client["keyvault"]["datakeys"];

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
    client_encryption_opts.key_vault_client(&client);

    class client_encryption client_encryption(std::move(client_encryption_opts));

    // Create a new data key for the encryptedField.
    auto data_key_id = client_encryption.create_data_key("local");

    // Explicitly encrypt a field:
    options::encrypt encrypt_opts{};
    encrypt_opts.key_id(data_key_id.view());
    encrypt_opts.algorithm(options::encrypt::encryption_algorithm::k_deterministic);

    auto to_encrypt = make_value("secret message");
    auto encrypted_message = client_encryption.encrypt(to_encrypt, encrypt_opts);

    coll.insert_one(make_document(kvp("encryptedField", encrypted_message)));

    // Automatically decrypts any encrypted fields.
    auto res = coll.find_one({});
    std::cout << "Explicitly encrypted document: " << bsoncxx::to_json(*res) << std::endl;

    // Explicitly decrypt the field:
    auto encrypted_message_retrieved = res->view()["encryptedField"].get_value();
    auto decrypted_message = client_encryption.decrypt(encrypted_message_retrieved);
    std::cout << "Explicitly decrypted message: " << decrypted_message.view().get_string().value
              << std::endl;
}
