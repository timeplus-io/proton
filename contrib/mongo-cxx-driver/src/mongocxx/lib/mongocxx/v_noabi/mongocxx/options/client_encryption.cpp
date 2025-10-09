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

#include <mongocxx/client.hpp>
#include <mongocxx/options/client_encryption.hpp>
#include <mongocxx/private/client.hh>
#include <mongocxx/private/libbson.hh>
#include <mongocxx/private/libmongoc.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace options {

client_encryption& client_encryption::key_vault_client(mongocxx::v_noabi::client* client) {
    _key_vault_client = client;
    return *this;
}

const stdx::optional<mongocxx::v_noabi::client*>& client_encryption::key_vault_client() const {
    return _key_vault_client;
}

client_encryption& client_encryption::key_vault_namespace(client_encryption::ns_pair ns) {
    _key_vault_namespace = ns;
    return *this;
}

const stdx::optional<client_encryption::ns_pair>& client_encryption::key_vault_namespace() const {
    return _key_vault_namespace;
}

client_encryption& client_encryption::kms_providers(
    bsoncxx::v_noabi::document::view_or_value kms_providers) {
    _kms_providers = std::move(kms_providers);
    return *this;
}

const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& client_encryption::kms_providers()
    const {
    return _kms_providers;
}

client_encryption& client_encryption::tls_opts(bsoncxx::v_noabi::document::view_or_value tls_opts) {
    _tls_opts = std::move(tls_opts);
    return *this;
}

const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& client_encryption::tls_opts()
    const {
    return _tls_opts;
}

void* client_encryption::convert() const {
    mongoc_client_encryption_opts_t* opts_t = libmongoc::client_encryption_opts_new();

    if (_key_vault_client) {
        mongoc_client_t* client_t = (*_key_vault_client)->_get_impl().client_t;
        libmongoc::client_encryption_opts_set_keyvault_client(opts_t, client_t);
    }

    if (_key_vault_namespace) {
        auto ns = *_key_vault_namespace;
        libmongoc::client_encryption_opts_set_keyvault_namespace(
            opts_t, ns.first.c_str(), ns.second.c_str());
    }

    if (_kms_providers) {
        libbson::scoped_bson_t kms_providers{*_kms_providers};
        libmongoc::client_encryption_opts_set_kms_providers(opts_t, kms_providers.bson());
    }

    if (_tls_opts) {
        libbson::scoped_bson_t tls_opts{*_tls_opts};
        libmongoc::client_encryption_opts_set_tls_opts(opts_t, tls_opts.bson());
    }

    return opts_t;
}

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx
