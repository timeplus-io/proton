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
#include <mongocxx/exception/error_code.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/options/auto_encryption.hpp>
#include <mongocxx/pool.hpp>
#include <mongocxx/private/client.hh>
#include <mongocxx/private/libbson.hh>
#include <mongocxx/private/libmongoc.hh>
#include <mongocxx/private/pool.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace options {

auto_encryption::auto_encryption() noexcept : _bypass(false), _bypass_query_analysis(false) {}

auto_encryption& auto_encryption::key_vault_client(mongocxx::v_noabi::client* client) {
    _key_vault_client = client;
    return *this;
}

const stdx::optional<mongocxx::v_noabi::client*>& auto_encryption::key_vault_client() const {
    return _key_vault_client;
}

auto_encryption& auto_encryption::key_vault_pool(mongocxx::v_noabi::pool* pool) {
    _key_vault_pool = pool;
    return *this;
}

const stdx::optional<mongocxx::v_noabi::pool*>& auto_encryption::key_vault_pool() const {
    return _key_vault_pool;
}

auto_encryption& auto_encryption::key_vault_namespace(auto_encryption::ns_pair ns) {
    _key_vault_namespace = ns;
    return *this;
}

const stdx::optional<auto_encryption::ns_pair>& auto_encryption::key_vault_namespace() const {
    return _key_vault_namespace;
}

auto_encryption& auto_encryption::kms_providers(
    bsoncxx::v_noabi::document::view_or_value kms_providers) {
    _kms_providers = std::move(kms_providers);
    return *this;
}

const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& auto_encryption::kms_providers()
    const {
    return _kms_providers;
}

auto_encryption& auto_encryption::tls_opts(bsoncxx::v_noabi::document::view_or_value tls_opts) {
    _tls_opts = std::move(tls_opts);
    return *this;
}

const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& auto_encryption::tls_opts() const {
    return _tls_opts;
}

auto_encryption& auto_encryption::schema_map(bsoncxx::v_noabi::document::view_or_value schema_map) {
    _schema_map = std::move(schema_map);
    return *this;
}

const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& auto_encryption::schema_map()
    const {
    return _schema_map;
}

auto_encryption& auto_encryption::encrypted_fields_map(
    bsoncxx::v_noabi::document::view_or_value encrypted_fields_map) {
    _encrypted_fields_map = std::move(encrypted_fields_map);
    return *this;
}

const stdx::optional<bsoncxx::v_noabi::document::view_or_value>&
auto_encryption::encrypted_fields_map() const {
    return _encrypted_fields_map;
}

auto_encryption& auto_encryption::bypass_auto_encryption(bool should_bypass) {
    _bypass = should_bypass;
    return *this;
}

bool auto_encryption::bypass_auto_encryption() const {
    return _bypass;
}

auto_encryption& auto_encryption::bypass_query_analysis(bool should_bypass) {
    _bypass_query_analysis = should_bypass;
    return *this;
}

bool auto_encryption::bypass_query_analysis() const {
    return _bypass_query_analysis;
}

auto_encryption& auto_encryption::extra_options(bsoncxx::v_noabi::document::view_or_value extra) {
    _extra_options = std::move(extra);
    return *this;
}

const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& auto_encryption::extra_options()
    const {
    return _extra_options;
}

void* auto_encryption::convert() const {
    using libbson::scoped_bson_t;

    if (_key_vault_client && _key_vault_pool) {
        throw exception{error_code::k_invalid_parameter,
                        "cannot set both key vault client and key vault pool, please choose one"};
    }

    auto mongoc_auto_encrypt_opts = libmongoc::auto_encryption_opts_new();
    if (!mongoc_auto_encrypt_opts) {
        throw std::logic_error{"could not get object from libmongoc"};
    }

    if (_key_vault_client) {
        mongoc_client_t* client_t = (*_key_vault_client)->_get_impl().client_t;
        libmongoc::auto_encryption_opts_set_keyvault_client(mongoc_auto_encrypt_opts, client_t);
    }

    if (_key_vault_pool) {
        mongoc_client_pool_t* pool_t = (*_key_vault_pool)->_impl->client_pool_t;
        libmongoc::auto_encryption_opts_set_keyvault_client_pool(mongoc_auto_encrypt_opts, pool_t);
    }

    if (_key_vault_namespace) {
        auto ns = *_key_vault_namespace;
        libmongoc::auto_encryption_opts_set_keyvault_namespace(
            mongoc_auto_encrypt_opts, ns.first.c_str(), ns.second.c_str());
    }

    if (_kms_providers) {
        scoped_bson_t kms_providers{*_kms_providers};
        libmongoc::auto_encryption_opts_set_kms_providers(mongoc_auto_encrypt_opts,
                                                          kms_providers.bson());
    }

    if (_tls_opts) {
        scoped_bson_t tls_opts{*_tls_opts};
        libmongoc::auto_encryption_opts_set_tls_opts(mongoc_auto_encrypt_opts, tls_opts.bson());
    }

    if (_schema_map) {
        scoped_bson_t schema_map{*_schema_map};
        libmongoc::auto_encryption_opts_set_schema_map(mongoc_auto_encrypt_opts, schema_map.bson());
    }

    if (_encrypted_fields_map) {
        scoped_bson_t encrypted_fields_map{*_encrypted_fields_map};
        libmongoc::auto_encryption_opts_set_encrypted_fields_map(mongoc_auto_encrypt_opts,
                                                                 encrypted_fields_map.bson());
    }

    if (_bypass) {
        libmongoc::auto_encryption_opts_set_bypass_auto_encryption(mongoc_auto_encrypt_opts, true);
    }

    if (_bypass_query_analysis) {
        libmongoc::auto_encryption_opts_set_bypass_query_analysis(mongoc_auto_encrypt_opts, true);
    }

    if (_extra_options) {
        scoped_bson_t extra{*_extra_options};
        libmongoc::auto_encryption_opts_set_extra(mongoc_auto_encrypt_opts, extra.bson());
    }

    return mongoc_auto_encrypt_opts;
}

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx
