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

#include <mongocxx/options/data_key.hpp>
#include <mongocxx/private/libbson.hh>
#include <mongocxx/private/libmongoc.hh>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {
namespace v_noabi {
namespace options {

data_key& data_key::master_key(bsoncxx::v_noabi::document::view_or_value master_key) {
    _master_key = std::move(master_key);
    return *this;
}

const stdx::optional<bsoncxx::v_noabi::document::view_or_value>& data_key::master_key() const {
    return _master_key;
}

data_key& data_key::key_alt_names(std::vector<std::string> key_alt_names) {
    _key_alt_names = std::move(key_alt_names);

    return *this;
}

const std::vector<std::string>& data_key::key_alt_names() const {
    return _key_alt_names;
}

void* data_key::convert() const {
    mongoc_client_encryption_datakey_opts_t* opts_t =
        libmongoc::client_encryption_datakey_opts_new();

    if (_master_key) {
        libbson::scoped_bson_t master_key{*_master_key};
        libmongoc::client_encryption_datakey_opts_set_masterkey(opts_t, master_key.bson());
    }

    if (!_key_alt_names.empty()) {
        auto altnames = _key_alt_names;
        char** names = (char**)bson_malloc(sizeof(char*) * altnames.size());
        uint32_t i = 0;

        for (auto&& name : altnames) {
            names[i++] = const_cast<char*>(name.data());
        }

        libmongoc::client_encryption_datakey_opts_set_keyaltnames(opts_t, names, i);

        bson_free(names);
    }

    if (_key_material) {
        uint32_t size = static_cast<uint32_t>(_key_material->size());
        libmongoc::client_encryption_datakey_opts_set_keymaterial(
            opts_t, _key_material->data(), size);
    }

    return opts_t;
}

data_key& data_key::key_material(data_key::key_material_type key_material) {
    _key_material = std::move(key_material);
    return *this;
}

const stdx::optional<data_key::key_material_type>& data_key::key_material() {
    return _key_material;
}

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx
