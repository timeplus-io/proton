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

#include <bsoncxx/stdx/make_unique.hpp>
#include <mongocxx/client_encryption.hpp>
#include <mongocxx/private/client_encryption.hh>
#include <mongocxx/private/database.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

using mongocxx::libbson::scoped_bson_t;

client_encryption::client_encryption(options::client_encryption opts)
    : _impl(stdx::make_unique<impl>(std::move(opts))) {}

client_encryption::~client_encryption() noexcept = default;
client_encryption::client_encryption(client_encryption&&) = default;
client_encryption& client_encryption::operator=(client_encryption&&) = default;

bsoncxx::v_noabi::types::bson_value::value client_encryption::create_data_key(
    std::string kms_provider, const options::data_key& opts) {
    return _impl->create_data_key(kms_provider, opts);
}

bsoncxx::v_noabi::types::bson_value::value client_encryption::encrypt(
    bsoncxx::v_noabi::types::bson_value::view value, const options::encrypt& opts) {
    return _impl->encrypt(value, opts);
}

bsoncxx::v_noabi::document::value client_encryption::encrypt_expression(
    bsoncxx::v_noabi::document::view_or_value expr, const options::encrypt& opts) {
    return _impl->encrypt_expression(expr, opts);
}

bsoncxx::v_noabi::types::bson_value::value client_encryption::decrypt(
    bsoncxx::v_noabi::types::bson_value::view value) {
    return _impl->decrypt(value);
}

collection client_encryption::create_encrypted_collection(
    const database& db,
    const std::string& coll_name,
    const bsoncxx::v_noabi::document::view& options,
    bsoncxx::v_noabi::document::value& out_options,
    const std::string& kms_provider,
    const stdx::optional<bsoncxx::v_noabi::document::view>& masterkey) {
    auto& db_impl = db._get_impl();
    return _impl->create_encrypted_collection(
        db, db_impl.database_t, coll_name, options, out_options, kms_provider, masterkey);
}

result::rewrap_many_datakey client_encryption::rewrap_many_datakey(
    bsoncxx::v_noabi::document::view_or_value filter, const options::rewrap_many_datakey& opts) {
    return _impl->rewrap_many_datakey(filter, opts);
}

result::delete_result client_encryption::delete_key(
    bsoncxx::v_noabi::types::bson_value::view_or_value id) {
    return _impl->delete_key(id);
}

stdx::optional<bsoncxx::v_noabi::document::value> client_encryption::get_key(
    bsoncxx::v_noabi::types::bson_value::view_or_value id) {
    return _impl->get_key(id);
}

mongocxx::v_noabi::cursor client_encryption::get_keys() {
    return _impl->get_keys();
}

stdx::optional<bsoncxx::v_noabi::document::value> client_encryption::add_key_alt_name(
    bsoncxx::v_noabi::types::bson_value::view_or_value id,
    bsoncxx::v_noabi::string::view_or_value key_alt_name) {
    return _impl->add_key_alt_name(id, key_alt_name);
}

stdx::optional<bsoncxx::v_noabi::document::value> client_encryption::get_key_by_alt_name(
    bsoncxx::v_noabi::string::view_or_value key_alt_name) {
    return _impl->get_key_by_alt_name(key_alt_name);
}

stdx::optional<bsoncxx::v_noabi::document::value> client_encryption::remove_key_alt_name(
    bsoncxx::v_noabi::types::bson_value::view_or_value id,
    bsoncxx::v_noabi::string::view_or_value key_alt_name) {
    return _impl->remove_key_alt_name(id, key_alt_name);
}

}  // namespace v_noabi
}  // namespace mongocxx
