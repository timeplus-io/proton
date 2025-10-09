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

#include <bsoncxx/stdx/make_unique.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <mongocxx/client_session.hpp>
#include <mongocxx/exception/private/mongoc_error.hh>
#include <mongocxx/index_view.hpp>
#include <mongocxx/options/index_view.hpp>
#include <mongocxx/private/index_view.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

index_view::index_view(void* coll, void* client)
    : _impl{stdx::make_unique<impl>(static_cast<mongoc_collection_t*>(coll),
                                    static_cast<mongoc_client_t*>(client))} {}

index_view::index_view(index_view&&) noexcept = default;
index_view& index_view::operator=(index_view&&) noexcept = default;
index_view::~index_view() = default;

cursor index_view::list() {
    return _get_impl().list(nullptr);
}

cursor index_view::list(const client_session& session) {
    return _get_impl().list(&session);
}

bsoncxx::v_noabi::stdx::optional<std::string> index_view::create_one(
    const bsoncxx::v_noabi::document::view_or_value& keys,
    const bsoncxx::v_noabi::document::view_or_value& index_options,
    const options::index_view& options) {
    return create_one(index_model{keys, index_options}, options);
}

bsoncxx::v_noabi::stdx::optional<std::string> index_view::create_one(
    const client_session& session,
    const bsoncxx::v_noabi::document::view_or_value& keys,
    const bsoncxx::v_noabi::document::view_or_value& index_options,
    const options::index_view& options) {
    return create_one(session, index_model{keys, index_options}, options);
}

bsoncxx::v_noabi::stdx::optional<std::string> index_view::create_one(
    const index_model& model, const options::index_view& options) {
    return _get_impl().create_one(nullptr, model, options);
}

bsoncxx::v_noabi::stdx::optional<std::string> index_view::create_one(
    const client_session& session, const index_model& model, const options::index_view& options) {
    return _get_impl().create_one(&session, model, options);
}

bsoncxx::v_noabi::document::value index_view::create_many(const std::vector<index_model>& indexes,
                                                          const options::index_view& options) {
    return _get_impl().create_many(nullptr, indexes, options);
}

bsoncxx::v_noabi::document::value index_view::create_many(const client_session& session,
                                                          const std::vector<index_model>& indexes,
                                                          const options::index_view& options) {
    return _get_impl().create_many(&session, indexes, options);
}

void index_view::drop_one(bsoncxx::v_noabi::stdx::string_view name,
                          const options::index_view& options) {
    return _get_impl().drop_one(nullptr, name, options);
}

void index_view::drop_one(const client_session& session,
                          bsoncxx::v_noabi::stdx::string_view name,
                          const options::index_view& options) {
    return _get_impl().drop_one(&session, name, options);
}

void index_view::drop_one(const bsoncxx::v_noabi::document::view_or_value& keys,
                          const bsoncxx::v_noabi::document::view_or_value& index_options,
                          const options::index_view& options) {
    bsoncxx::v_noabi::document::view opts_view = index_options.view();

    if (opts_view["name"]) {
        drop_one(bsoncxx::v_noabi::string::to_string(opts_view["name"].get_string().value),
                 options);
    } else {
        drop_one(_get_impl().get_index_name_from_keys(keys), options);
    }
}

void index_view::drop_one(const client_session& session,
                          const bsoncxx::v_noabi::document::view_or_value& keys,
                          const bsoncxx::v_noabi::document::view_or_value& index_options,
                          const options::index_view& options) {
    bsoncxx::v_noabi::document::view opts_view = index_options.view();

    if (opts_view["name"]) {
        drop_one(session,
                 bsoncxx::v_noabi::string::to_string(opts_view["name"].get_string().value),
                 options);
    } else {
        drop_one(session, _get_impl().get_index_name_from_keys(keys), options);
    }
}

void index_view::drop_one(const index_model& model, const options::index_view& options) {
    drop_one(model.keys(), model.options(), options);
}

void index_view::drop_one(const client_session& session,
                          const index_model& model,
                          const options::index_view& options) {
    drop_one(session, model.keys(), model.options(), options);
}

void index_view::drop_all(const options::index_view& options) {
    _get_impl().drop_all(nullptr, options);
}

void index_view::drop_all(const client_session& session, const options::index_view& options) {
    _get_impl().drop_all(&session, options);
}

index_view::impl& index_view::_get_impl() {
    return *_impl;
}

}  // namespace v_noabi
}  // namespace mongocxx
