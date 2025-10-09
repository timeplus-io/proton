// Copyright 2017-present MongoDB Inc.
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
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/exception/private/mongoc_error.hh>
#include <mongocxx/private/client.hh>
#include <mongocxx/private/client_session.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

// Private constructors.
client_session::client_session(const mongocxx::v_noabi::client* client,
                               const mongocxx::v_noabi::options::client_session& options)
    : _impl(stdx::make_unique<impl>(client, options)) {}

client_session::client_session(client_session&&) noexcept = default;

client_session& client_session::operator=(client_session&&) noexcept = default;

client_session::~client_session() noexcept = default;

const mongocxx::v_noabi::client& client_session::client() const noexcept {
    return _impl->client();
}

const mongocxx::v_noabi::options::client_session& client_session::options() const noexcept {
    return _impl->options();
}

std::uint32_t client_session::server_id() const noexcept {
    return _impl->server_id();
}

bsoncxx::v_noabi::document::view client_session::id() const noexcept {
    return _impl->id();
}

bsoncxx::v_noabi::document::view client_session::cluster_time() const noexcept {
    return _impl->cluster_time();
}

bsoncxx::v_noabi::types::b_timestamp client_session::operation_time() const noexcept {
    return _impl->operation_time();
}

void client_session::advance_cluster_time(const bsoncxx::v_noabi::document::view& cluster_time) {
    _impl->advance_cluster_time(cluster_time);
}

void client_session::advance_operation_time(
    const bsoncxx::v_noabi::types::b_timestamp& operation_time) {
    _impl->advance_operation_time(operation_time);
}

void client_session::start_transaction(
    const stdx::optional<options::transaction>& transaction_opts) {
    _impl->start_transaction(transaction_opts);
}

void client_session::commit_transaction() {
    _impl->commit_transaction();
}

void client_session::abort_transaction() {
    _impl->abort_transaction();
}

void client_session::with_transaction(with_transaction_cb cb, options::transaction opts) {
    _impl->with_transaction(this, std::move(cb), std::move(opts));
}

client_session::transaction_state client_session::get_transaction_state() const noexcept {
    return _impl->get_transaction_state();
}

bool client_session::get_dirty() const noexcept {
    return _impl->get_dirty();
}

const client_session::impl& client_session::_get_impl() const {
    // Never null.
    return *_impl;
}

client_session::impl& client_session::_get_impl() {
    auto cthis = const_cast<const client_session*>(this);
    return const_cast<client_session::impl&>(cthis->_get_impl());
}

}  // namespace v_noabi
}  // namespace mongocxx
