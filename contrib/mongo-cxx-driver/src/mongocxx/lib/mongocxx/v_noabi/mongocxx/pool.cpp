// Copyright 2015 MongoDB Inc.
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

#include <utility>

#include <bsoncxx/stdx/make_unique.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/exception/error_code.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/exception/private/mongoc_error.hh>
#include <mongocxx/options/private/apm.hh>
#include <mongocxx/options/private/server_api.hh>
#include <mongocxx/options/private/ssl.hh>
#include <mongocxx/pool.hpp>
#include <mongocxx/private/client.hh>
#include <mongocxx/private/pool.hh>
#include <mongocxx/private/uri.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

// Attempts to create a new client pool using the uri. Throws an exception upon error.
static mongoc_client_pool_t* construct_client_pool(mongoc_uri_t* uri) {
    bson_error_t error;
    auto pool = libmongoc::client_pool_new_with_error(uri, &error);
    if (!pool) {
        // If constructing a client pool failed, throw an exception from the bson_error_t.
        throw_exception<operation_exception>(error);
    }

    return pool;
}

void pool::_release(client* client) {
    libmongoc::client_pool_push(_impl->client_pool_t, client->_get_impl().client_t);
    // prevent client destructor from destroying the underlying mongoc_client_t
    client->_get_impl().client_t = nullptr;
    delete client;
}

pool::~pool() = default;

pool::pool(const uri& uri, const options::pool& options)
    : _impl{stdx::make_unique<impl>(construct_client_pool(uri._impl->uri_t))} {
#if defined(MONGOCXX_ENABLE_SSL) && defined(MONGOC_ENABLE_SSL)
    if (options.client_opts().tls_opts()) {
        if (!uri.tls())
            throw exception{error_code::k_invalid_parameter,
                            "cannot set TLS options if 'tls=true' not in URI"};

        auto mongoc_opts = options::make_tls_opts(*options.client_opts().tls_opts());
        _impl->tls_options = std::move(mongoc_opts.second);
        libmongoc::client_pool_set_ssl_opts(_impl->client_pool_t, &mongoc_opts.first);
    }
#else
    if (uri.tls() || options.client_opts().tls_opts())
        throw exception{error_code::k_ssl_not_supported};
#endif

    if (options.client_opts().auto_encryption_opts()) {
        const auto& auto_encrypt_opts = *(options.client_opts().auto_encryption_opts());
        auto mongoc_auto_encrypt_opts =
            static_cast<mongoc_auto_encryption_opts_t*>(auto_encrypt_opts.convert());

        bson_error_t error;
        auto r = libmongoc::client_pool_enable_auto_encryption(
            _impl->client_pool_t, mongoc_auto_encrypt_opts, &error);

        libmongoc::auto_encryption_opts_destroy(mongoc_auto_encrypt_opts);

        if (!r) {
            throw_exception<operation_exception>(error);
        }
    }

    if (options.client_opts().apm_opts()) {
        _impl->listeners = *options.client_opts().apm_opts();
        auto callbacks = options::make_apm_callbacks(_impl->listeners);
        // We cast the APM class to a void* so we can pass it into libmongoc's context.
        // It will be cast back to an APM class in the event handlers.
        auto context = static_cast<void*>(&(_impl->listeners));
        libmongoc::client_pool_set_apm_callbacks(_impl->client_pool_t, callbacks.get(), context);
    }

    if (options.client_opts().server_api_opts()) {
        const auto& server_api_opts = *options.client_opts().server_api_opts();
        auto mongoc_server_api_opts = options::make_server_api(server_api_opts);

        bson_error_t error;
        auto result = libmongoc::client_pool_set_server_api(
            _impl->client_pool_t, mongoc_server_api_opts.get(), &error);

        if (!result) {
            throw_exception<operation_exception>(error);
        }
    }
}

client* pool::entry::operator->() const& noexcept {
    return _client.get();
}

client& pool::entry::operator*() const& noexcept {
    return *_client;
}

pool::entry& pool::entry::operator=(std::nullptr_t) noexcept {
    _client = nullptr;
    return *this;
}

pool::entry::operator bool() const noexcept {
    return static_cast<bool>(_client);
}

// construct a pool entry from a pointer to a client
pool::entry::entry(pool::entry::unique_client p) : _client(std::move(p)) {}

pool::entry pool::acquire() {
    return entry(entry::unique_client(new client(libmongoc::client_pool_pop(_impl->client_pool_t)),
                                      [this](client* client) { _release(client); }));
}

stdx::optional<pool::entry> pool::try_acquire() {
    auto cli = libmongoc::client_pool_try_pop(_impl->client_pool_t);
    if (!cli)
        return stdx::nullopt;

    return entry(
        entry::unique_client(new client(cli), [this](client* client) { _release(client); }));
}

}  // namespace v_noabi
}  // namespace mongocxx
