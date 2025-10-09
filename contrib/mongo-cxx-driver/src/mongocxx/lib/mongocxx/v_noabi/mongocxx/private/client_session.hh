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

#pragma once

#include <exception>

#include <bsoncxx/private/helpers.hh>
#include <bsoncxx/private/libbson.hh>
#include <mongocxx/client_session.hpp>
#include <mongocxx/exception/error_code.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/exception/private/mongoc_error.hh>
#include <mongocxx/options/private/transaction.hh>
#include <mongocxx/private/client.hh>
#include <mongocxx/private/libbson.hh>
#include <mongocxx/private/libmongoc.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

namespace {

struct with_transaction_ctx {
    client_session* parent;
    client_session::with_transaction_cb cb;
    std::exception_ptr eptr;
};

// The callback we pass into libmongoc is a wrapped version of the
// user callback. Before giving control back to libmongoc, we convert
// any exception the user callback emits into an error_t and reply object;
// libmongoc uses these to determine whether to retry.
bool with_transaction_cpp_cb(mongoc_client_session_t*,
                             void* ctx,
                             bson_t** reply,
                             bson_error_t* error) noexcept {
    with_transaction_ctx* cb_ctx = static_cast<with_transaction_ctx*>(ctx);

    try {
        cb_ctx->cb(cb_ctx->parent);
        return true;
    } catch (const operation_exception& e) {
        make_bson_error(error, e);
        if (e.raw_server_error()) {
            libbson::scoped_bson_t raw{e.raw_server_error()->view()};
            *reply = bson_copy(raw.bson());
        }
        return false;
    } catch (...) {
        cb_ctx->eptr = std::current_exception();
        make_generic_bson_error(error);
        return false;
    }
}

}  // namespace

class client_session::impl {
   public:
    impl(const mongocxx::v_noabi::client* client, const options::client_session& session_options)
        : _client(client), _options(session_options), _session_t(nullptr, nullptr) {
        // Create a mongoc_session_opts_t from session_options.
        std::unique_ptr<mongoc_session_opt_t, decltype(libmongoc::session_opts_destroy)> opt_t{
            libmongoc::session_opts_new(), libmongoc::session_opts_destroy};

        if (_options._causal_consistency) {
            libmongoc::session_opts_set_causal_consistency(opt_t.get(),
                                                           session_options.causal_consistency());
        }

        if (_options._enable_snapshot_reads) {
            libmongoc::session_opts_set_snapshot(opt_t.get(), session_options.snapshot());
        }

        if (session_options.default_transaction_opts()) {
            libmongoc::session_opts_set_default_transaction_opts(
                opt_t.get(),
                (session_options.default_transaction_opts())->_get_impl().get_transaction_opt_t());
        }

        bson_error_t error;
        auto s =
            libmongoc::client_start_session(_client->_get_impl().client_t, opt_t.get(), &error);
        if (!s) {
            throw mongocxx::v_noabi::exception{error_code::k_cannot_create_session, error.message};
        }

        _session_t = unique_session{
            s, [](mongoc_client_session_t* cs) { libmongoc::client_session_destroy(cs); }};
    }

    const mongocxx::v_noabi::client& client() const noexcept {
        return *_client;
    }

    const options::client_session& options() const noexcept {
        return _options;
    }

    std::uint32_t server_id() const noexcept {
        return libmongoc::client_session_get_server_id(_session_t.get());
    }

    // Get session id, also known as "logical session id" or "lsid".
    bsoncxx::v_noabi::document::view id() const noexcept {
        return bsoncxx::helpers::view_from_bson_t(
            libmongoc::client_session_get_lsid(_session_t.get()));
    }

    bsoncxx::v_noabi::document::view cluster_time() const noexcept {
        const bson_t* ct = libmongoc::client_session_get_cluster_time(_session_t.get());
        if (ct) {
            return bsoncxx::helpers::view_from_bson_t(ct);
        }

        return bsoncxx::helpers::view_from_bson_t(&_empty_cluster_time);
    }

    bsoncxx::v_noabi::types::b_timestamp operation_time() const noexcept {
        bsoncxx::v_noabi::types::b_timestamp ts;
        libmongoc::client_session_get_operation_time(
            _session_t.get(), &ts.timestamp, &ts.increment);
        return ts;
    }

    void advance_cluster_time(const bsoncxx::v_noabi::document::view& cluster_time) noexcept {
        bson_t bson;
        bson_init_static(&bson, cluster_time.data(), cluster_time.length());
        libmongoc::client_session_advance_cluster_time(_session_t.get(), &bson);
    }

    void advance_operation_time(
        const bsoncxx::v_noabi::types::b_timestamp& operation_time) noexcept {
        libmongoc::client_session_advance_operation_time(
            _session_t.get(), operation_time.timestamp, operation_time.increment);
    }

    void start_transaction(const stdx::optional<options::transaction>& transaction_opts) {
        bson_error_t error;
        mongoc_transaction_opt_t* transaction_opt_t = nullptr;

        if (transaction_opts) {
            transaction_opt_t = transaction_opts->_get_impl().get_transaction_opt_t();
        }

        if (!libmongoc::client_session_start_transaction(
                _session_t.get(), transaction_opt_t, &error)) {
            throw_exception<operation_exception>(error);
        }
    }

    void commit_transaction() {
        libbson::scoped_bson_t reply;
        bson_error_t error;
        if (!libmongoc::client_session_commit_transaction(
                _session_t.get(), reply.bson_for_init(), &error)) {
            throw_exception<operation_exception>(reply.steal(), error);
        }
    }

    void abort_transaction() {
        bson_error_t error;
        if (!libmongoc::client_session_abort_transaction(_session_t.get(), &error)) {
            throw_exception<operation_exception>(error);
        }
    }

    void with_transaction(client_session* parent,
                          client_session::with_transaction_cb cb,
                          options::transaction opts) {
        auto session_t = _session_t.get();
        auto opts_t = opts._get_impl().get_transaction_opt_t();

        with_transaction_ctx ctx{parent, std::move(cb), nullptr};

        libbson::scoped_bson_t reply;
        bson_error_t error;

        auto res = libmongoc::client_session_with_transaction(
            session_t, &with_transaction_cpp_cb, opts_t, &ctx, reply.bson_for_init(), &error);

        if (!res) {
            if (ctx.eptr) {
                std::rethrow_exception(ctx.eptr);
            }

            throw_exception<operation_exception>(reply.steal(), error);
        }
    }

    transaction_state get_transaction_state() const noexcept {
        switch (libmongoc::client_session_get_transaction_state(_session_t.get())) {
            case MONGOC_TRANSACTION_NONE:
                return transaction_state::k_transaction_none;
            case MONGOC_TRANSACTION_STARTING:
                return transaction_state::k_transaction_starting;
            case MONGOC_TRANSACTION_IN_PROGRESS:
                return transaction_state::k_transaction_in_progress;
            case MONGOC_TRANSACTION_COMMITTED:
                return transaction_state::k_transaction_committed;
            case MONGOC_TRANSACTION_ABORTED:
                return transaction_state::k_transaction_aborted;
            default:
                MONGOCXX_UNREACHABLE;
        }
    }

    bool get_dirty() const noexcept {
        return libmongoc::client_session_get_dirty(_session_t.get());
    }

    bsoncxx::v_noabi::document::value to_document() const {
        bson_error_t error;
        bson_t bson = BSON_INITIALIZER;
        if (!libmongoc::client_session_append(_session_t.get(), &bson, &error)) {
            throw mongocxx::v_noabi::logic_error{error_code::k_invalid_session, error.message};
        }

        // document::value takes ownership of the bson buffer.
        return bsoncxx::helpers::value_from_bson_t(&bson);
    }

    mongoc_client_session_t* get_session_t() const noexcept {
        return _session_t.get();
    }

   private:
    const mongocxx::v_noabi::client* _client;
    options::client_session _options;

    using unique_session =
        std::unique_ptr<mongoc_client_session_t,
                        std::function<void MONGOCXX_CALL(mongoc_client_session_t*)>>;

    unique_session _session_t;

    bson_t _empty_cluster_time = BSON_INITIALIZER;
};

}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
