// Copyright 2014 MongoDB Inc.
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

#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/stdx/make_unique.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/exception/error_code.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/exception/private/mongoc_error.hh>
#include <mongocxx/options/auto_encryption.hpp>
#include <mongocxx/options/private/apm.hh>
#include <mongocxx/options/private/server_api.hh>
#include <mongocxx/options/private/ssl.hh>
#include <mongocxx/private/client.hh>
#include <mongocxx/private/client_session.hh>
#include <mongocxx/private/libbson.hh>
#include <mongocxx/private/pipeline.hh>
#include <mongocxx/private/read_concern.hh>
#include <mongocxx/private/read_preference.hh>
#include <mongocxx/private/uri.hh>
#include <mongocxx/private/write_concern.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

using namespace libbson;
using bsoncxx::v_noabi::builder::basic::kvp;

namespace {
class database_names {
   public:
    explicit database_names(char** names) {
        _names = names;
    };

    ~database_names() {
        bson_strfreev(_names);
    }

    database_names(database_names&&) = delete;
    database_names& operator=(database_names&&) = delete;
    database_names(const database_names&) = delete;
    database_names& operator=(const database_names&) = delete;

    const char* operator[](const std::size_t i) const {
        return _names[i];
    };

    bool operator!() const {
        return _names == nullptr;
    }

   private:
    char** _names;
};
}  // namespace

client::client() noexcept = default;

client::client(const mongocxx::v_noabi::uri& uri, const options::client& options) {
#if defined(MONGOCXX_ENABLE_SSL) && defined(MONGOC_ENABLE_SSL)
    if (options.tls_opts()) {
        if (!uri.tls())
            throw exception{error_code::k_invalid_parameter,
                            "cannot set TLS options if 'tls=true' not in URI"};
    }
#else
    if (uri.tls() || options.tls_opts()) {
        throw exception{error_code::k_ssl_not_supported};
    }
#endif
    auto new_client = libmongoc::client_new_from_uri(uri._impl->uri_t);
    if (!new_client) {
        // Shouldn't happen after checks above, but future libmongoc's may change behavior.
        throw exception{error_code::k_invalid_parameter, "could not construct client from URI"};
    }

    _impl = stdx::make_unique<impl>(std::move(new_client));

    if (options.apm_opts()) {
        _impl->listeners = *options.apm_opts();
        auto callbacks = options::make_apm_callbacks(_impl->listeners);
        // We cast the APM class to a void* so we can pass it into libmongoc's context.
        // It will be cast back to an APM class in the event handlers.
        auto context = static_cast<void*>(&(_impl->listeners));
        libmongoc::client_set_apm_callbacks(_get_impl().client_t, callbacks.get(), context);
    }

    if (options.auto_encryption_opts()) {
        const auto& auto_encrypt_opts = *options.auto_encryption_opts();
        auto mongoc_auto_encrypt_opts =
            static_cast<mongoc_auto_encryption_opts_t*>(auto_encrypt_opts.convert());

        bson_error_t error;
        auto r = libmongoc::client_enable_auto_encryption(
            _get_impl().client_t, mongoc_auto_encrypt_opts, &error);

        libmongoc::auto_encryption_opts_destroy(mongoc_auto_encrypt_opts);

        if (!r) {
            throw_exception<operation_exception>(error);
        }
    }

    if (options.server_api_opts()) {
        const auto& server_api_opts = *options.server_api_opts();
        auto mongoc_server_api_opts = options::make_server_api(server_api_opts);

        bson_error_t error;
        auto result = libmongoc::client_set_server_api(
            _get_impl().client_t, mongoc_server_api_opts.get(), &error);

        if (!result) {
            throw_exception<operation_exception>(error);
        }
    }

#if defined(MONGOCXX_ENABLE_SSL) && defined(MONGOC_ENABLE_SSL)
    if (options.tls_opts()) {
        auto mongoc_opts = options::make_tls_opts(*options.tls_opts());
        _impl->tls_options = std::move(mongoc_opts.second);
        libmongoc::client_set_ssl_opts(_get_impl().client_t, &mongoc_opts.first);
    }
#endif
}

client::client(void* implementation)
    : _impl{stdx::make_unique<impl>(static_cast<::mongoc_client_t*>(implementation))} {}

client::client(client&&) noexcept = default;
client& client::operator=(client&&) noexcept = default;

client::~client() = default;

client::operator bool() const noexcept {
    return static_cast<bool>(_impl);
}

void client::read_concern_deprecated(mongocxx::v_noabi::read_concern rc) {
    auto client_t = _get_impl().client_t;
    libmongoc::client_set_read_concern(client_t, rc._impl->read_concern_t);
}

void client::read_concern(mongocxx::v_noabi::read_concern rc) {
    return read_concern_deprecated(std::move(rc));
}

mongocxx::v_noabi::read_concern client::read_concern() const {
    auto rc = libmongoc::client_get_read_concern(_get_impl().client_t);
    return {stdx::make_unique<read_concern::impl>(libmongoc::read_concern_copy(rc))};
}

void client::read_preference_deprecated(mongocxx::v_noabi::read_preference rp) {
    libmongoc::client_set_read_prefs(_get_impl().client_t, rp._impl->read_preference_t);
}

void client::read_preference(mongocxx::v_noabi::read_preference rp) {
    return read_preference_deprecated(std::move(rp));
}

mongocxx::v_noabi::read_preference client::read_preference() const {
    mongocxx::v_noabi::read_preference rp(stdx::make_unique<read_preference::impl>(
        libmongoc::read_prefs_copy(libmongoc::client_get_read_prefs(_get_impl().client_t))));
    return rp;
}

mongocxx::v_noabi::uri client::uri() const {
    mongocxx::v_noabi::uri connection_string(stdx::make_unique<uri::impl>(
        libmongoc::uri_copy(libmongoc::client_get_uri(_get_impl().client_t))));
    return connection_string;
}

void client::write_concern_deprecated(mongocxx::v_noabi::write_concern wc) {
    libmongoc::client_set_write_concern(_get_impl().client_t, wc._impl->write_concern_t);
}

void client::write_concern(mongocxx::v_noabi::write_concern wc) {
    return write_concern_deprecated(std::move(wc));
}

mongocxx::v_noabi::write_concern client::write_concern() const {
    mongocxx::v_noabi::write_concern wc(stdx::make_unique<write_concern::impl>(
        libmongoc::write_concern_copy(libmongoc::client_get_write_concern(_get_impl().client_t))));
    return wc;
}

mongocxx::v_noabi::database client::database(bsoncxx::v_noabi::string::view_or_value name) const& {
    return mongocxx::v_noabi::database(*this, std::move(name));
}

cursor client::list_databases() const {
    return libmongoc::client_find_databases_with_opts(_get_impl().client_t, nullptr);
}

cursor client::list_databases(const client_session& session) const {
    bsoncxx::v_noabi::builder::basic::document options_doc;
    options_doc.append(
        bsoncxx::v_noabi::builder::concatenate_doc{session._get_impl().to_document()});
    scoped_bson_t options_bson(options_doc.extract());
    return libmongoc::client_find_databases_with_opts(_get_impl().client_t, options_bson.bson());
}

cursor client::list_databases(const bsoncxx::v_noabi::document::view_or_value opts) const {
    scoped_bson_t opts_bson{opts.view()};
    return libmongoc::client_find_databases_with_opts(_get_impl().client_t, opts_bson.bson());
}

cursor client::list_databases(const client_session& session,
                              const bsoncxx::v_noabi::document::view_or_value opts) const {
    bsoncxx::v_noabi::builder::basic::document options_doc;
    options_doc.append(
        bsoncxx::v_noabi::builder::concatenate_doc{session._get_impl().to_document()});
    options_doc.append(bsoncxx::v_noabi::builder::concatenate_doc{opts});
    mongocxx::libbson::scoped_bson_t opts_bson(options_doc.extract());
    return libmongoc::client_find_databases_with_opts(_get_impl().client_t, opts_bson.bson());
}

std::vector<std::string> client::list_database_names(
    bsoncxx::v_noabi::document::view_or_value filter) const {
    bsoncxx::v_noabi::builder::basic::document options_builder;

    options_builder.append(kvp("filter", filter));

    scoped_bson_t options_bson(options_builder.extract());
    bson_error_t error;

    const database_names names(libmongoc::client_get_database_names_with_opts(
        _get_impl().client_t, options_bson.bson(), &error));

    if (!names) {
        throw_exception<operation_exception>(error);
    }

    std::vector<std::string> _names;
    for (std::size_t i = 0u; names[i]; ++i) {
        _names.emplace_back(names[i]);
    }

    return _names;
}

std::vector<std::string> client::list_database_names(
    const client_session& session, const bsoncxx::v_noabi::document::view_or_value filter) const {
    bsoncxx::v_noabi::builder::basic::document options_builder;

    options_builder.append(
        bsoncxx::v_noabi::builder::concatenate_doc{session._get_impl().to_document()});
    options_builder.append(kvp("filter", filter));

    mongocxx::libbson::scoped_bson_t opts_bson(options_builder.extract());
    bson_error_t error;

    const database_names names(libmongoc::client_get_database_names_with_opts(
        _get_impl().client_t, opts_bson.bson(), &error));

    if (!names) {
        throw_exception<operation_exception>(error);
    }

    std::vector<std::string> res;
    for (std::size_t i = 0u; names[i]; ++i) {
        res.emplace_back(names[i]);
    }

    return res;
}

mongocxx::v_noabi::client_session client::start_session(
    const mongocxx::v_noabi::options::client_session& options) {
    return client_session(this, options);
}

void client::reset() {
    libmongoc::client_reset(_get_impl().client_t);
}

change_stream client::watch(const options::change_stream& options) {
    return watch(pipeline{}, options);
}

change_stream client::watch(const client_session& session, const options::change_stream& options) {
    return _watch(&session, pipeline{}, options);
}

change_stream client::watch(const pipeline& pipe, const options::change_stream& options) {
    return _watch(nullptr, pipe, options);
}

change_stream client::watch(const client_session& session,
                            const pipeline& pipe,
                            const options::change_stream& options) {
    return _watch(&session, pipe, options);
}

change_stream client::_watch(const client_session* session,
                             const pipeline& pipe,
                             const options::change_stream& options) {
    bsoncxx::v_noabi::builder::basic::document container;
    container.append(bsoncxx::v_noabi::builder::basic::kvp("pipeline", pipe._impl->view_array()));
    scoped_bson_t pipeline_bson{container.view()};

    bsoncxx::v_noabi::builder::basic::document options_builder;
    options_builder.append(bsoncxx::v_noabi::builder::concatenate(options.as_bson()));
    if (session) {
        options_builder.append(
            bsoncxx::v_noabi::builder::concatenate_doc{session->_get_impl().to_document()});
    }

    scoped_bson_t options_bson{options_builder.extract()};

    return change_stream{
        libmongoc::client_watch(_get_impl().client_t, pipeline_bson.bson(), options_bson.bson())};
}

const client::impl& client::_get_impl() const {
    if (!_impl) {
        throw logic_error{error_code::k_invalid_client_object};
    }
    return *_impl;
}

client::impl& client::_get_impl() {
    auto cthis = const_cast<const client*>(this);
    return const_cast<client::impl&>(cthis->_get_impl());
}

}  // namespace v_noabi
}  // namespace mongocxx
