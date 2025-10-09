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

#include <bsoncxx/stdx/make_unique.hpp>
#include <bsoncxx/types.hpp>
#include <mongocxx/exception/error_code.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/private/libmongoc.hh>
#include <mongocxx/private/read_concern.hh>
#include <mongocxx/private/read_preference.hh>
#include <mongocxx/private/uri.hh>
#include <mongocxx/private/write_concern.hh>
#include <mongocxx/uri.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

namespace {

// Some of the 'uri_get_*' string accessors may return nullptr.  Check for this case and convert to
// the empty string.
std::string to_string_null_safe(const char* str) {
    if (str == nullptr) {
        return std::string{};
    }
    return str;
}

}  // namespace

const std::string uri::k_default_uri = "mongodb://localhost:27017";

uri::uri(std::unique_ptr<impl>&& implementation) {
    _impl.reset(implementation.release());
}

uri::uri(bsoncxx::v_noabi::string::view_or_value uri_string) {
    bson_error_t error;

    _impl = stdx::make_unique<impl>(
        libmongoc::uri_new_with_error(uri_string.terminated().data(), &error));

    if (_impl->uri_t == nullptr) {
        throw logic_error{error_code::k_invalid_uri, error.message};
    }
}

uri::uri(uri&&) noexcept = default;
uri& uri::operator=(uri&&) noexcept = default;

uri::~uri() = default;

std::string uri::auth_mechanism() const {
    return to_string_null_safe(libmongoc::uri_get_auth_mechanism(_impl->uri_t));
}

std::string uri::auth_source() const {
    return libmongoc::uri_get_auth_source(_impl->uri_t);
}

std::string uri::database() const {
    return to_string_null_safe(libmongoc::uri_get_database(_impl->uri_t));
}

std::vector<uri::host> uri::hosts() const {
    std::vector<host> result;

    for (auto host_list = libmongoc::uri_get_hosts(_impl->uri_t); host_list;
         host_list = host_list->next) {
        result.push_back(host{host_list->host, host_list->port, host_list->family});
    }

    return result;
}

bsoncxx::v_noabi::document::view uri::options() const {
    auto opts_bson = libmongoc::uri_get_options(_impl->uri_t);
    return bsoncxx::v_noabi::document::view{::bson_get_data(opts_bson), opts_bson->len};
}

std::string uri::password() const {
    return to_string_null_safe(libmongoc::uri_get_password(_impl->uri_t));
}

mongocxx::v_noabi::read_concern uri::read_concern() const {
    auto rc = libmongoc::uri_get_read_concern(_impl->uri_t);
    return mongocxx::v_noabi::read_concern(
        stdx::make_unique<read_concern::impl>(libmongoc::read_concern_copy(rc)));
}

mongocxx::v_noabi::read_preference uri::read_preference() const {
    auto rp = libmongoc::uri_get_read_prefs_t(_impl->uri_t);
    return (mongocxx::v_noabi::read_preference)(
        stdx::make_unique<read_preference::impl>(libmongoc::read_prefs_copy(rp)));
}

std::string uri::replica_set() const {
    return to_string_null_safe(libmongoc::uri_get_replica_set(_impl->uri_t));
}

std::string uri::to_string() const {
    return libmongoc::uri_get_string(_impl->uri_t);
}

bool uri::ssl() const {
    return libmongoc::uri_get_tls(_impl->uri_t);
}

bool uri::tls() const {
    return libmongoc::uri_get_tls(_impl->uri_t);
}

std::string uri::username() const {
    return to_string_null_safe(libmongoc::uri_get_username(_impl->uri_t));
}

mongocxx::v_noabi::write_concern uri::write_concern() const {
    auto wc = libmongoc::uri_get_write_concern(_impl->uri_t);
    return mongocxx::v_noabi::write_concern(
        stdx::make_unique<write_concern::impl>(libmongoc::write_concern_copy(wc)));
}

static stdx::optional<stdx::string_view> _string_option(mongoc_uri_t* uri, std::string opt_name) {
    const char* value;

    value = libmongoc::uri_get_option_as_utf8(uri, opt_name.c_str(), NULL);
    if (!value) {
        return {};
    }

    return stdx::string_view{value};
}

static stdx::optional<std::int32_t> _int32_option(mongoc_uri_t* uri, std::string opt_name) {
    bson_iter_t iter;
    const bson_t* options_bson = libmongoc::uri_get_options(uri);

    if (!bson_iter_init_find_case(&iter, options_bson, opt_name.c_str()) ||
        !BSON_ITER_HOLDS_INT32(&iter)) {
        return {};
    }
    return bson_iter_int32(&iter);
}

static stdx::optional<bool> _bool_option(mongoc_uri_t* uri, std::string opt_name) {
    bson_iter_t iter;
    const bson_t* options_bson = libmongoc::uri_get_options(uri);

    if (!bson_iter_init_find_case(&iter, options_bson, opt_name.c_str()) ||
        !BSON_ITER_HOLDS_BOOL(&iter)) {
        return {};
    }
    return bson_iter_bool(&iter);
}

stdx::optional<bsoncxx::v_noabi::document::view> uri::credentials() {
    const bson_t* options_bson = libmongoc::uri_get_credentials(_impl->uri_t);
    const uint8_t* data = bson_get_data(options_bson);

    return bsoncxx::v_noabi::document::view(data, options_bson->len);
}

stdx::optional<std::int32_t> uri::srv_max_hosts() const {
    return _int32_option(_impl->uri_t, MONGOC_URI_SRVMAXHOSTS);
}

static stdx::optional<bsoncxx::v_noabi::document::view> _credential_document_option(
    mongoc_uri_t* uri, std::string opt_name) {
    bson_iter_t iter;
    const uint8_t* data;
    uint32_t len;
    const bson_t* options_bson = libmongoc::uri_get_credentials(uri);

    if (!bson_iter_init_find_case(&iter, options_bson, opt_name.c_str()) ||
        !BSON_ITER_HOLDS_DOCUMENT(&iter)) {
        return {};
    }
    bson_iter_document(&iter, &len, &data);
    return bsoncxx::v_noabi::document::view(data, len);
}

stdx::optional<stdx::string_view> uri::appname() const {
    return _string_option(_impl->uri_t, "appname");
}

// Special case. authMechanismProperties are stored as part of libmongoc's credentials.
stdx::optional<bsoncxx::v_noabi::document::view> uri::auth_mechanism_properties() const {
    return _credential_document_option(_impl->uri_t, "authMechanismProperties");
}

std::vector<stdx::string_view> uri::compressors() const {
    const bson_t* compressors;
    std::vector<stdx::string_view> result;
    bson_iter_t iter;

    compressors = libmongoc::uri_get_compressors(_impl->uri_t);
    if (!compressors) {
        // Should not happen. libmongoc will return an empty document even if there were no
        // compressors present in the URI.
        return result;
    }
    bson_iter_init(&iter, compressors);
    while (bson_iter_next(&iter)) {
        result.push_back(stdx::string_view{bson_iter_key(&iter), bson_iter_key_len(&iter)});
    }
    return result;
}

stdx::optional<std::int32_t> uri::connect_timeout_ms() const {
    return _int32_option(_impl->uri_t, "connectTimeoutMS");
}

stdx::optional<bool> uri::direct_connection() const {
    return _bool_option(_impl->uri_t, "directConnection");
}

stdx::optional<std::int32_t> uri::heartbeat_frequency_ms() const {
    return _int32_option(_impl->uri_t, "heartbeatFrequencyMS");
}

stdx::optional<std::int32_t> uri::local_threshold_ms() const {
    return _int32_option(_impl->uri_t, "localThresholdMS");
}

stdx::optional<std::int32_t> uri::max_pool_size() const {
    return _int32_option(_impl->uri_t, "maxPoolSize");
}

stdx::optional<bool> uri::retry_reads() const {
    return _bool_option(_impl->uri_t, "retryReads");
}

stdx::optional<bool> uri::retry_writes() const {
    return _bool_option(_impl->uri_t, "retryWrites");
}

stdx::optional<std::int32_t> uri::server_selection_timeout_ms() const {
    return _int32_option(_impl->uri_t, "serverSelectionTimeoutMS");
}

stdx::optional<bool> uri::server_selection_try_once() const {
    return _bool_option(_impl->uri_t, "serverSelectionTryOnce");
}

stdx::optional<std::int32_t> uri::socket_timeout_ms() const {
    return _int32_option(_impl->uri_t, "socketTimeoutMS");
}

stdx::optional<bool> uri::tls_allow_invalid_certificates() const {
    return _bool_option(_impl->uri_t, "tlsAllowInvalidCertificates");
}

stdx::optional<bool> uri::tls_allow_invalid_hostnames() const {
    return _bool_option(_impl->uri_t, "tlsAllowInvalidHostnames");
}

stdx::optional<stdx::string_view> uri::tls_ca_file() const {
    return _string_option(_impl->uri_t, "tlsCAFile");
}

stdx::optional<stdx::string_view> uri::tls_certificate_key_file() const {
    return _string_option(_impl->uri_t, "tlsCertificateKeyFile");
}

stdx::optional<stdx::string_view> uri::tls_certificate_key_file_password() const {
    return _string_option(_impl->uri_t, "tlsCertificateKeyFilePassword");
}

stdx::optional<bool> uri::tls_disable_certificate_revocation_check() const {
    return _bool_option(_impl->uri_t, "tlsDisableCertificateRevocationCheck");
}

stdx::optional<bool> uri::tls_disable_ocsp_endpoint_check() const {
    return _bool_option(_impl->uri_t, "tlsDisableOCSPEndpointCheck");
}

stdx::optional<bool> uri::tls_insecure() const {
    return _bool_option(_impl->uri_t, "tlsInsecure");
}

stdx::optional<std::int32_t> uri::wait_queue_timeout_ms() const {
    return _int32_option(_impl->uri_t, "waitQueueTimeoutMS");
}

stdx::optional<std::int32_t> uri::zlib_compression_level() const {
    return _int32_option(_impl->uri_t, "zlibCompressionLevel");
}

}  // namespace v_noabi
}  // namespace mongocxx
