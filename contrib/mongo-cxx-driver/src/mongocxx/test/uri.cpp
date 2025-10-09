// Copyright 2016 MongoDB Inc.
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

#include <algorithm>
#include <string>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/stdx/string_view.hpp>
#include <bsoncxx/test/catch.hh>
#include <mongocxx/exception/error_code.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/uri.hpp>

namespace {
TEST_CASE("URI", "[uri]") {
    SECTION("Default URI") {
        REQUIRE_NOTHROW(mongocxx::uri{});
        REQUIRE_NOTHROW(mongocxx::uri{mongocxx::uri::k_default_uri});
        REQUIRE(mongocxx::uri{}.to_string() ==
                mongocxx::uri{mongocxx::uri::k_default_uri}.to_string());

        mongocxx::uri u{};

        // Values that should be empty with a blank URI.
        REQUIRE(u.auth_mechanism() == "");
        REQUIRE(u.auth_source() == "admin");
        REQUIRE(u.database() == "");
        REQUIRE(u.hosts().size() == 1);
        REQUIRE(u.hosts()[0].name == "localhost");
        // Don't check 'u.hosts()[0].family'.  Value is platform-dependent.
        REQUIRE(u.hosts()[0].port == 27017);
        REQUIRE(u.options().empty());
        REQUIRE(u.password() == "");
        REQUIRE(u.read_concern().acknowledge_level() ==
                mongocxx::read_concern::level::k_server_default);
        REQUIRE(u.read_concern().acknowledge_string().empty());
        REQUIRE(u.read_preference().mode() == mongocxx::read_preference::read_mode::k_primary);
        REQUIRE(!u.read_preference().tags());
        REQUIRE(!u.read_preference().max_staleness());
        REQUIRE(u.replica_set() == "");
        REQUIRE(u.tls() == false);
        REQUIRE(u.to_string() == mongocxx::uri::k_default_uri);
        REQUIRE(u.username() == "");
        REQUIRE(u.write_concern().journal() == false);
        REQUIRE(u.write_concern().majority() == false);
        REQUIRE(!u.write_concern().nodes());
        REQUIRE(u.write_concern().timeout() == std::chrono::milliseconds{0});
        REQUIRE(u.write_concern().acknowledge_level() == mongocxx::write_concern::level::k_default);
    }

    SECTION("Valid URI") {
        REQUIRE_NOTHROW(mongocxx::uri{"mongodb://example.com"});
    }

    SECTION("Invalid URI") {
        std::string invalid{"mongo://example.com"};
        REQUIRE_THROWS_AS(mongocxx::uri{invalid}, mongocxx::logic_error);
        try {
            mongocxx::uri{invalid};
        } catch (const mongocxx::logic_error& e) {
            REQUIRE(e.code() == mongocxx::error_code::k_invalid_uri);

            std::string invalid_schema =
                "Invalid URI Schema, expecting 'mongodb://' or 'mongodb+srv://': ";

            REQUIRE(e.what() == invalid_schema + e.code().message());
        }
    }

    SECTION("Multiple hosts") {
        mongocxx::uri uri{"mongodb://host1:123,host2:456"};
        REQUIRE(uri.hosts().size() == 2);
        REQUIRE(uri.hosts()[0].name == "host1");
        REQUIRE(uri.hosts()[0].port == 123);
        REQUIRE(uri.hosts()[1].name == "host2");
        REQUIRE(uri.hosts()[1].port == 456);
    }

    SECTION("Getter is case insensitive") {
        mongocxx::uri uri = mongocxx::uri{"mongodb://host/?cOnNeCtTiMeOuTmS=123"};
        REQUIRE(uri.connect_timeout_ms());
        std::int32_t connect_timeout_ms = *uri.connect_timeout_ms();
        REQUIRE(123 == connect_timeout_ms);
    }
}

template <typename fn>
static void _test_string_option(std::string optname, fn getter_fn) {
    /* Not present. */
    mongocxx::uri uri{"mongodb://host/"};
    REQUIRE(!getter_fn(uri));

    /* Present. */
    uri = mongocxx::uri{"mongodb://host/?" + optname + "=abc"};
    REQUIRE(getter_fn(uri));
    REQUIRE(0 == getter_fn(uri)->compare("abc"));

    /* Present but empty. */
    uri = mongocxx::uri{"mongodb://host/?" + optname + "=&connectTimeoutMS=123"};
    REQUIRE(getter_fn(uri));
    REQUIRE(0 == getter_fn(uri)->compare(""));
}

template <typename fn>
static void _test_bool_option(std::string optname, fn getter_fn) {
    /* Not present. */
    mongocxx::uri uri{"mongodb://host/"};
    REQUIRE(!getter_fn(uri));

    /* Present. */
    uri = mongocxx::uri{"mongodb://host/?" + optname + "=true"};
    REQUIRE(getter_fn(uri));
    REQUIRE(true == *getter_fn(uri));

    /* Present, but false. */
    uri = mongocxx::uri{"mongodb://host/?" + optname + "=false"};
    REQUIRE(getter_fn(uri));
    REQUIRE(false == *getter_fn(uri));

    /* Present but empty. libmongoc considers it unset. */
    uri = mongocxx::uri{"mongodb://host/?" + optname + "="};
    REQUIRE(!getter_fn(uri));

    /* Present, but valid numeric. */
    uri = mongocxx::uri{"mongodb://host/?" + optname + "=1"};
    REQUIRE(getter_fn(uri));
    REQUIRE(true == *getter_fn(uri));

    /* Present, but invalid numeric. */
    REQUIRE_THROWS(mongocxx::uri{"mongodb://host/?" + optname + "=2"});

    /* Present, but wrong type. */
    REQUIRE_THROWS(mongocxx::uri{"mongodb://host/?" + optname + "=lol"});
}

template <typename fn>
static void _test_int32_option(std::string optname,
                               fn getter_fn,
                               std::string valid_value = "1234",
                               bool zero_allowed = true) {
    /* Not present. */
    mongocxx::uri uri{"mongodb://host/"};
    REQUIRE(!getter_fn(uri));

    /* Present. */
    uri = mongocxx::uri{"mongodb://host/?" + optname + "=" + valid_value};
    REQUIRE(getter_fn(uri));
    REQUIRE(stoi(valid_value) == *getter_fn(uri));

    if (zero_allowed) {
        uri = mongocxx::uri{"mongodb://host/?" + optname + "=0"};
        REQUIRE(getter_fn(uri));
        REQUIRE(0 == *getter_fn(uri));
    } else {
        REQUIRE_THROWS(mongocxx::uri{"mongodb://host/?" + optname + "=0"});
    }

    /* Present but empty. libmongoc considers it unset. */
    uri = mongocxx::uri{"mongodb://host/?" + optname + "=&tls=true"};
    REQUIRE(!getter_fn(uri));

    /* Present, but wrong type. Error. */
    REQUIRE_THROWS(mongocxx::uri{"mongodb://host/?" + optname + "=abc"});
}

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

template <typename fn>
static void _test_document_option(std::string optname, fn getter_fn) {
    /* Not present. */
    mongocxx::uri uri{"mongodb://host/"};
    REQUIRE(!getter_fn(uri));

    /* Present. */
    uri = mongocxx::uri{"mongodb://host/?" + optname + "=a:b"};
    REQUIRE(getter_fn(uri));
    REQUIRE(make_document(kvp("a", "b")) == *getter_fn(uri));

    /* Present, multiple fields. */
    uri = mongocxx::uri{"mongodb://host/?" + optname + "=a:b,a2:b2"};
    REQUIRE(getter_fn(uri));
    REQUIRE(make_document(kvp("a", "b"), kvp("a2", "b2")) == *getter_fn(uri));

    /* Present but empty. libmongoc considers it unset. */
    uri = mongocxx::uri{"mongodb://host/?" + optname + "=&connectTimeoutMS=123"};
    REQUIRE(getter_fn(uri));
    REQUIRE(make_document() == *getter_fn(uri));
}

TEST_CASE("uri::appname()", "[uri]") {
    _test_string_option("appname", [](mongocxx::uri& uri) { return uri.appname(); });
}

TEST_CASE("uri::auth_mechanism_properties()", "[uri]") {
    _test_document_option("authMechanismProperties",
                          [](mongocxx::uri& uri) { return uri.auth_mechanism_properties(); });
}

TEST_CASE("uri::connect_timeout_ms()", "[uri]") {
    _test_int32_option("connectTimeoutMS",
                       [](mongocxx::uri& uri) { return uri.connect_timeout_ms(); });
}

TEST_CASE("uri::direct_connection()", "[uri]") {
    _test_bool_option("directConnection",
                      [](mongocxx::uri& uri) { return uri.direct_connection(); });
}

TEST_CASE("uri::local_threshold_ms()", "[uri]") {
    _test_int32_option("localThresholdMS",
                       [](mongocxx::uri& uri) { return uri.local_threshold_ms(); });
}

TEST_CASE("uri::max_pool_size()", "[uri]") {
    _test_int32_option("maxPoolSize", [](mongocxx::uri& uri) { return uri.max_pool_size(); });
}

TEST_CASE("uri::retry_reads()", "[uri]") {
    _test_bool_option("retryReads", [](mongocxx::uri& uri) { return uri.retry_reads(); });
}

TEST_CASE("uri::retry_writes()", "[uri]") {
    _test_bool_option("retryWrites", [](mongocxx::uri& uri) { return uri.retry_writes(); });
}

TEST_CASE("uri::server_selection_timeout_ms()", "[uri]") {
    _test_int32_option("serverSelectionTimeoutMS",
                       [](mongocxx::uri& uri) { return uri.server_selection_timeout_ms(); });
}

TEST_CASE("uri::server_selection_try_once()", "[uri]") {
    _test_bool_option("serverSelectionTryOnce",
                      [](mongocxx::uri& uri) { return uri.server_selection_try_once(); });
}

TEST_CASE("uri::socket_timeout_ms()", "[uri]") {
    _test_int32_option("socketTimeoutMS",
                       [](mongocxx::uri& uri) { return uri.socket_timeout_ms(); });
}

TEST_CASE("uri::tls_allow_invalid_certificates()", "[uri]") {
    _test_bool_option("tlsAllowInvalidCertificates",
                      [](mongocxx::uri& uri) { return uri.tls_allow_invalid_certificates(); });
}

TEST_CASE("uri::tls_allow_invalid_hostnames()", "[uri]") {
    _test_bool_option("tlsAllowInvalidHostnames",
                      [](mongocxx::uri& uri) { return uri.tls_allow_invalid_hostnames(); });
}

TEST_CASE("uri::tls_ca_file()", "[uri]") {
    _test_string_option("tlsCAFile", [](mongocxx::uri& uri) { return uri.tls_ca_file(); });
}

TEST_CASE("uri::tls_certificate_key_file()", "[uri]") {
    _test_string_option("tlsCertificateKeyFile",
                        [](mongocxx::uri& uri) { return uri.tls_certificate_key_file(); });
}

TEST_CASE("uri::tls_certificate_key_file_password()", "[uri]") {
    _test_string_option("tlsCertificateKeyFilePassword",
                        [](mongocxx::uri& uri) { return uri.tls_certificate_key_file_password(); });
}

TEST_CASE("uri::tls_disable_certificate_revocation_check()", "[uri]") {
    _test_bool_option("tlsDisableCertificateRevocationCheck", [](mongocxx::uri& uri) {
        return uri.tls_disable_certificate_revocation_check();
    });
}

TEST_CASE("uri::tls_disable_ocsp_endpoint_check()", "[uri]") {
    _test_bool_option("tlsDisableOCSPEndpointCheck",
                      [](mongocxx::uri& uri) { return uri.tls_disable_ocsp_endpoint_check(); });
}

TEST_CASE("uri::tls_insecure()", "[uri]") {
    _test_bool_option("tlsInsecure", [](mongocxx::uri& uri) { return uri.tls_insecure(); });
}

TEST_CASE("uri::wait_queue_timeout_ms()", "[uri]") {
    _test_int32_option("waitQueueTimeoutMS",
                       [](mongocxx::uri& uri) { return uri.wait_queue_timeout_ms(); });
}

// Begin special cases.

// Compressors is a non-optional list of strings.
TEST_CASE("uri::compressors()", "[uri]") {
    std::string zlib = "zlib", noop = "noop";
    mongocxx::uri uri{"mongodb://host/?compressors=zlib,noop"};
    std::vector<bsoncxx::stdx::string_view> compressors = uri.compressors();
    REQUIRE(compressors.size() == 2);
    REQUIRE(std::find(compressors.begin(), compressors.end(), bsoncxx::stdx::string_view(zlib)) !=
            compressors.end());
    REQUIRE(std::find(compressors.begin(), compressors.end(), bsoncxx::stdx::string_view(noop)) !=
            compressors.end());

    /* Present with some invalid, only returns valid. */
    uri = mongocxx::uri{"mongodb://host/?compressors=zlib,invalid"};
    compressors = uri.compressors();
    REQUIRE(compressors.size() == 1);
    REQUIRE(std::find(compressors.begin(), compressors.end(), bsoncxx::stdx::string_view(zlib)) !=
            compressors.end());

    /* Not present, empty list. */
    uri = mongocxx::uri{"mongodb://host/"};
    compressors = uri.compressors();
    REQUIRE(compressors.size() == 0);

    /* Present but no valid, empty list. */
    uri = mongocxx::uri{"mongodb://host/?compressors=invalid,invalid"};
    compressors = uri.compressors();
    REQUIRE(compressors.size() == 0);

    /* Present but empty, empty list. */
    uri = mongocxx::uri{"mongodb://host/?compressors=&connectTimeousMS=123"};
    compressors = uri.compressors();
    REQUIRE(compressors.size() == 0);
}

// Zero is not allowed for heartbeatFrequencyMS.
TEST_CASE("uri::heartbeat_frequency_ms()", "[uri]") {
    _test_int32_option("heartbeatFrequencyMS",
                       [](mongocxx::uri& uri) { return uri.heartbeat_frequency_ms(); },
                       "1234",
                       false);
}

// -1 to 9 are only valid values of zlib compression level.
TEST_CASE("uri::zlib_compression_level()", "[uri]") {
    _test_int32_option("zlibCompressionLevel",
                       [](mongocxx::uri& uri) { return uri.zlib_compression_level(); },
                       "5",
                       true);
}

// End special cases.

}  // namespace
