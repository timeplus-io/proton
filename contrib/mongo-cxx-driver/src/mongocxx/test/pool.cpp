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

#include <cstddef>
#include <string>

#include <bsoncxx/test/catch.hh>
#include <mongocxx/client.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/tls.hpp>
#include <mongocxx/pool.hpp>
#include <mongocxx/private/libmongoc.hh>
#include <third_party/catch/include/helpers.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace {
using namespace mongocxx;

TEST_CASE("a pool is created with the correct MongoDB URI", "[pool]") {
    MOCK_POOL

    instance::current();

    bool destroy_called = false;
    client_pool_destroy->visit([&](::mongoc_client_pool_t*) { destroy_called = true; });

    std::string expected_uri("mongodb://mongodb.example.com:9999");
    uri mongodb_uri{expected_uri};

    std::string actual_uri{};
    bool new_called = false;

    client_pool_new_with_error->visit([&](const mongoc_uri_t* uri, bson_error_t* error) {
        new_called = true;
        actual_uri = mongoc_uri_get_string(uri);
        error->code = 0;
        return nullptr;
    });

    {
        pool p{mongodb_uri};

        REQUIRE(new_called);
        REQUIRE(expected_uri == actual_uri);

        REQUIRE(!destroy_called);
    }

    REQUIRE(destroy_called);
}

#if defined(MONGOCXX_ENABLE_SSL) && defined(MONGOC_ENABLE_SSL)
TEST_CASE(
    "If we pass an engaged SSL options struct to the pool class, we will use it to configure the "
    "underlying mongoc pool",
    "[pool]") {
    MOCK_POOL

    instance::current();

    const std::string pem_file = "foo";
    const std::string pem_password = "bar";
    const std::string ca_file = "baz";
    const std::string ca_dir = "garply";
    const std::string crl_file = "crl_file";
    const bool allow_invalid_certificates = true;

    bool set_tls_opts_called = false;
    options::tls tls_opts;
    tls_opts.pem_file(pem_file);
    tls_opts.pem_password(pem_password);
    tls_opts.ca_file(ca_file);
    tls_opts.ca_dir(ca_dir);
    tls_opts.crl_file(crl_file);
    tls_opts.allow_invalid_certificates(allow_invalid_certificates);

    ::mongoc_ssl_opt_t interposed = {};

    client_pool_set_ssl_opts->visit([&](::mongoc_client_pool_t*, const ::mongoc_ssl_opt_t* opts) {
        set_tls_opts_called = true;
        interposed = *opts;
    });

    pool p{uri{"mongodb://mongodb.example.com:9999/?tls=true"},
           options::client().tls_opts(tls_opts)};

    REQUIRE(set_tls_opts_called);
    REQUIRE(interposed.pem_file == pem_file);
    REQUIRE(interposed.pem_pwd == pem_password);
    REQUIRE(interposed.ca_file == ca_file);
    REQUIRE(interposed.ca_dir == ca_dir);
    REQUIRE(interposed.crl_file == crl_file);
    REQUIRE(interposed.weak_cert_validation == allow_invalid_certificates);
}
#endif

TEST_CASE("calling acquire on a pool returns an entry that manages its client", "[pool]") {
    MOCK_POOL

    instance::current();

    bool pop_called = false;
    client_pool_pop->visit([&](::mongoc_client_pool_t*) {
        pop_called = true;
        return nullptr;
    });

    bool push_called = false;
    client_pool_push->visit(
        [&](::mongoc_client_pool_t*, ::mongoc_client_t*) { push_called = true; });

    SECTION("entry releases its client at end of scope") {
        {
            pool p{};
            auto client = p.acquire();

            REQUIRE(pop_called);
            REQUIRE(!push_called);
        }

        REQUIRE(push_called);
    }

    SECTION("entry releases its client when set to nullptr") {
        pool p{};
        auto client = p.acquire();

        REQUIRE(pop_called);
        REQUIRE(!push_called);
        client = nullptr;
        REQUIRE(push_called);
    }
}

TEST_CASE("try_acquire returns an engaged stdx::optional<entry>", "[pool]") {
    instance::current();
    pool p{};
    auto client = p.try_acquire();
    REQUIRE(!!client);
}

TEST_CASE(
    "try_acquire returns a disengaged stdx::optional<entry> if mongoc_client_pool_try_pop "
    "returns a null pointer",
    "[pool]") {
    MOCK_POOL

    instance::current();

    client_pool_try_pop->interpose([](::mongoc_client_pool_t*) { return nullptr; });

    {
        pool p{};
        auto client = p.try_acquire();
        REQUIRE(!client);
    }
}

TEST_CASE("a pool is created with an invalid connection string", "[pool]") {
    instance::current();
    std::string uristr = "mongodb+srv://foo.bar.baz";

    REQUIRE_THROWS_AS(pool{mongocxx::uri(uristr)}, operation_exception);
}
}  // namespace
