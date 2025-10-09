// Copyright 2018-present MongoDB Inc.
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

#include <sstream>

#include <bsoncxx/private/helpers.hh>
#include <bsoncxx/stdx/make_unique.hpp>
#include <bsoncxx/test/catch.hh>
#include <mongocxx/client.hpp>
#include <mongocxx/exception/bulk_write_exception.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/exception/server_error_code.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/private/libmongoc.hh>
#include <mongocxx/test/client_helpers.hh>
#include <third_party/catch/include/helpers.hpp>

namespace {
using bsoncxx::from_json;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;
using bsoncxx::document::value;
using bsoncxx::types::b_timestamp;

using namespace mongocxx;
using test_util::server_has_sessions;

TEST_CASE("session options", "[session]") {
    instance::current();

    client c{uri{}, test_util::add_test_server_api()};

    if (!server_has_sessions(c)) {
        return;
    }

    SECTION("default") {
        // Make sure the defaults don't cause a client exception:
        options::client_session opts;

        REQUIRE_NOTHROW(c.start_session(opts));
    }

    SECTION("default-- check values") {
        auto s = c.start_session();

        CHECK(s.options().causal_consistency());
        CHECK_FALSE(s.options().snapshot());
    }

    SECTION("default causal consistency") {
        options::client_session opts;

        // If internally un-set, we "see" the default value of true on read:
        REQUIRE(opts.causal_consistency());
    }

    SECTION("manually set causal consistency") {
        options::client_session opts;

        opts.causal_consistency(false);
        REQUIRE_FALSE(opts.causal_consistency());

        auto s = c.start_session(opts);

        // Setting for causal consistency should now be present, but false:
        REQUIRE_FALSE(s.options().causal_consistency());
    }

    SECTION("manually set snapshot consistency") {
        options::client_session opts;

        REQUIRE_FALSE(opts.snapshot());
        opts.causal_consistency(false);
        opts.snapshot(true);

        REQUIRE(opts.snapshot());
        REQUIRE_FALSE(opts.causal_consistency());

        auto s = c.start_session(opts);

        // Nothing from the preconditions should have changed (because we
        // specified values above; i.e. there have been no side-effects):
        REQUIRE(opts.snapshot());
        REQUIRE_FALSE(opts.causal_consistency());

        // ...and the returned options have been set:
        REQUIRE(s.options().snapshot());
        REQUIRE_FALSE(s.options().causal_consistency());
    }

    SECTION("causal and snapshot consistency are mutually exclusive") {
        // Trying to set both casusal and snapshot consistency should result
        // in an error when starting the session:
        options::client_session opts;

        opts.snapshot(true);
        opts.causal_consistency(true);

        REQUIRE(opts.causal_consistency());
        REQUIRE(opts.snapshot());

        REQUIRE_THROWS_AS(c.start_session(opts), mongocxx::exception);
    }

    SECTION("invalid combinations of causal and snapshot consistency should fail") {
        // Essentially, we can expect any number of combinations of un-set (i.e. does
        // not contain a value), true, or false to be possible; but in the end any result
        // leading to a final combination of [present, true, true] should result in an
        // exception.

        options::client_session opts;

        // Unfortunately, stdx::nullopt doesn't always play well with others, so we'll use
        // an enumeration:
        enum struct optional_state { empty, no, yes };

        auto causal_consistenty_opt = GENERATE(Catch::Generators::as<optional_state>{},
                                               optional_state::empty,
                                               optional_state::no,
                                               optional_state::yes);
        auto snapshot_consistenty_opt = GENERATE(Catch::Generators::as<optional_state>{},
                                                 optional_state::empty,
                                                 optional_state::no,
                                                 optional_state::yes);

        // Only actually set a value if we generate a non-empty setting:
        if (optional_state::empty != causal_consistenty_opt)
            opts.causal_consistency(optional_state::yes == causal_consistenty_opt);

        if (optional_state::empty != snapshot_consistenty_opt)
            opts.snapshot(optional_state::yes == snapshot_consistenty_opt);

        // We should always expect an error if both are enabled:
        if (optional_state::yes == causal_consistenty_opt &&
            optional_state::yes == snapshot_consistenty_opt) {
            REQUIRE_THROWS_AS(c.start_session(opts), mongocxx::exception);
        } else {
            CHECK_NOTHROW(c.start_session(opts));
        }
    }
}

TEST_CASE("start_session failure", "[session]") {
    using namespace mongocxx::test_util;

    MOCK_CLIENT

    instance::current();

    client_start_session
        ->interpose([](mongoc_client_t*, const mongoc_session_opt_t*, bson_error_t* error) {
            bson_set_error(error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_SESSION_FAILURE, "foo");
            return (mongoc_client_session_t*)nullptr;
        })
        .forever();

    client c{uri{}, test_util::add_test_server_api()};

    REQUIRE_THROWS_MATCHES(
        c.start_session(), mongocxx::exception, mongocxx_exception_matcher{"foo"});
}

TEST_CASE("session", "[session]") {
    using namespace mongocxx::test_util;

    instance::current();

    client c{uri{}, test_util::add_test_server_api()};

    if (!server_has_sessions(c)) {
        return;
    }

    auto s = c.start_session();

    SECTION("id") {
        REQUIRE(!s.id().empty());
    }

    SECTION("cluster time and operation time") {
        b_timestamp zero, nonzero;
        zero.timestamp = 0;
        zero.increment = 0;
        nonzero.timestamp = 1;
        nonzero.increment = 0;

        // The session hasn't been used for anything yet.
        REQUIRE(s.cluster_time().empty());
        REQUIRE(s.operation_time() == zero);

        // Advance the cluster time - just a basic test, rely on libmongoc's logic.
        s.advance_cluster_time(
            from_json("{\"clusterTime\": {\"$timestamp\": {\"t\": 1, \"i\": 0}}}"));
        REQUIRE(!s.cluster_time().empty());
        REQUIRE(s.operation_time() == zero);

        // Advance the operation time, just a basic test again.
        s.advance_operation_time(nonzero);
        REQUIRE(s.operation_time() == nonzero);
    }

    SECTION("pool") {
        // "Pool is LIFO" test from Driver Sessions Spec.
        auto session_a = stdx::make_unique<client_session>(c.start_session());
        auto session_b = stdx::make_unique<client_session>(c.start_session());
        auto a_id = value(session_a->id());
        auto b_id = value(session_b->id());

        c["db"].run_command(*session_a, make_document(kvp("ping", 1)));
        c["db"].run_command(*session_b, make_document(kvp("ping", 1)));

        // End session A, then session B.
        session_a = nullptr;
        session_b = nullptr;

        auto session_c = stdx::make_unique<client_session>(c.start_session());
        REQUIRE(session_c->id() == b_id);
        auto session_d = stdx::make_unique<client_session>(c.start_session());
        REQUIRE(session_d->id() == a_id);
    }

    SECTION("wrong client") {
        using Catch::Matchers::Contains;

        // "Session argument is for the right client" test from Driver Sessions Spec.
        // Passing a session from client "c" should fail with client "c2" and related objects.
        client c2{uri{}, test_util::add_test_server_api()};
        auto db2 = c2["db"];
        auto collection2 = db2["collection"];

#define REQUIRE_THROWS_INVALID_SESSION(_expr) \
    REQUIRE_THROWS_MATCHES(                   \
        (_expr), mongocxx::exception, mongocxx_exception_matcher{"Invalid sessionId"})

        REQUIRE_THROWS_INVALID_SESSION(collection2.count_documents(s, {}));
        REQUIRE_THROWS_INVALID_SESSION(collection2.create_index(s, make_document(kvp("a", 1))));
        REQUIRE_THROWS_INVALID_SESSION(collection2.distinct(s, "a", {}));
        REQUIRE_THROWS_INVALID_SESSION(collection2.drop(s));
        REQUIRE_THROWS_INVALID_SESSION(collection2.find_one(s, {}));
        REQUIRE_THROWS_INVALID_SESSION(collection2.rename(s, "foo", true));
        REQUIRE_THROWS_INVALID_SESSION(db2.create_collection(s, "foo"));
        REQUIRE_THROWS_INVALID_SESSION(db2.run_command(s, {}));

        // Test iterators.
        auto cursor = c2.list_databases(s);
        REQUIRE_THROWS_INVALID_SESSION(cursor.begin());
        cursor = collection2.aggregate(s, {});
        REQUIRE_THROWS_INVALID_SESSION(cursor.begin());
        cursor = collection2.list_indexes(s);
        REQUIRE_THROWS_INVALID_SESSION(cursor.begin());
        cursor = db2.list_collections(s);
        REQUIRE_THROWS_INVALID_SESSION(cursor.begin());
        cursor = collection2.find(s, {});
        REQUIRE_THROWS_INVALID_SESSION(cursor.begin());
        auto stream = collection2.watch(s);
        REQUIRE_THROWS_INVALID_SESSION(stream.begin());
        auto indexes2 = collection2.indexes();
        cursor = indexes2.list(s);
        REQUIRE_THROWS_INVALID_SESSION(cursor.begin());

        // Test CRUD member functions.
        std::vector<model::write> writes;
        std::vector<bsoncxx::document::view> docs;
        auto bulk = collection2.create_bulk_write(s);

        REQUIRE_THROWS_INVALID_SESSION(bulk.execute());
        REQUIRE_THROWS_INVALID_SESSION(collection2.bulk_write(s, writes));
        REQUIRE_THROWS_INVALID_SESSION(collection2.delete_many(s, {}));
        REQUIRE_THROWS_INVALID_SESSION(collection2.delete_one(s, {}));
        REQUIRE_THROWS_INVALID_SESSION(collection2.find_one_and_delete(s, {}));
        REQUIRE_THROWS_INVALID_SESSION(collection2.find_one_and_replace(s, {}, {}));
        REQUIRE_THROWS_INVALID_SESSION(collection2.find_one_and_update(s, {}, {}));
        REQUIRE_THROWS_INVALID_SESSION(collection2.insert_many(s, docs));
        REQUIRE_THROWS_INVALID_SESSION(collection2.insert_one(s, {}));
        REQUIRE_THROWS_INVALID_SESSION(collection2.replace_one(s, {}, {}));
        REQUIRE_THROWS_INVALID_SESSION(collection2.update_many(s, {}, {}));
        REQUIRE_THROWS_INVALID_SESSION(collection2.update_one(s, {}, {}));
        REQUIRE_THROWS_INVALID_SESSION(collection2.write(s, model::insert_one{{}}));

        // Test index_view member functions.
        std::vector<index_model> models;

        REQUIRE_THROWS_INVALID_SESSION(indexes2.create_one(s, make_document(kvp("a", 1))));
        REQUIRE_THROWS_INVALID_SESSION(indexes2.create_many(s, models));
        REQUIRE_THROWS_INVALID_SESSION(indexes2.drop_one(s, "foo"));
        REQUIRE_THROWS_INVALID_SESSION(indexes2.drop_all(s));

        // Test gridfs::bucket.
        auto bucket2 = db2.gridfs_bucket();
        auto one = bsoncxx::types::bson_value::view{bsoncxx::types::b_int32{1}};

        REQUIRE_THROWS_INVALID_SESSION(bucket2.open_upload_stream(s, "file"));
        REQUIRE_THROWS_INVALID_SESSION(bucket2.open_download_stream(s, one));

#undef REQUIRE_THROWS_INVALID_SESSION
    }
}

class session_test {
   public:
    session_test() {
        options::client client_opts;
        options::apm apm_opts;

        apm_opts.on_command_started([&](const events::command_started_event& event) {
            std::string command_name{event.command_name().data()};

            // Ignore auth commands like "saslStart", and handshakes with "hello" (and the legacy
            // "hello" command).
            std::string sasl{"sasl"};
            if (event.command_name().substr(0, sasl.size()).compare(sasl) == 0 ||
                command_name.compare("hello") == 0 || command_name.compare("isMaster") == 0) {
                return;
            }

            events.emplace_back(command_name, bsoncxx::document::value(event.command()));
        });

        client_opts.apm_opts(apm_opts);
        client = mongocxx::client(uri{}, test_util::add_test_server_api(client_opts));
    }

    void test_method_with_session(const std::function<void(bool)>& f, const client_session& s) {
        using std::string;

        events.clear();

        // A method with an explicit session must send its logical session id or
        // "lsid".
        f(true);
        if (events.size() == 0) {
            throw std::logic_error{"no events after calling command with explicit session"};
        }

        for (auto& event : events) {
            if (!event.command["lsid"]) {
                throw std::logic_error{"no lsid in " + event.command_name +
                                       " with explicit session"};
            }
            if (event.command["lsid"].get_document().view() != s.id()) {
                throw std::logic_error{"wrong lsid in " + event.command_name +
                                       " with explicit session"};
            }
        }

        events.clear();

        // A method called with no session must send an implicit session id with the
        // command.
        f(false);
        if (events.size() == 0) {
            throw std::logic_error{"no events after calling command with implicit session"};
        }

        for (auto& event : events) {
            if (!event.command["lsid"]) {
                throw std::logic_error{"no lsid in " + event.command_name +
                                       " with implicit session"};
            }
            if (event.command["lsid"].get_document().view() == s.id()) {
                throw std::logic_error{"wrong lsid in " + event.command_name +
                                       " with implicit session"};
            }
        }
    }

    class apm_event {
       public:
        apm_event(const std::string& command_name_, const bsoncxx::document::value& document_)
            : command_name(command_name_), value(document_), command(value.view()) {}

        std::string command_name;
        bsoncxx::document::value value;
        bsoncxx::document::view command;
    };

    std::vector<apm_event> events;
    mongocxx::client client;
};

TEST_CASE("lsid", "[session]") {
    instance::current();

    session_test test;

    if (!server_has_sessions(test.client)) {
        return;
    }

    auto s = test.client.start_session();
    auto db = test.client["lsid"];
    auto bucket = db.gridfs_bucket();
    auto collection = db["collection"];
    auto indexes = collection.indexes();
    collection.drop();

    // Ensure some data for aggregate() and find().
    for (int i = 0; i != 10; ++i) {
        collection.insert_one({});
    }

    SECTION("bucket") {
        auto f = [&s, &bucket, &db](bool use_session) {
            // Start clean.
            use_session ? db["fs.files"].drop(s) : db["fs.files"].drop();
            use_session ? db["fs.chunks"].drop(s) : db["fs.chunks"].drop();

            auto one = bsoncxx::types::bson_value::view{bsoncxx::types::b_int32{1}};
            auto two = bsoncxx::types::bson_value::view{bsoncxx::types::b_int32{2}};
            auto data = (uint8_t*)"foo";
            size_t len = 4;
            // Ensure multiple chunks.
            options::gridfs::upload opts;
            opts.chunk_size_bytes(1);

            auto up = use_session ? bucket.open_upload_stream(s, "file", opts)
                                  : bucket.open_upload_stream("file", opts);
            up.write(data, len);
            up.close();

            up = use_session ? bucket.open_upload_stream_with_id(s, one, "file", opts)
                             : bucket.open_upload_stream_with_id(one, "file", opts);
            up.write(data, len);
            up.close();

            up = use_session ? bucket.open_upload_stream_with_id(s, one, "file", opts)
                             : bucket.open_upload_stream_with_id(one, "file", opts);
            up.write(data, len);
            up.abort();

            std::stringstream stream("foo");
            use_session ? bucket.upload_from_stream(s, "file", &stream, opts)
                        : bucket.upload_from_stream("file", &stream, opts);
            use_session ? bucket.upload_from_stream_with_id(s, two, "file", &stream, opts)
                        : bucket.upload_from_stream_with_id(two, "file", &stream, opts);

            auto down = use_session ? bucket.open_download_stream(s, two)
                                    : bucket.open_download_stream(two);
            use_session ? bucket.download_to_stream(s, two, &stream)
                        : bucket.download_to_stream(two, &stream);

            auto cursor = use_session ? bucket.find(s, {}) : bucket.find({});
            auto total = std::distance(cursor.begin(), cursor.end());

            REQUIRE(total == 4);
            use_session ? bucket.delete_file(s, one) : bucket.delete_file(one);
        };

        test.test_method_with_session(f, s);
    }

    SECTION("client::list_databases") {
        auto f = [&s, &test](bool use_session) {
            auto cursor =
                use_session ? test.client.list_databases(s) : test.client.list_databases();
            auto total = std::distance(cursor.begin(), cursor.end());

            REQUIRE(total > 0);
        };

        test.test_method_with_session(f, s);
    }

    SECTION("collection::aggregate") {
        auto f = [&s, &collection](bool use_session) {
            options::aggregate opts;
            opts.batch_size(2);
            auto cursor =
                use_session ? collection.aggregate(s, {}, opts) : collection.aggregate({}, opts);
            auto total = std::distance(cursor.begin(), cursor.end());

            REQUIRE(total == 10);
        };

        test.test_method_with_session(f, s);
        REQUIRE(test.events.size() >= 2);
    }

    SECTION("collection::bulk_write") {
        std::vector<model::write> vec;
        vec.emplace_back(model::insert_one{{}});

        auto bulk_write_vector = [&s, &collection, &vec](bool use_session) {
            use_session ? collection.bulk_write(s, vec) : collection.bulk_write(vec);
        };

        test.test_method_with_session(bulk_write_vector, s);

        auto bulk_write_iterator = [&s, &collection, &vec](bool use_session) {
            use_session ? collection.bulk_write(s, vec.begin(), vec.end())
                        : collection.bulk_write(vec.begin(), vec.end());
        };

        test.test_method_with_session(bulk_write_iterator, s);
    }

    SECTION("collection::count") {
        auto f = [&s, &collection](bool use_session) {
            auto total =
                use_session ? collection.count_documents(s, {}) : collection.count_documents({});
            REQUIRE(total == 10);
        };

        test.test_method_with_session(f, s);
    }

    SECTION("collection::create_bulk_write") {
        auto f = [&s, &collection](bool use_session) {
            auto bulk =
                use_session ? collection.create_bulk_write(s) : collection.create_bulk_write();

            bulk.append(model::insert_one{{}});
            bulk.execute();
        };

        test.test_method_with_session(f, s);
    }

    SECTION("collection::create_index") {
        auto f = [&s, &collection](bool use_session) {
            if (use_session) {
                collection.drop(s);
                collection.create_index(s, make_document(kvp("a", 1)));
            } else {
                collection.drop();
                collection.create_index(make_document(kvp("a", 1)));
            }
        };

        test.test_method_with_session(f, s);
    }

    SECTION("collection::delete_many") {
        auto f = [&s, &collection](bool use_session) {
            use_session ? collection.delete_many(s, {}) : collection.delete_many({});
        };

        test.test_method_with_session(f, s);
    }

    SECTION("collection::delete_one") {
        auto f = [&s, &collection](bool use_session) {
            use_session ? collection.delete_one(s, {}) : collection.delete_one({});
        };

        test.test_method_with_session(f, s);
    }

    SECTION("collection::distinct") {
        auto f = [&s, &collection](bool use_session) {
            use_session ? collection.distinct(s, "a", {}) : collection.distinct("a", {});
        };

        test.test_method_with_session(f, s);
    }

    SECTION("collection::find") {
        auto f = [&s, &collection](bool use_session) {
            options::find opts;
            opts.batch_size(2);
            auto cursor = use_session ? collection.find(s, {}, opts) : collection.find({}, opts);

            auto total = std::distance(cursor.begin(), cursor.end());
            REQUIRE(total == 10);
        };

        test.test_method_with_session(f, s);
        // A find and a getMore.
        REQUIRE(test.events.size() >= 2);
    }

    SECTION("collection::find_one") {
        auto f = [&s, &collection](bool use_session) {
            use_session ? collection.find_one(s, {}) : collection.find_one({});
        };

        test.test_method_with_session(f, s);
    }

    SECTION("collection::find_one_and_delete") {
        auto f = [&s, &collection](bool use_session) {
            use_session ? collection.find_one_and_delete(s, {})
                        : collection.find_one_and_delete({});
        };

        test.test_method_with_session(f, s);
    }

    SECTION("collection::find_one_and_replace") {
        auto f = [&s, &collection](bool use_session) {
            use_session ? collection.find_one_and_replace(s, {}, {})
                        : collection.find_one_and_replace({}, {});
        };

        test.test_method_with_session(f, s);
    }

    SECTION("collection::find_one_and_update") {
        auto f = [&s, &collection](bool use_session) {
            use_session ? collection.find_one_and_update(s, {}, {})
                        : collection.find_one_and_update({}, {});
        };

        test.test_method_with_session(f, s);
    }

    SECTION("collection::insert_many") {
        bsoncxx::document::value doc({});
        std::vector<bsoncxx::document::view> docs{doc.view()};

        auto insert_vector = [&s, &collection, &docs](bool use_session) {
            use_session ? collection.insert_many(s, docs) : collection.insert_many(docs);
        };

        test.test_method_with_session(insert_vector, s);

        auto insert_iter = [&s, &collection, &docs](bool use_session) {
            use_session ? collection.insert_many(s, docs.begin(), docs.end())
                        : collection.insert_many(docs.begin(), docs.end());
        };

        test.test_method_with_session(insert_iter, s);
    }

    SECTION("collection::insert_one") {
        auto f = [&s, &collection](bool use_session) {
            use_session ? collection.insert_one(s, {}) : collection.insert_one({});
        };

        test.test_method_with_session(f, s);
    }

    SECTION("collection::list_indexes") {
        auto f = [&s, &collection](bool use_session) {
            use_session ? collection.list_indexes(s) : collection.list_indexes();
        };

        test.test_method_with_session(f, s);
    }

    SECTION("collection::rename") {
        auto f = [&s, &collection](bool use_session) {
            if (use_session) {
                collection.insert_one(s, {});
                auto c2 = collection;
                c2.rename(s, "collection2", true);
            } else {
                collection.insert_one({});
                auto c2 = collection;
                c2.rename("collection2", true);
            }
        };

        test.test_method_with_session(f, s);
    }

    SECTION("collection::replace_one") {
        auto f = [&s, &collection](bool use_session) {
            use_session ? collection.replace_one(s, {}, {}) : collection.replace_one({}, {});
        };

        test.test_method_with_session(f, s);
    }

    SECTION("collection::update_many") {
        auto f = [&s, &collection](bool use_session) {
            auto u = make_document(kvp("$set", make_document(kvp("a", 1))));
            use_session ? collection.update_many(s, {}, u.view())
                        : collection.update_many({}, u.view());
        };

        test.test_method_with_session(f, s);
    }

    SECTION("collection::update_one") {
        auto f = [&s, &collection](bool use_session) {
            auto u = make_document(kvp("$set", make_document(kvp("a", 1))));
            use_session ? collection.update_one(s, {}, u.view())
                        : collection.update_one({}, u.view());
        };

        test.test_method_with_session(f, s);
    }

    SECTION("collection::watch") {
        if (!test_util::is_replica_set(test.client)) {
            WARN("skip: watch() requires replica set");
            return;
        }

        auto f = [&s, &collection](bool use_session) {
            options::change_stream opts;
            opts.max_await_time(std::chrono::milliseconds(1));
            auto stream = use_session ? collection.watch(s, opts) : collection.watch(opts);
            for (auto it = stream.begin(); it != stream.end(); it++)
                ;
        };

        test.test_method_with_session(f, s);
    }

    SECTION("collection::write") {
        auto f = [&s, &collection](bool use_session) {
            auto insert_op = model::insert_one{{}};
            use_session ? collection.write(s, insert_op) : collection.write(insert_op);
        };

        test.test_method_with_session(f, s);
    }

    SECTION("create and drop collection") {
        auto f = [&s, &db](bool use_session) {
            use_session ? db.create_collection(s, "foo") : db.create_collection("foo");
            use_session ? db["foo"].drop(s) : db["foo"].drop();
        };

        test.test_method_with_session(f, s);
    }

    SECTION("database::drop") {
        auto f = [&s, &db](bool use_session) { use_session ? db.drop(s) : db.drop(); };

        test.test_method_with_session(f, s);
    }

    SECTION("database::list_collections") {
        auto f = [&s, &db](bool use_session) {
            auto cursor = use_session ? db.list_collections(s) : db.list_collections();
            for (auto it = cursor.begin(); it != cursor.end(); it++)
                ;
        };

        test.test_method_with_session(f, s);
    }

    SECTION("database::list_collection_names") {
        auto f = [&s, &db](bool use_session) {
            auto vector = use_session ? db.list_collection_names(s) : db.list_collection_names();
            for (auto it = vector.begin(); it != vector.end(); it++)
                ;
        };

        test.test_method_with_session(f, s);
    }

    SECTION("database::run_command") {
        auto f = [&s, &db](bool use_session) {
            auto cmd = make_document(kvp("ping", 1));
            use_session ? db.run_command(s, cmd.view()) : db.run_command(cmd.view());
        };

        test.test_method_with_session(f, s);
    }

    SECTION("index_view::list") {
        auto f = [&s, &indexes](bool use_session) {
            auto cursor = use_session ? indexes.list(s) : indexes.list();
            for (auto it = cursor.begin(); it != cursor.end(); it++)
                ;
        };

        test.test_method_with_session(f, s);
    }

    SECTION("index_view::create_one") {
        auto create_with_keys = [&s, &indexes](bool use_session) {
            auto keys = make_document(kvp("a", 1));
            use_session ? indexes.create_one(s, keys.view()) : indexes.create_one(keys.view());
        };

        test.test_method_with_session(create_with_keys, s);

        auto create_with_model = [&s, &indexes](bool use_session) {
            auto model = index_model{make_document(kvp("a", 1))};
            use_session ? indexes.create_one(s, model) : indexes.create_one(model);
        };

        test.test_method_with_session(create_with_model, s);
    }

    SECTION("index_view::create_many") {
        auto f = [&s, &indexes](bool use_session) {
            auto models = std::vector<index_model>{index_model{make_document(kvp("a", 1))},
                                                   index_model{make_document(kvp("b", 1))}};

            use_session ? indexes.create_many(s, models) : indexes.create_many(models);
        };

        test.test_method_with_session(f, s);
    }

    SECTION("index_view::drop_one") {
        auto drop_by_name = [&s, &indexes](bool use_session) {
            auto keys = make_document(kvp("a", 1));
            auto name =
                use_session ? indexes.create_one(s, keys.view()) : indexes.create_one(keys.view());
            use_session ? indexes.drop_one(s, name.value()) : indexes.drop_one(name.value());
        };

        test.test_method_with_session(drop_by_name, s);

        auto drop_by_keys = [&s, &indexes](bool use_session) {
            auto keys = make_document(kvp("a", 1));
            use_session ? indexes.create_one(s, keys.view()) : indexes.create_one(keys.view());
            use_session ? indexes.drop_one(s, keys.view()) : indexes.drop_one(keys.view());
        };

        test.test_method_with_session(drop_by_keys, s);

        auto drop_by_model = [&s, &indexes](bool use_session) {
            auto keys = make_document(kvp("a", 1));
            use_session ? indexes.create_one(s, keys.view()) : indexes.create_one(keys.view());
            auto model = index_model{keys.view()};
            use_session ? indexes.drop_one(s, model) : indexes.drop_one(model);
        };

        test.test_method_with_session(drop_by_model, s);
    }

    SECTION("index_view::drop_all") {
        auto f = [&s, &indexes](bool use_session) {
            use_session ? indexes.drop_all(s) : indexes.drop_all();
        };

        test.test_method_with_session(f, s);
    }
}

TEST_CASE("with_transaction", "[session]") {
    using namespace mongocxx::test_util;

    instance::current();

    session_test test;

    if (!server_has_sessions(test.client)) {
        return;
    }

    auto session = test.client.start_session();

    // The following three tests are prose tests from the with_transaction spec.
    SECTION("prose tests for with_transaction") {
        SECTION("callback raises a custom error") {
            // Multi-document transactions require server 4.2+.
            if (compare_versions(get_server_version(test.client), "4.2") < 0) {
                WARN("Skipping - MongoDB server 4.2 or newer required");
                return;
            }

            // Test an operation_exception
            REQUIRE_THROWS_MATCHES(session.with_transaction([](client_session*) {
                throw operation_exception{{}, "The meaning of life"};
            }),
                                   operation_exception,
                                   mongocxx_exception_matcher{"The meaning of life"});

            // Test a different exception
            REQUIRE_THROWS_AS(session.with_transaction(
                                  [](client_session*) { throw std::logic_error{"it's 42"}; }),
                              std::logic_error);
        }

        SECTION("callback returns a value") {
            // The C++ driver does not support returning values from the callback.
        }

        SECTION("retry timeout is enforced") {
            // We let the libmongoc tests cover timeout tests. The c driver does not
            // expose the mock timeout mechanism that allows this to be tested without
            // performing long sleeps.
        }
    }  // End prose tests.
}

TEST_CASE("unacknowledged write in session", "[session]") {
    using namespace mongocxx::test_util;

    instance::current();

    session_test test;

    if (!server_has_sessions(test.client)) {
        return;
    }

    auto s = test.client.start_session();
    auto db = test.client["lsid"];
    auto collection = db["collection"];
    auto noack = write_concern{};
    noack.acknowledge_level(write_concern::level::k_unacknowledged);

    SECTION("insert_one") {
        options::insert insert;
        insert.write_concern(noack);
        REQUIRE_THROWS_MATCHES(
            collection.insert_one(s, {}, insert),
            mongocxx::exception,
            mongocxx_exception_matcher{"Cannot use client session with unacknowledged writes"});
    }
}
}  // namespace
