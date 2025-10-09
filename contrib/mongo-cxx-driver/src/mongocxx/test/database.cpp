// Copyright 2014-present MongoDB Inc.
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

#include <set>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <bsoncxx/test/catch.hh>
#include <mongocxx/client.hpp>
#include <mongocxx/database.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/index_view.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/index_view.hpp>
#include <mongocxx/private/conversions.hh>
#include <mongocxx/private/libbson.hh>
#include <mongocxx/private/libmongoc.hh>
#include <mongocxx/test/client_helpers.hh>
#include <third_party/catch/include/helpers.hpp>

namespace {
using namespace mongocxx;

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_array;
using bsoncxx::builder::basic::make_document;
using test_util::server_has_sessions;

bool check_for_collections(cursor cursor, std::set<std::string> expected_colls) {
    for (auto&& coll : cursor) {
        // Skip system collections which the MMAPv1 storage engine returns,
        // while WiredTiger does not.
        auto name_string = bsoncxx::string::to_string(coll["name"].get_string().value);
        auto pos = name_string.find("system.");
        if (pos != std::string::npos && pos == 0) {
            continue;
        }

        auto iter = expected_colls.find(name_string);
        if (iter == expected_colls.end()) {
            return false;
        }
        expected_colls.erase(iter);
    }
    return expected_colls.empty();
}

TEST_CASE("A default constructed database is false-ish", "[database]") {
    instance::current();

    database d;
    REQUIRE(!d);
}

TEST_CASE("A default constructed database cannot perform operations", "[database]") {
    instance::current();

    database d;
    REQUIRE_THROWS_AS(d.name(), mongocxx::logic_error);
}

TEST_CASE("mongocxx::database copy constructor", "[database]") {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};

    SECTION("constructing from valid") {
        database database_a = client["a"];
        database database_b{database_a};
        REQUIRE(database_b);
        REQUIRE(database_b.name() == stdx::string_view{"a"});
    }

    SECTION("constructing from invalid") {
        database database_a;
        database database_b{database_a};
        REQUIRE(!database_b);
    }
}

TEST_CASE("mongocxx::database copy assignment operator", "[database]") {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};

    SECTION("assigning valid to valid") {
        database database_a = client["a1"];
        database database_b = client["b1"];
        database_b = database_a;
        REQUIRE(database_b);
        REQUIRE(database_b.name() == stdx::string_view{"a1"});
    }

    SECTION("assigning invalid to valid") {
        database database_a;
        database database_b = client["b2"];
        database_b = database_a;
        REQUIRE(!database_b);
    }

    SECTION("assigning valid to invalid") {
        database database_a = client["a3"];
        database database_b;
        database_b = database_a;
        REQUIRE(database_b);
        REQUIRE(database_b.name() == stdx::string_view{"a3"});
    }

    SECTION("assigning invalid to invalid") {
        database database_a;
        database database_b;
        database_b = database_a;
        REQUIRE(!database_b);
    }
}

TEST_CASE("A database", "[database]") {
    stdx::string_view database_name{"database"};
    MOCK_CLIENT
    MOCK_DATABASE

    instance::current();

    client mongo_client{uri{}, test_util::add_test_server_api()};

    SECTION("is created by a client") {
        bool called = false;
        get_database->interpose([&](mongoc_client_t*, const char* d_name) {
            called = true;
            REQUIRE(database_name == stdx::string_view{d_name});
            return nullptr;
        });

        database obtained_database = mongo_client[database_name];
        REQUIRE(obtained_database);
        REQUIRE(called);
        REQUIRE(obtained_database.name() == database_name);
    }

    SECTION("cleans up its underlying mongoc database on destruction") {
        bool destroy_called = false;

        database_destroy->interpose([&](mongoc_database_t*) { destroy_called = true; });

        {
            database database = mongo_client["database"];
            REQUIRE(!destroy_called);
        }

        REQUIRE(destroy_called);
    }

    SECTION("throws an exception when has_collection causes an error") {
        database_has_collection->interpose(
            [](mongoc_database_t*, const char*, bson_error_t* error) {
                bson_set_error(error,
                               MONGOC_ERROR_COMMAND,
                               MONGOC_ERROR_COMMAND_INVALID_ARG,
                               "expected error from mock");
                return false;
            });
        database database = mongo_client["database"];
        REQUIRE_THROWS(database.has_collection("some_collection"));
    }

    SECTION("supports move operations") {
        bool destroy_called = false;
        database_destroy->interpose([&](mongoc_database_t*) { destroy_called = true; });

        {
            client mongo_client{uri{}, test_util::add_test_server_api()};
            database a = mongo_client[database_name];

            database b{std::move(a)};
            REQUIRE(!destroy_called);

            database c = std::move(b);
            REQUIRE(!destroy_called);
        }
        REQUIRE(destroy_called);
    }

    SECTION("Read Concern", "[database]") {
        database mongo_database(mongo_client["database"]);

        auto database_set_rc_called = false;
        read_concern rc{};
        rc.acknowledge_level(read_concern::level::k_majority);

        database_set_read_concern->interpose(
            [&database_set_rc_called](::mongoc_database_t*, const ::mongoc_read_concern_t* rc_t) {
                REQUIRE(rc_t);
                const auto result = libmongoc::read_concern_get_level(rc_t);
                REQUIRE(result);
                REQUIRE(strcmp(result, "majority") == 0);
                database_set_rc_called = true;
            });

        mongo_database.read_concern(rc);
        REQUIRE(database_set_rc_called);
    }

    SECTION("has a read preferences which may be set and obtained") {
        bool destroy_called = false;
        database_destroy->interpose([&](mongoc_database_t*) { destroy_called = true; });

        database mongo_database(mongo_client["database"]);
        read_preference preference{};
        preference.mode(read_preference::read_mode::k_secondary_preferred);

        auto deleter = [](mongoc_read_prefs_t* var) { mongoc_read_prefs_destroy(var); };
        std::unique_ptr<mongoc_read_prefs_t, decltype(deleter)> saved_preference(nullptr, deleter);

        bool called = false;
        database_set_preference->interpose(
            [&](mongoc_database_t*, const mongoc_read_prefs_t* read_prefs) {
                called = true;
                saved_preference.reset(mongoc_read_prefs_copy(read_prefs));
                REQUIRE(mongoc_read_prefs_get_mode(read_prefs) ==
                        libmongoc::conversions::read_mode_t_from_read_mode(
                            read_preference::read_mode::k_secondary_preferred));
            });

        database_get_preference
            ->interpose([&](const mongoc_database_t*) { return saved_preference.get(); })
            .forever();

        mongo_database.read_preference(std::move(preference));
        REQUIRE(called);

        REQUIRE(read_preference::read_mode::k_secondary_preferred ==
                mongo_database.read_preference().mode());
    }

    SECTION("has a write concern which may be set and obtained") {
        bool destroy_called = false;
        database_destroy->interpose([&](mongoc_database_t*) { destroy_called = true; });

        database mongo_database(mongo_client[database_name]);
        write_concern concern;
        concern.majority(std::chrono::milliseconds(100));

        mongoc_write_concern_t* underlying_wc;

        bool set_called = false;
        database_set_concern->interpose(
            [&](mongoc_database_t*, const mongoc_write_concern_t* concern) {
                set_called = true;
                underlying_wc = mongoc_write_concern_copy(concern);
            });

        bool get_called = false;
        database_get_concern->interpose([&](const mongoc_database_t*) {
            get_called = true;
            return underlying_wc;
        });

        mongo_database.write_concern(concern);
        REQUIRE(set_called);

        MOCK_CONCERN
        bool copy_called = false;
        concern_copy->interpose([&](const mongoc_write_concern_t*) {
            copy_called = true;
            return mongoc_write_concern_copy(underlying_wc);
        });

        REQUIRE(concern.majority() == mongo_database.write_concern().majority());

        REQUIRE(get_called);
        REQUIRE(copy_called);

        libmongoc::write_concern_destroy(underlying_wc);
    }

    SECTION("may create a collection") {
        MOCK_COLLECTION
        stdx::string_view collection_name{"dummy_collection"};
        database database = mongo_client[database_name];
        collection obtained_collection = database[collection_name];
        REQUIRE(obtained_collection.name() == collection_name);
    }

    SECTION("supports run_command") {
        bool called = false;

        bsoncxx::document::value doc = make_document(kvp("foo", 5));
        libbson::scoped_bson_t bson_doc{doc.view()};

        database_command_with_opts->interpose([&](mongoc_database_t*,
                                                  const bson_t*,
                                                  const mongoc_read_prefs_t*,
                                                  const bson_t*,
                                                  bson_t* reply,
                                                  bson_error_t*) {
            called = true;
            ::bson_copy_to(bson_doc.bson(), reply);
            return true;
        });

        database database = mongo_client[database_name];
        bsoncxx::document::value command = make_document(kvp("command", 1));
        auto response = database.run_command(command.view());
        REQUIRE(called);
        REQUIRE(response.view()["foo"].get_int32() == 5);
    }
}

TEST_CASE("Database integration tests", "[database]") {
    instance::current();

    client mongo_client{uri{}, test_util::add_test_server_api()};
    stdx::string_view database_name{"database"};
    database database = mongo_client[database_name];

    auto case_insensitive_collation = make_document(kvp("locale", "en_US"), kvp("strength", 2));

    SECTION("aggregation", "[database]") {
        pipeline pipeline;

        auto get_results = [](cursor&& cursor) {
            std::vector<bsoncxx::document::value> results;
            std::transform(cursor.begin(),
                           cursor.end(),
                           std::back_inserter(results),
                           [](bsoncxx::document::view v) { return bsoncxx::document::value{v}; });
            return results;
        };

        SECTION("listLocalSessions") {
            if (!server_has_sessions(mongo_client)) {
                return;
            }

            // SERVER-79306: Ensure the database exists for consistent behavior with sharded
            // clusters.
            database.create_collection("dummy");

            auto session1 = mongo_client.start_session();

            pipeline.list_local_sessions({});
            pipeline.limit(1);
            pipeline.add_fields(make_document(kvp("name", "Jane")));
            pipeline.project(make_document(kvp("name", 1)));

            auto cursor = database.aggregate(pipeline);
            auto results = get_results(std::move(cursor));

            REQUIRE(results.size() == 1);
            REQUIRE(results[0].view()["name"].get_string().value == stdx::string_view("Jane"));
        }
    }

    SECTION("A database may create a collection via create_collection") {
        stdx::string_view collection_name{"collection_create_with_opts"};

        SECTION("without any options") {
            database[collection_name].drop();
            collection obtained_collection = database.create_collection(collection_name);
            REQUIRE(obtained_collection.name() == collection_name);
        }

        SECTION("with options") {
            database[collection_name].drop();

            collection obtained_collection = database.create_collection(
                collection_name, make_document(kvp("capped", true), kvp("size", 1024 * 1024)));
            REQUIRE(obtained_collection.name() == collection_name);
        }

        SECTION("with default options") {
            database[collection_name].drop();

            collection obtained_collection = database.create_collection(collection_name, {});
            REQUIRE(obtained_collection.name() == collection_name);
            obtained_collection.drop();
        }

        SECTION("with deprecated options") {
            database[collection_name].drop();

            options::create_collection_deprecated opts;
            opts.capped(true);
            opts.size(256);
            opts.max(100);
            opts.no_padding(false);

            BSONCXX_SUPPRESS_DEPRECATION_WARNINGS_BEGIN
            collection obtained_collection = database.create_collection(collection_name, opts);
            BSONCXX_SUPPRESS_DEPRECATION_WARNINGS_END

            REQUIRE(obtained_collection.name() == collection_name);
            obtained_collection.drop();
        }

        SECTION("verify that collection is created server side") {
            database[collection_name].drop();

            database.create_collection(collection_name);

            auto cursor = database.list_collections();
            bool found = false;
            for (const auto& coll : cursor) {
                auto name = bsoncxx::string::to_string(coll["name"].get_string().value);
                if (name == std::string(collection_name)) {
                    found = true;
                    break;
                }
            }
            REQUIRE(found);
        }
    }

    SECTION("A database may be dropped") {
        stdx::string_view collection_name{"collection_drop"};
        database[collection_name].drop();

        database.create_collection(collection_name);
        REQUIRE(database.has_collection(collection_name));
        database.drop();
        REQUIRE(!database.has_collection(collection_name));
    }

    SECTION("read_concern is inherited from parent", "[database]") {
        read_concern::level majority = read_concern::level::k_majority;
        read_concern::level local = read_concern::level::k_local;

        read_concern rc{};
        rc.acknowledge_level(majority);
        mongo_client.read_concern_deprecated(rc);

        mongocxx::database rc_db = mongo_client[database_name];

        SECTION("when parent is a client") {
            REQUIRE(rc_db.read_concern().acknowledge_level() == majority);
        }

        SECTION("except when read_concern is explicitly set") {
            read_concern set_rc{};
            set_rc.acknowledge_level(read_concern::level::k_local);
            rc_db.read_concern(set_rc);

            REQUIRE(rc_db.read_concern().acknowledge_level() == local);
        }
    }

    SECTION("list_collections returns a correct result") {
        mongocxx::database db = mongo_client["list_collections"];
        db.drop();

        collection default_collection = db.create_collection("list_collections_default");
        collection capped_collection = db.create_collection(
            "list_collections_capped", make_document(kvp("capped", true), kvp("size", 256)));

        index_view default_index = default_collection.indexes();
        index_view capped_index = capped_collection.indexes();

        default_index.create_one(make_document(kvp("a", 1)));
        capped_index.create_one(make_document(kvp("a", 1)));

        default_collection.insert_one(make_document(kvp("a", 5)));
        capped_collection.insert_one(make_document(kvp("a", 5)));

        auto cursor1 = db.list_collections();
        std::set<std::string> expected_colls1{"list_collections_default",
                                              "list_collections_capped"};
        REQUIRE(check_for_collections(std::move(cursor1), expected_colls1));

        auto cursor2 = db.list_collections(make_document(kvp("options.capped", true)));
        std::set<std::string> expected_colls2{"list_collections_capped"};
        REQUIRE(check_for_collections(std::move(cursor2), expected_colls2));
    }

    SECTION("list_collection_names returns a correct result") {
        mongocxx::database db = mongo_client["list_collection_names"];
        db.drop();

        std::set<std::string> expected_colls;
        for (std::size_t i = 0; i < 10; ++i) {
            std::string coll_str = "list_collection_names_coll" + std::to_string(i);
            db.create_collection(coll_str);
            expected_colls.insert(coll_str);
        }

        auto names = db.list_collection_names();
        for (auto const& name : names) {
            REQUIRE(expected_colls.erase(name) == 1);
        }

        REQUIRE(expected_colls.size() == 0);
    }
}

// As C++11 lacks generic lambdas, and "ordinary" templates can't appear at block scope,
// we'll have to define our helper for the serviceId tests here. This implementation would
// be more straightforward in newer versions of C++ (C++14 and on allow generic lambdas),
// see CXX-2350 (migration to more recent C++ standards); our C++17 optional<> implementation
// also has a few inconsistencies with the standard, which appear to vary across platforms.
template <typename EventT>
struct check_service_id {
    const bool expect_service_id;

    check_service_id(const bool expect_service_id) : expect_service_id(expect_service_id) {}

    void operator()(const EventT& event) {
        INFO("checking for service_id()")
        CAPTURE(event.command_name(), expect_service_id);

        auto service_id = event.service_id();

        if (expect_service_id)
            CHECK(service_id);
        else
            CHECK_FALSE(service_id);
    }
};

TEST_CASE("serviceId presence depends on load-balancing mode") {
    // Repeat this test case for a situation in which service_id should and should not be returned:
    auto expect_service_id = GENERATE(true, false);

    instance::current();

    auto client_opts = test_util::add_test_server_api();

    // Set Application Performance Monitoring options (APM):
    mongocxx::options::apm apm_opts;
    apm_opts.on_command_started(
        check_service_id<mongocxx::events::command_started_event>{expect_service_id});
    apm_opts.on_command_succeeded(
        check_service_id<mongocxx::events::command_succeeded_event>{expect_service_id});
    apm_opts.on_command_failed(
        check_service_id<mongocxx::events::command_failed_event>{expect_service_id});

    // Set up mocking for get_service_id events:
    auto apm_command_started_get_service_id =
        libmongoc::apm_command_started_get_service_id.create_instance();
    auto apm_command_succeeded_get_service_id =
        libmongoc::apm_command_succeeded_get_service_id.create_instance();
    auto apm_command_failed_get_service_id =
        libmongoc::apm_command_failed_get_service_id.create_instance();

    // Set up mocked functions that DO emit a service_id:
    if (expect_service_id) {
        // Return a bson_oid_t with data where the service_id has some value:
        const auto make_service_id_bson_oid_t = [](const void*) -> const bson_oid_t* {
            static bson_oid_t tmp = {0x65};
            return &tmp;
        };

        // Add forever() so that the mock function also extends to endSession:
        apm_command_started_get_service_id->interpose(make_service_id_bson_oid_t).forever();
        apm_command_succeeded_get_service_id->interpose(make_service_id_bson_oid_t).forever();
        apm_command_failed_get_service_id->interpose(make_service_id_bson_oid_t).forever();
    }

    // Set up mocked functions that DO NOT emit a service_id:
    if (!expect_service_id) {
        auto make_empty_bson_oid_t = [](const void*) -> bson_oid_t* { return nullptr; };

        apm_command_started_get_service_id->interpose(make_empty_bson_oid_t).forever();
        apm_command_succeeded_get_service_id->interpose(make_empty_bson_oid_t).forever();
        apm_command_failed_get_service_id->interpose(make_empty_bson_oid_t).forever();
    }

    // Set up our connection:
    client_opts.apm_opts(apm_opts);
    // Adding ?loadBalanced=true results in an error in the C driver because
    // the initial hello response from the server does not include serviceId.
    client mongo_client(uri("mongodb://localhost:27017/"), client_opts);
    stdx::string_view database_name{"database"};
    database database = mongo_client[database_name];

    // Run a command, triggering start and completion events:
    auto cmd = make_document(kvp("ping", 1));
    database.run_command(cmd.view());

    // Attempt to trigger failure:
    cmd = make_document(kvp("some_sort_of_invalid_command_that_should_never_happen", 1));
    CHECK_THROWS(database.run_command(cmd.view()));
}

}  // namespace
