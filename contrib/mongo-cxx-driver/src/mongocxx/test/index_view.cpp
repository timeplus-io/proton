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

#include <chrono>
#include <vector>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <bsoncxx/test/catch.hh>
#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/index_view.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/index_view.hpp>
#include <mongocxx/test/client_helpers.hh>

namespace {
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;
using namespace mongocxx;

bool test_commands_enabled(const client& conn) {
    auto result = conn["admin"].run_command(
        make_document(kvp("getParameter", 1), kvp("enableTestCommands", 1)));
    auto result_view = result.view();

    if (!result_view["enableTestCommands"]) {
        return false;
    }

    auto server_version = test_util::get_server_version(conn);

    if (test_util::compare_versions(server_version, "3.2") >= 0) {
        return result_view["enableTestCommands"].get_bool();
    }

    return result_view["enableTestCommands"].get_int32() == 1;
}

bool fail_with_max_timeout(const client& conn) {
    if (!test_commands_enabled(conn)) {
        return false;
    }

    conn["admin"].run_command(
        make_document(kvp("configureFailPoint", "maxTimeAlwaysTimeOut"), kvp("mode", "alwaysOn")));

    return true;
}

void disable_fail_point(const client& conn) {
    if (test_commands_enabled(conn)) {
        conn["admin"].run_command(
            make_document(kvp("configureFailPoint", "maxTimeAlwaysTimeOut"), kvp("mode", "off")));
    }
}

TEST_CASE("create_one", "[index_view]") {
    instance::current();

    client mongodb_client{uri{}, test_util::add_test_server_api()};
    database db = mongodb_client["index_view_create_one"];

    SECTION("works with document and options") {
        collection coll = db["index_view_create_one_doc_and_opts"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.
        index_view indexes = coll.indexes();

        auto key = make_document(kvp("a", 1));
        auto options = make_document(kvp("name", "myIndex"));
        stdx::optional<std::string> result = indexes.create_one(key.view(), options.view());

        REQUIRE(result);
        REQUIRE(*result == "myIndex");
    }

    SECTION("works with document and no options") {
        collection coll = db["index_view_create_one_doc"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.
        index_view indexes = coll.indexes();

        auto key = make_document(kvp("a", 1), kvp("b", -1));
        stdx::optional<std::string> result = indexes.create_one(key.view());

        REQUIRE(result);
        REQUIRE(*result == "a_1_b_-1");
    }

    SECTION("with index_model and options") {
        collection coll = db["index_view_create_one_index_model_opts"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.
        index_view indexes = coll.indexes();

        auto key = make_document(kvp("a", 1));
        auto options = make_document(kvp("name", "myIndex"));
        index_model model(key.view(), options.view());
        stdx::optional<std::string> result = indexes.create_one(model);

        REQUIRE(result);
        REQUIRE(*result == "myIndex");
    }

    SECTION("with index_model and no options") {
        collection coll = db["index_view_create_one_index_model"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.
        index_view indexes = coll.indexes();

        auto key = make_document(kvp("a", 1), kvp("b", -1));
        index_model model(key.view());
        stdx::optional<std::string> result = indexes.create_one(model);

        REQUIRE(result);
        REQUIRE(*result == "a_1_b_-1");
    }

    SECTION("tests maxTimeMS option works") {
        collection coll = db["index_view_create_one_maxTimeMS"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.
        index_view indexes = coll.indexes();

        auto key = make_document(kvp("aaa", 1));
        index_model model(key.view());
        options::index_view options;
        options.max_time(std::chrono::milliseconds(1));

        if (fail_with_max_timeout(mongodb_client)) {
            REQUIRE_THROWS_AS(indexes.create_one(model, options), operation_exception);
        }
        disable_fail_point(mongodb_client);
    }

    SECTION("fails for same keys and options") {
        collection coll = db["index_view_create_one_exists_fail"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.
        index_view indexes = coll.indexes();

        auto keys = make_document(kvp("a", 1));

        REQUIRE(indexes.create_one(keys.view()));
        REQUIRE(!indexes.create_one(keys.view()));
    }

    SECTION("fails for same name") {
        collection coll = db["index_view_create_one_same_name_fail"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.
        index_view indexes = coll.indexes();

        auto keys1 = make_document(kvp("a", 1));
        auto keys2 = make_document(kvp("a", -1));
        auto options = make_document(kvp("name", "myIndex"));

        REQUIRE_NOTHROW(indexes.create_one(keys1.view(), options.view()));
        REQUIRE_THROWS_AS(indexes.create_one(keys2.view(), options.view()), operation_exception);
    }

    SECTION("commitQuorum option") {
        if (test_util::get_topology(mongodb_client) == "single") {
            WARN("skip: commitQuorum option requires a replica set");
            return;
        }

        collection coll = db["index_view_create_one_commit_quorum"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.
        index_view indexes = coll.indexes();

        auto key = make_document(kvp("a", 1));
        index_model model(key.view());
        options::index_view options;

        auto commit_quorum_regex =
            Catch::Matches("(.*)commit( )?quorum(.*)", Catch::CaseSensitive::No);

        bool is_supported = test_util::get_max_wire_version(mongodb_client) >= 9;
        CAPTURE(is_supported);

        SECTION("works with int") {
            options.commit_quorum(1);
            if (is_supported) {
                REQUIRE_NOTHROW(indexes.create_one(model, options));
            } else {
                REQUIRE_THROWS_WITH(indexes.create_one(model, options), commit_quorum_regex);
            }
        }

        SECTION("works with string") {
            options.commit_quorum("majority");
            if (is_supported) {
                REQUIRE_NOTHROW(indexes.create_one(model, options));
            } else {
                REQUIRE_THROWS_WITH(indexes.create_one(model, options), commit_quorum_regex);
            }
        }
    }
}

TEST_CASE("create_many", "[index_view]") {
    instance::current();

    client mongodb_client{uri{}, test_util::add_test_server_api()};
    database db = mongodb_client["index_view_create_many"];

    std::vector<index_model> models{index_model(make_document(kvp("a", 1))),
                                    index_model(make_document(kvp("b", 1), kvp("c", -1))),
                                    index_model(make_document(kvp("c", -1)))};

    SECTION("test maxTimeMS option") {
        collection coll = db["index_view_create_many_maxTimeMS"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.

        index_view indexes = coll.indexes();

        options::index_view options;
        options.max_time(std::chrono::milliseconds(1));

        if (fail_with_max_timeout(mongodb_client)) {
            REQUIRE_THROWS_AS(indexes.create_many(models, options), operation_exception);
        }
        disable_fail_point(mongodb_client);
    }

    SECTION("create three") {
        collection coll = db["index_view_create_many"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.

        index_view indexes = coll.indexes();

        bsoncxx::document::value result = indexes.create_many(models);
        bsoncxx::document::view result_view = result.view();

        // SERVER-78611: sharded clusters may place fields in a raw response document instead of in
        // the top-level document.
        if (const auto raw = result_view["raw"]) {
            for (const auto& shard_response : raw.get_document().view()) {
                result_view = shard_response.get_document().view();
            }
        }

        REQUIRE((result_view["numIndexesAfter"].get_int32() -
                 result_view["numIndexesBefore"].get_int32()) == 3);

        std::vector<std::string> expected_names{"a_1", "b_1_c_-1", "c_-1"};
        std::int8_t found = 0;

        for (auto&& index : indexes.list()) {
            auto name = index["name"].get_string();

            for (auto expected : expected_names) {
                if (stdx::string_view(expected) == name.value) {
                    found++;
                }
            }
        }
        REQUIRE(found == 3);
    }
}

TEST_CASE("drop_one", "[index_view]") {
    instance::current();

    client mongodb_client{uri{}, test_util::add_test_server_api()};
    database db = mongodb_client["index_view_drop_one"];

    SECTION("drops index by name") {
        collection coll = db["index_view_drop_one_by_name"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.

        index_view indexes = coll.indexes();
        auto cursor = indexes.list();
        REQUIRE(std::distance(cursor.begin(), cursor.end()) == 1);

        auto key = make_document(kvp("a", 1));
        stdx::optional<std::string> result = indexes.create_one(key.view());

        auto cursor1 = indexes.list();
        REQUIRE(std::distance(cursor1.begin(), cursor1.end()) == 2);
        REQUIRE(result);

        indexes.drop_one(*result);
        auto cursor2 = indexes.list();
        REQUIRE(std::distance(cursor2.begin(), cursor2.end()) == 1);
    }

    SECTION("drops index by key and options") {
        collection coll = db["index_view_drop_one_by_key_and_opts"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.

        index_view indexes = coll.indexes();
        auto cursor = indexes.list();
        REQUIRE(std::distance(cursor.begin(), cursor.end()) == 1);

        auto key = make_document(kvp("a", 1));
        stdx::optional<std::string> result = indexes.create_one(key.view());

        auto cursor1 = indexes.list();
        REQUIRE(std::distance(cursor1.begin(), cursor1.end()) == 2);
        REQUIRE(result);

        indexes.drop_one(key.view());
        auto cursor2 = indexes.list();
        REQUIRE(std::distance(cursor2.begin(), cursor2.end()) == 1);
    }

    SECTION("drops index by index_model") {
        collection coll = db["index_view_drop_one_by_index_model"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.

        index_view indexes = coll.indexes();
        auto cursor = indexes.list();
        REQUIRE(std::distance(cursor.begin(), cursor.end()) == 1);

        auto key = make_document(kvp("a", 1));
        index_model model(key.view());
        stdx::optional<std::string> result = indexes.create_one(key.view());

        auto cursor1 = indexes.list();
        REQUIRE(std::distance(cursor1.begin(), cursor1.end()) == 2);
        REQUIRE(result);

        indexes.drop_one(model);
        auto cursor2 = indexes.list();
        REQUIRE(std::distance(cursor2.begin(), cursor2.end()) == 1);
    }

    SECTION("fails for drop_one on *") {
        collection coll = db["index_view_drop_one_*_fail"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.

        index_view indexes = coll.indexes();
        auto cursor = indexes.list();
        REQUIRE(std::distance(cursor.begin(), cursor.end()) == 1);

        REQUIRE_THROWS_AS(indexes.drop_one("*"), logic_error);
    }

    SECTION("fails for index that doesn't exist") {
        collection coll = db["index_view_drop_one_nonexistant_fail"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.

        index_view indexes = coll.indexes();
        auto cursor = indexes.list();
        REQUIRE(std::distance(cursor.begin(), cursor.end()) == 1);

        REQUIRE_THROWS_AS(indexes.drop_one("foo"), operation_exception);
    }
}

TEST_CASE("drop_all", "[index_view]") {
    instance::current();

    client mongodb_client{uri{}, test_util::add_test_server_api()};
    database db = mongodb_client["index_view_drop_all"];

    std::vector<index_model> models{index_model{make_document(kvp("a", 1))},
                                    index_model{make_document(kvp("b", 1), kvp("c", -1))},
                                    index_model{make_document(kvp("c", -1))}};

    SECTION("drop normally") {
        collection coll = db["index_view_drop_all"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.

        index_view indexes = coll.indexes();

        bsoncxx::document::value result = indexes.create_many(models);
        bsoncxx::document::view result_view = result.view();

        // SERVER-78611: sharded clusters may place fields in a raw response document instead of
        // in the top-level document.
        if (const auto raw = result_view["raw"]) {
            for (const auto& shard_response : raw.get_document().view()) {
                result_view = shard_response.get_document().view();
            }
        }

        auto cursor1 = indexes.list();
        REQUIRE((unsigned)std::distance(cursor1.begin(), cursor1.end()) == models.size() + 1);
        REQUIRE((unsigned)(result_view["numIndexesAfter"].get_int32() -
                           result_view["numIndexesBefore"].get_int32()) == models.size());

        indexes.drop_all();
        auto cursor2 = indexes.list();

        REQUIRE(std::distance(cursor2.begin(), cursor2.end()) == 1);
    }

    SECTION("test maxTimeMS option") {
        collection coll = db["index_view_drop_all_maxTimeMS"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.

        index_view indexes = coll.indexes();

        bsoncxx::document::value result = indexes.create_many(models);
        bsoncxx::document::view result_view = result.view();

        // SERVER-78611: sharded clusters may place fields in a raw response document instead of
        // in the top-level document.
        if (const auto raw = result_view["raw"]) {
            for (const auto& shard_response : raw.get_document().view()) {
                result_view = shard_response.get_document().view();
            }
        }

        auto cursor1 = indexes.list();
        REQUIRE((unsigned)std::distance(cursor1.begin(), cursor1.end()) == models.size() + 1);
        REQUIRE((unsigned)(result_view["numIndexesAfter"].get_int32() -
                           result_view["numIndexesBefore"].get_int32()) == models.size());

        options::index_view options;
        options.max_time(std::chrono::milliseconds(1));

        if (fail_with_max_timeout(mongodb_client)) {
            REQUIRE_THROWS_AS(indexes.drop_all(options), operation_exception);
        }
        disable_fail_point(mongodb_client);
    }
}

TEST_CASE("index creation and deletion with different collation") {
    instance::current();

    client mongodb_client{uri{}, test_util::add_test_server_api()};

    if (test_util::get_max_wire_version(mongodb_client) >= 5) {
        database db = mongodb_client["index_view_collation"];
        collection coll = db["index_view_collation"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.

        bsoncxx::document::value keys = make_document(kvp("a", 1), kvp("bcd", -1), kvp("d", 1));
        bsoncxx::document::value us_collation =
            make_document(kvp("collation", make_document(kvp("locale", "en_US"))));
        bsoncxx::document::value ko_collation = make_document(
            kvp("name", "custom_index_name"), kvp("collation", make_document(kvp("locale", "ko"))));

        index_model index_us{keys.view(), us_collation.view()};
        index_model index_ko{keys.view(), ko_collation.view()};

        index_view view = coll.indexes();

        view.create_one(index_us);
        view.create_one(index_ko);

        auto cursor = view.list();
        REQUIRE(std::distance(cursor.begin(), cursor.end()) == 3);

        view.drop_one("a_1_bcd_-1_d_1");

        auto cursor_after_drop = view.list();

        REQUIRE(std::distance(cursor_after_drop.begin(), cursor_after_drop.end()) == 2);

        auto cursor_after_drop1 = view.list();
        auto index_it = cursor_after_drop1.begin();
        ++index_it;
        bsoncxx::document::view index = *index_it;

        REQUIRE(bsoncxx::string::to_string(index["name"].get_string().value) ==
                "custom_index_name");

        coll.drop();
        db.drop();
    }
}
}  // namespace
