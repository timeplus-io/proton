#include <chrono>
#include <thread>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/oid.hpp>
#include <bsoncxx/test/catch.hh>
#include <mongocxx/client.hpp>
#include <mongocxx/cursor.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/search_index_view.hpp>
#include <mongocxx/test/client_helpers.hh>

namespace {
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;
using namespace mongocxx;

bool does_search_index_exist_on_cursor(cursor& c, search_index_model& model, bool with_status) {
    for (auto&& doc : c) {
        // check the name, that the index is queryable, and that the definition matches.
        if (doc["name"].get_string().value == *model.name() && doc["queryable"].get_bool().value &&
            doc["latestDefinition"].get_document().view() == model.definition()) {
            // optional addition check needed if with_status is set
            if (!with_status || (with_status && bsoncxx::string::to_string(
                                                    doc["status"].get_string().value) == "READY")) {
                return true;
            }
        }
    }
    return false;
}

// `assert_soon` repeatedly calls `fn` and asserts `fn` eventually returns true.
// `fn` is called every five seconds. Fails if `fn` does not return true within five minutes.
void assert_soon(std::function<bool()> fn) {
    auto start = std::chrono::high_resolution_clock::now();
    while (std::chrono::high_resolution_clock::now() - start < std::chrono::minutes(5)) {
        if (fn()) {
            return;
        }
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }

    FAIL("Expected function to return true within five minutes, but did not");
}

TEST_CASE("atlas search indexes prose tests", "") {
    instance::current();

    auto uri_getenv = std::getenv("MONGODB_URI");
    if (!uri_getenv) {
        WARN("Skipping - Test requires the environment variable: MONGODB_URI");
        return;
    }

    client mongodb_client{uri{uri_getenv}};

    database db = mongodb_client["test"];

    SECTION("create one with name and definition") {
        // use a randomly generated collection name as there's a server side limitation that
        // prevents multiple search indexes with the same name, definition and collection name
        // from being created.
        bsoncxx::oid id;
        auto coll = db.create_collection(id.to_string());
        auto siv = coll.search_indexes();
        // {
        //   name: 'test-search-index',
        //   definition : {
        //     mappings : { dynamic: false }
        //   }
        // }
        auto name = "test-search-index";
        auto definition = make_document(kvp("mappings", make_document(kvp("dynamic", false))));
        auto model = search_index_model(name, definition.view());

        REQUIRE(siv.create_one(name, definition.view()) == "test-search-index");

        assert_soon([&siv, &model](void) -> bool {
            auto cursor = siv.list();
            return does_search_index_exist_on_cursor(cursor, model, false);
        });

        std::cout << "create one with name and definition SUCCESS" << std::endl;
    }

    SECTION("create one with model") {
        bsoncxx::oid id;
        auto coll = db.create_collection(id.to_string());
        auto siv = coll.search_indexes();
        // {
        //   name: 'test-search-index',
        //   definition : {
        //     mappings : { dynamic: false }
        //   }
        // }
        auto name = "test-search-index";
        auto definition = make_document(kvp("mappings", make_document(kvp("dynamic", false))));
        auto model = search_index_model(name, definition.view());

        REQUIRE(siv.create_one(model) == "test-search-index");

        assert_soon([&siv, &model](void) -> bool {
            auto cursor = siv.list();
            return does_search_index_exist_on_cursor(cursor, model, false);
        });

        std::cout << "create one with model SUCCESS" << std::endl;
    }

    SECTION("create many") {
        bsoncxx::oid id;
        auto coll = db.create_collection(id.to_string());
        auto siv = coll.search_indexes();
        // {
        //   name: 'test-search-index-1',
        //   definition : {
        //     mappings : { dynamic: false }
        //   }
        // }
        auto name1 = "test-search-index-1";
        auto definition1 = make_document(kvp("mappings", make_document(kvp("dynamic", false))));
        auto model1 = search_index_model(name1, definition1.view());

        // {
        //   name: 'test-search-index-2',
        //   definition : {
        //     mappings : { dynamic: false }
        //   }
        // }
        auto name2 = "test-search-index-2";
        auto definition2 = make_document(kvp("mappings", make_document(kvp("dynamic", false))));
        auto model2 = search_index_model(name2, definition2.view());

        std::vector<search_index_model> models = {model1, model2};

        std::vector<std::string> result = siv.create_many(models);
        std::vector<std::string> expected = {"test-search-index-1", "test-search-index-2"};
        REQUIRE(result == expected);

        assert_soon([&siv, &model1, &model2](void) -> bool {
            auto cursor = siv.list();
            return does_search_index_exist_on_cursor(cursor, model1, false) &&
                   does_search_index_exist_on_cursor(cursor, model2, false);
        });

        std::cout << "create many SUCCESS" << std::endl;
    }

    SECTION("drop one") {
        bsoncxx::oid id;
        auto coll = db.create_collection(id.to_string());
        auto siv = coll.search_indexes();
        // {
        //   name: 'test-search-index',
        //   definition : {
        //     mappings : { dynamic: false }
        //   }
        // }
        auto name = "test-search-index";
        auto definition = make_document(kvp("mappings", make_document(kvp("dynamic", false))));
        auto model = search_index_model(name, definition.view());

        REQUIRE(siv.create_one(model) == "test-search-index");

        assert_soon([&siv, &model](void) -> bool {
            auto cursor = siv.list();
            return does_search_index_exist_on_cursor(cursor, model, false);
        });

        siv.drop_one(name);

        assert_soon([&siv](void) -> bool {
            auto cursor = siv.list();
            // Return true if empty results are returned.
            return cursor.begin() == cursor.end();
        });

        std::cout << "drop one SUCCESS" << std::endl;
    }

    SECTION("update one") {
        bsoncxx::oid id;
        auto coll = db.create_collection(id.to_string());
        auto siv = coll.search_indexes();
        // {
        //   name: 'test-search-index',
        //   definition : {
        //     mappings : { dynamic: false }
        //   }
        // }
        auto name = "test-search-index";
        auto definition = make_document(kvp("mappings", make_document(kvp("dynamic", false))));
        auto model = search_index_model(name, definition.view());

        REQUIRE(siv.create_one(model) == "test-search-index");

        assert_soon([&siv, &model](void) -> bool {
            auto cursor = siv.list();
            return does_search_index_exist_on_cursor(cursor, model, false);
        });

        // definition : {
        //   mappings : { dynamic: true }
        // }
        auto new_definition = make_document(kvp("mappings", make_document(kvp("dynamic", true))));
        auto new_model = search_index_model(name, new_definition.view());
        siv.update_one(name, new_definition.view());

        assert_soon([&siv, &new_model](void) -> bool {
            auto cursor = siv.list();
            return does_search_index_exist_on_cursor(cursor, new_model, true);
        });

        std::cout << "update one SUCCESS" << std::endl;
    }

    SECTION("drop one suppress namespace not found") {
        bsoncxx::oid id;
        auto coll = db[id.to_string()];
        coll.search_indexes().drop_one("apples");

        std::cout << "drop one supress namespace not found SUCCESS" << std::endl;
    }

    SECTION("create and list search indexes with non-default readConcern and writeConcern") {
        bsoncxx::oid id;
        auto coll = db.create_collection(id.to_string());

        // Apply non-default write concern.
        auto nondefault_wc = mongocxx::write_concern();
        nondefault_wc.nodes(1);
        coll.write_concern(nondefault_wc);

        // Apply non-default read concern.
        auto nondefault_rc = mongocxx::read_concern();
        nondefault_rc.acknowledge_level(mongocxx::read_concern::level::k_majority);
        coll.read_concern(nondefault_rc);

        auto siv = coll.search_indexes();
        auto name = "test-search-index-case6";
        auto definition = make_document(kvp("mappings", make_document(kvp("dynamic", false))));
        auto model = search_index_model(name, definition.view());

        REQUIRE(siv.create_one(name, definition.view()) == "test-search-index-case6");

        assert_soon([&siv, &model](void) -> bool {
            auto cursor = siv.list();
            return does_search_index_exist_on_cursor(cursor, model, false);
        });

        std::cout << "create and list search indexes with non-default readConcern and writeConcern "
                     "SUCCESS"
                  << std::endl;
    }
}
}  // namespace
