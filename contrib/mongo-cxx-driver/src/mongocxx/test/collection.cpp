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

#include <chrono>
#include <iostream>
#include <iterator>
#include <new>
#include <sstream>
#include <vector>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/stdx/make_unique.hpp>
#include <bsoncxx/stdx/string_view.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <bsoncxx/test/catch.hh>
#include <bsoncxx/types.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/exception/bulk_write_exception.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/exception/query_exception.hpp>
#include <mongocxx/exception/write_exception.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pipeline.hpp>
#include <mongocxx/private/libbson.hh>
#include <mongocxx/private/libmongoc.hh>
#include <mongocxx/read_concern.hpp>
#include <mongocxx/test/client_helpers.hh>
#include <mongocxx/write_concern.hpp>

namespace {

using bsoncxx::builder::basic::document;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_array;
using bsoncxx::builder::basic::make_document;

using namespace mongocxx;
using test_util::server_has_sessions;

TEST_CASE("A default constructed collection cannot perform operations", "[collection]") {
    instance::current();

    collection c;
    REQUIRE_THROWS_AS(c.name(), mongocxx::logic_error);
}

TEST_CASE("mongocxx::collection copy constructor", "[collection]") {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};
    database db = client["collection_copy_constructor"];

    SECTION("constructing from valid") {
        collection collection_a = db["a"];
        collection collection_b{collection_a};
        REQUIRE(collection_b);
        REQUIRE(collection_b.name() == stdx::string_view{"a"});
    }

    SECTION("constructing from invalid") {
        collection collection_a;
        collection collection_b{collection_a};
        REQUIRE(!collection_b);
    }
}

TEST_CASE("mongocxx::collection copy assignment operator", "[collection]") {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};
    database db = client["collection_copy_assignment"];

    SECTION("assigning valid to valid") {
        collection collection_a = db["a1"];
        collection collection_b = db["b1"];
        collection_b = collection_a;
        REQUIRE(collection_b);
        REQUIRE(collection_b.name() == stdx::string_view{"a1"});
    }

    SECTION("assigning invalid to valid") {
        collection collection_a;
        collection collection_b = db["b2"];
        collection_b = collection_a;
        REQUIRE(!collection_b);
    }

    SECTION("assigning valid to invalid") {
        collection collection_a = db["a3"];
        collection collection_b;
        collection_b = collection_a;
        REQUIRE(collection_b);
        REQUIRE(collection_b.name() == stdx::string_view{"a3"});
    }

    SECTION("assigning invalid to invalid") {
        collection collection_a;
        collection collection_b;
        collection_b = collection_a;
        REQUIRE(!collection_b);
    }
}

TEST_CASE("collection renaming", "[collection]") {
    instance::current();

    client mongodb_client{uri{}, test_util::add_test_server_api()};
    database db = mongodb_client["collection_renaming"];

    auto filter = make_document(kvp("key--------unique", "value"));

    std::string collname{"mongo_cxx_driver"};
    std::string other_collname{"mongo_cxx_again"};

    collection coll = db[collname];
    collection other_coll = db[other_collname];

    coll.drop();
    other_coll.drop();

    coll.insert_one(filter.view());  // Ensure that the collection exists.
    other_coll.insert_one({});

    REQUIRE(coll.name() == stdx::string_view(collname));

    std::string new_name{"mongo_cxx_newname"};
    coll.rename(new_name, false);

    REQUIRE(coll.name() == stdx::string_view(new_name));

    REQUIRE(coll.find_one(filter.view(), {}));

    coll.rename(other_collname, true);
    REQUIRE(coll.name() == stdx::string_view(other_collname));
    REQUIRE(coll.find_one(filter.view(), {}));

    coll.drop();
}

TEST_CASE("collection dropping") {
    instance::current();

    client mongodb_client{uri{}, test_util::add_test_server_api()};
    database db = mongodb_client["collection_dropping"];

    std::string collname{"mongo_cxx_driver"};
    collection coll = db[collname];
    coll.insert_one({});  // Ensure that the collection exists.

    REQUIRE_NOTHROW(coll.drop());
}

TEST_CASE("CRUD functionality", "[driver::collection]") {
    instance::current();

    client mongodb_client{uri{}, test_util::add_test_server_api()};
    database db = mongodb_client["collection_crud_functionality"];

    auto case_insensitive_collation = make_document(kvp("locale", "en_US"), kvp("strength", 2));

    auto noack = write_concern{};
    noack.acknowledge_level(write_concern::level::k_unacknowledged);

    write_concern default_wc;
    default_wc.acknowledge_level(write_concern::level::k_default);

    SECTION("insert and read single document", "[collection]") {
        collection coll = db["insert_and_read_one"];
        coll.drop();

        auto b = make_document(kvp("_id", bsoncxx::oid{}), kvp("x", 1));

        REQUIRE(coll.insert_one(b.view()));

        auto c = make_document(kvp("x", 1));
        REQUIRE(coll.insert_one(c.view()));

        auto cursor = coll.find(b.view());

        std::size_t i = 0;
        for (auto&& x : cursor) {
            REQUIRE(x["_id"].get_oid().value == b.view()["_id"].get_oid().value);
            i++;
        }

        REQUIRE(i == 1);
    }

    SECTION("insert_one returns correct result object", "[collection]") {
        stdx::string_view expected_id{"foo"};

        auto doc = make_document(kvp("_id", expected_id));

        SECTION("default write concern returns result") {
            collection coll = db["insert_one_default_write"];
            coll.drop();
            auto result = coll.insert_one(doc.view());
            REQUIRE(result);
            REQUIRE(result->result().inserted_count() == 1);
            REQUIRE(result->inserted_id().type() == bsoncxx::type::k_string);
            REQUIRE(result->inserted_id().get_string().value == expected_id);
        }

        SECTION("unacknowledged write concern returns disengaged optional", "[collection]") {
            if (test_util::get_max_wire_version(mongodb_client) > 13) {
                WARN("Skipping - getLastError removed in SERVER-57390");
                return;
            }
            collection coll = db["insert_one_unack_write"];
            coll.drop();
            options::insert opts{};
            opts.write_concern(noack);

            auto result = coll.insert_one(doc.view(), opts);
            REQUIRE(!result);

            // Block until server has received the write request, to prevent
            // this unacknowledged write from racing with writes to this
            // collection from other sections.
            db.run_command(make_document(kvp("getLastError", 1)));

            auto count = coll.count_documents({});
            REQUIRE(count == 1);
        }

        SECTION("bypass_document_validation ignores validation_criteria", "[collection]") {
            std::string collname = "insert_one_bypass_document_validation";
            db[collname].drop();
            collection coll = db.create_collection(
                collname,
                make_document(
                    kvp("validator", make_document(kvp("_id", make_document(kvp("$eq", "baz")))))));

            options::insert options;
            options.bypass_document_validation(true);

            stdx::optional<result::insert_one> result;
            REQUIRE_NOTHROW(result = coll.insert_one(doc.view(), options));
            REQUIRE(result);
            REQUIRE(result->result().inserted_count() == 1);
            REQUIRE(result->inserted_id().type() == bsoncxx::type::k_string);
            REQUIRE(result->inserted_id().get_string().value == expected_id);
        }
    }

    SECTION("insert and read multiple documents", "[collection]") {
        collection coll = db["insert_and_read_multi"];
        coll.drop();
        bsoncxx::builder::basic::document b1;
        bsoncxx::builder::basic::document b2;
        bsoncxx::builder::basic::document b3;
        bsoncxx::builder::basic::document b4;

        b1.append(kvp("_id", bsoncxx::oid{}), kvp("x", 1));
        b2.append(kvp("x", 2));
        b3.append(kvp("x", 3));
        b4.append(kvp("_id", bsoncxx::oid{}), kvp("x", 4));

        std::vector<bsoncxx::document::view> docs{};
        docs.push_back(b1.view());
        docs.push_back(b2.view());
        docs.push_back(b3.view());
        docs.push_back(b4.view());

        auto result = coll.insert_many(docs, options::insert{});
        auto cursor = coll.find({});

        SECTION("result count is correct") {
            REQUIRE(result);
            REQUIRE(result->inserted_count() == 4);
        }

        SECTION("read inserted values with range-for") {
            std::int32_t i = 0;
            for (auto&& x : cursor) {
                i++;
                REQUIRE(x["x"].get_int32() == i);
            }

            REQUIRE(i == 4);
        }

        SECTION("multiple iterators move in lockstep") {
            auto end = cursor.end();
            REQUIRE(cursor.begin() != end);

            auto iter1 = cursor.begin();
            auto iter2 = cursor.begin();
            REQUIRE(iter1 == iter2);
            REQUIRE(*iter1 == *iter2);
            iter1++;
            REQUIRE(iter1 == iter2);
            REQUIRE(iter1 != end);
            REQUIRE(*iter1 == *iter2);
        }
    }

    SECTION("insert_many returns correct result object", "[collection]") {
        bsoncxx::builder::basic::document b1;
        bsoncxx::builder::basic::document b2;

        b1.append(kvp("_id", "foo"), kvp("x", 1));
        b2.append(kvp("x", 2));

        std::vector<bsoncxx::document::view> docs{};
        docs.push_back(b1.view());
        docs.push_back(b2.view());

        SECTION("default write concern returns result") {
            collection coll = db["insert_many_default_write"];
            coll.drop();
            auto result = coll.insert_many(docs);

            REQUIRE(result);

            // Verify result->result() is correct:
            REQUIRE(result->result().inserted_count() == 2);

            // Verify result->inserted_count() is correct:
            REQUIRE(result->inserted_count() == 2);

            // Verify result->inserted_ids() is correct:
            auto id_map = result->inserted_ids();
            REQUIRE(id_map[0].type() == bsoncxx::type::k_string);
            REQUIRE(id_map[0].get_string().value == stdx::string_view{"foo"});
            REQUIRE(id_map[1].type() == bsoncxx::type::k_oid);
            auto second_inserted_doc = coll.find_one(make_document(kvp("x", 2)));
            REQUIRE(second_inserted_doc);
            REQUIRE(second_inserted_doc->view()["_id"]);
            REQUIRE(second_inserted_doc->view()["_id"].type() == bsoncxx::type::k_oid);
            REQUIRE(id_map[1].get_oid().value ==
                    second_inserted_doc->view()["_id"].get_oid().value);
        }

        SECTION("unacknowledged write concern returns disengaged optional") {
            if (test_util::get_max_wire_version(mongodb_client) > 13) {
                WARN("Skipping - getLastError removed in SERVER-57390");
                return;
            }
            collection coll = db["insert_many_unack_write"];
            coll.drop();
            options::insert opts{};
            opts.write_concern(noack);

            auto result = coll.insert_many(docs, opts);
            REQUIRE(!result);

            // Block until server has received the write request, to prevent
            // this unacknowledged write from racing with writes to this
            // collection from other sections.
            db.run_command(make_document(kvp("getLastError", 1)));
        }

        SECTION("result::insert_many copy ctor/assign reuses id references", "[collection]") {
            // Verify that two id->value maps have the same underlying content,
            // but are not pointing at the same memory.
            using id_map = mongocxx::result::insert_many::id_map;
            const auto verifyEquivalentNotIdentical = [](const id_map& lhs, const id_map& rhs) {
                REQUIRE(lhs.size() == rhs.size());
                for (const auto& lhsIdVal : lhs) {
                    const auto& rhsIdVal = rhs.find(lhsIdVal.first);

                    // copyIds[idx] doesn't exist, but ids[idx] does.
                    REQUIRE(rhsIdVal != rhs.end());

                    const auto& lhsVal = lhsIdVal.second;
                    const auto& rhsVal = rhsIdVal->second;

                    // The element wasn't duplicated.
                    REQUIRE(lhsVal.raw() != rhsVal.raw());

                    // Contents should match.
                    REQUIRE(lhsVal.length() == rhsVal.length());
                    REQUIRE(memcmp(lhsVal.raw(), rhsVal.raw(), lhsVal.length()) == 0);
                }
            };

            std::string collname("result_insert_many_stale_references");
            db[collname].drop();
            auto coll = db.create_collection(collname);
            const auto result = coll.insert_many(docs);
            REQUIRE(result);

            const auto& ids = result->inserted_ids();
            REQUIRE(!ids.empty());

            mongocxx::result::insert_many resultCopy(*result);
            const auto& copyIds = resultCopy.inserted_ids();
            verifyEquivalentNotIdentical(ids, copyIds);

            auto resultAssign = *result;
            const auto& assignIds = resultAssign.inserted_ids();
            verifyEquivalentNotIdentical(ids, assignIds);

            verifyEquivalentNotIdentical(copyIds, assignIds);
        }

        SECTION("bypass_document_validation ignores validation_criteria", "[collection]") {
            std::string collname = "insert_many_bypass_document_validation";
            db[collname].drop();
            collection coll = db.create_collection(
                collname,
                make_document(
                    kvp("validator", make_document(kvp("x", make_document(kvp("$eq", 1)))))));

            options::insert options;
            options.bypass_document_validation(true);

            stdx::optional<result::insert_many> result;
            REQUIRE_NOTHROW(result = coll.insert_many(docs, options));

            REQUIRE(result);

            // Verify result->result() is correct:
            REQUIRE(result->result().inserted_count() == 2);

            // Verify result->inserted_count() is correct:
            REQUIRE(result->inserted_count() == 2);

            // Verify result->inserted_ids() is correct:
            auto id_map = result->inserted_ids();
            REQUIRE(id_map[0].type() == bsoncxx::type::k_string);
            REQUIRE(id_map[0].get_string().value == stdx::string_view{"foo"});
            REQUIRE(id_map[1].type() == bsoncxx::type::k_oid);
            auto second_inserted_doc = coll.find_one(make_document(kvp("x", 2)));
            REQUIRE(second_inserted_doc);
            REQUIRE(second_inserted_doc->view()["_id"]);
            REQUIRE(second_inserted_doc->view()["_id"].type() == bsoncxx::type::k_oid);
            REQUIRE(id_map[1].get_oid().value ==
                    second_inserted_doc->view()["_id"].get_oid().value);
        }
    }

    SECTION("find does not leak on error", "[collection]") {
        collection coll = db["find_error_no_leak"];
        coll.drop();
        auto find_opts = options::find{}.max_await_time(std::chrono::milliseconds{-1});

        REQUIRE_THROWS_AS(coll.find({}, find_opts), logic_error);
    }

    SECTION("find with collation", "[collection]") {
        collection coll = db["find_with_collation"];
        coll.drop();
        auto b = make_document(kvp("x", "foo"));
        REQUIRE(coll.insert_one(b.view()));

        auto predicate = make_document(kvp("x", "FOO"));
        auto find_opts = options::find{}.collation(case_insensitive_collation.view());
        auto cursor = coll.find(predicate.view(), find_opts);
        if (test_util::supports_collation(mongodb_client)) {
            REQUIRE(std::distance(cursor.begin(), cursor.end()) == 1);
        } else {
            REQUIRE_THROWS_AS(std::distance(cursor.begin(), cursor.end()), query_exception);
        }
    }

    SECTION("find with return_key", "[collection]") {
        collection coll = db["find_with_return_key"];
        coll.drop();
        auto doc = make_document(kvp("a", 3));
        REQUIRE(coll.insert_one(doc.view()));

        index_view indexes = coll.indexes();

        auto key = make_document(kvp("a", 1));
        stdx::optional<std::string> result = indexes.create_one(key.view());

        auto find_opts = options::find{}.return_key(true);
        auto cursor = coll.find(doc.view(), find_opts);

        std::size_t i = 0;
        for (auto&& x : cursor) {
            REQUIRE(!x["_id"]);
            REQUIRE(x["a"].get_int32().value == 3);
            i++;
        }

        REQUIRE(i == 1);
    }

    SECTION("find with show_record_id", "[collection") {
        collection coll = db["find_with_show_record_id"];
        coll.drop();
        auto doc = make_document(kvp("a", 3));
        REQUIRE(coll.insert_one(doc.view()));

        auto find_opts = options::find{}.show_record_id(true);
        auto cursor = coll.find(doc.view(), find_opts);

        std::size_t i = 0;
        for (auto&& x : cursor) {
            REQUIRE(x.find("$recordId") != x.end());
            i++;
        }

        REQUIRE(i == 1);
    }

    SECTION("find_one with collation", "[collection]") {
        collection coll = db["find_one_with_collation"];
        coll.drop();
        auto b = make_document(kvp("x", "foo"));
        REQUIRE(coll.insert_one(b.view()));

        auto predicate = make_document(kvp("x", "FOO"));
        auto find_opts = options::find{}.collation(case_insensitive_collation.view());
        if (test_util::supports_collation(mongodb_client)) {
            REQUIRE(coll.find_one(predicate.view(), find_opts));
        } else {
            REQUIRE_THROWS_AS(coll.find_one(predicate.view(), find_opts), query_exception);
        }
    }

    SECTION("insert and update single document", "[collection]") {
        collection coll = db["insert_and_update_one"];
        coll.drop();
        auto b1 = make_document(kvp("_id", 1));

        coll.insert_one(b1.view());

        auto doc = coll.find_one({});
        REQUIRE(doc);
        REQUIRE(doc->view()["_id"].get_int32() == 1);

        bsoncxx::builder::basic::document update_doc;
        update_doc.append(kvp("$set", make_document(kvp("changed", true))));

        coll.update_one(b1.view(), update_doc.view());

        auto updated = coll.find_one({});
        REQUIRE(updated);
        REQUIRE(updated->view()["changed"].get_bool() == true);
    }

    SECTION("update_one can take a pipeline", "[collection]") {
        if (!test_util::newer_than(mongodb_client, "4.1.11")) {
            WARN("skip: pipeline updates require 4.1.11");
            return;
        }

        collection coll = db["update_one_pipeline"];
        coll.drop();

        auto bson = make_document(kvp("_id", 1));
        coll.insert_one(bson.view());

        auto doc = coll.find_one({});
        REQUIRE(doc);
        REQUIRE(doc->view()["_id"].get_int32() == 1);

        pipeline update;
        auto new_fields = make_document(kvp("name", "Charlotte"));
        update.add_fields(new_fields.view());

        coll.update_one(bson.view(), {});
        coll.update_one(bson.view(), update);

        auto result = coll.find_one(bson.view());
        REQUIRE(result);
        REQUIRE(result->view()["name"].get_string().value == stdx::string_view("Charlotte"));

        // Try adding stages with append_stage(s) instead
        pipeline array_update;

        using bsoncxx::builder::basic::sub_document;
        bsoncxx::builder::basic::array stages{};

        bsoncxx::builder::basic::document stage{};
        stage.append(kvp("$addFields", make_document(kvp("lastname", "Krause"))));
        bsoncxx::builder::basic::document stage2{};
        stage2.append(kvp("$addFields", make_document(kvp("department", "VIS"))));
        stages.append(stage.extract());
        stages.append(stage2.extract());

        bsoncxx::builder::basic::document stage3{};
        stage3.append(kvp("$addFields", make_document(kvp("count", 1))));

        array_update.append_stages(stages.extract());
        array_update.append_stage(stage3.extract());
        coll.update_one(bson.view(), array_update);

        result = coll.find_one(bson.view());
        REQUIRE(result);
        REQUIRE(result->view()["lastname"].get_string().value == stdx::string_view("Krause"));
        REQUIRE(result->view()["department"].get_string().value == stdx::string_view("VIS"));
        REQUIRE(result->view()["count"].get_int32().value == 1);
    }

    SECTION("update_one returns correct result object", "[collection]") {
        auto b1 = make_document(kvp("_id", 1));

        bsoncxx::builder::basic::document update_doc;
        update_doc.append(kvp("$set", make_document(kvp("changed", true))));

        SECTION("default write concern returns result") {
            collection coll = db["update_one_default_write"];
            coll.drop();

            coll.insert_one(b1.view());

            auto result = coll.update_one(b1.view(), update_doc.view());
            REQUIRE(result);
            REQUIRE(result->result().matched_count() == 1);
        }

        SECTION("unacknowledged write concern returns disengaged optional") {
            if (test_util::get_max_wire_version(mongodb_client) > 13) {
                WARN("Skipping - getLastError removed in SERVER-57390");
                return;
            }
            collection coll = db["update_one_unack_write"];
            coll.drop();

            coll.insert_one(b1.view());
            options::update opts{};
            opts.write_concern(noack);

            auto result = coll.update_one(b1.view(), update_doc.view(), opts);
            REQUIRE(!result);

            // Block until server has received the write request, to prevent
            // this unacknowledged write from racing with writes to this
            // collection from other sections.
            db.run_command(make_document(kvp("getLastError", 1)));
        }

        SECTION("bypass_document_validation ignores validation_criteria", "[collection]") {
            std::string collname = "update_one_bypass_document_validation";
            db[collname].drop();
            collection coll = db.create_collection(
                collname,
                make_document(kvp(
                    "validator", make_document(kvp("changed", make_document(kvp("$eq", false)))))));

            options::update options;
            options.bypass_document_validation(true);

            auto doc = make_document(kvp("_id", 1), kvp("changed", false));

            coll.insert_one(doc.view());

            stdx::optional<result::update> result;
            REQUIRE_NOTHROW(result = coll.update_one(doc.view(), update_doc.view(), options));

            REQUIRE(result);
            REQUIRE(result->result().matched_count() == 1);
        }
    }

    SECTION("update_one with collation", "[collection]") {
        collection coll = db["update_one_with_collation"];
        coll.drop();
        auto b = make_document(kvp("x", "foo"));
        REQUIRE(coll.insert_one(b.view()));

        auto predicate = make_document(kvp("x", "FOO"));

        bsoncxx::builder::basic::document update_doc;
        update_doc.append(kvp("$set", make_document(kvp("changed", true))));

        auto update_opts = options::update{}.collation(case_insensitive_collation.view());
        if (test_util::supports_collation(mongodb_client)) {
            INFO("unacknowledged write concern fails");
            update_opts.write_concern(noack);
            REQUIRE_THROWS_AS(coll.update_one(predicate.view(), update_doc.view(), update_opts),
                              operation_exception);

            INFO("default write concern succeeds");
            update_opts.write_concern(default_wc);
            auto result = coll.update_one(predicate.view(), update_doc.view(), update_opts);
            REQUIRE(result);
            REQUIRE(result->modified_count() == 1);
        } else {
            REQUIRE_THROWS_AS(coll.update_one(predicate.view(), update_doc.view(), update_opts),
                              bulk_write_exception);
        }
    }

    SECTION("insert and update multiple documents", "[collection]") {
        collection coll = db["insert_and_update_multi"];
        coll.drop();
        auto b1 = make_document(kvp("x", 1));

        coll.insert_one(b1.view());
        coll.insert_one(b1.view());

        auto b2 = make_document(kvp("x", 2));

        coll.insert_one(b2.view());

        REQUIRE(coll.count_documents(b1.view()) == 2);

        auto bchanged = make_document(kvp("changed", true));

        bsoncxx::builder::basic::document update_doc;
        update_doc.append(kvp("$set", bchanged.view()));

        coll.update_many(b1.view(), update_doc.view());

        REQUIRE(coll.count_documents(bchanged.view()) == 2);
    }

    SECTION("update_many returns correct result object", "[collection]") {
        auto b1 = make_document(kvp("x", 1));

        auto bchanged = make_document(kvp("changed", true));

        bsoncxx::builder::basic::document update_doc;
        update_doc.append(kvp("$set", bchanged.view()));

        SECTION("default write concern returns result") {
            collection coll = db["update_many_default_write"];
            coll.drop();

            coll.insert_one(b1.view());
            coll.insert_one(b1.view());

            auto result = coll.update_many(b1.view(), update_doc.view());
            REQUIRE(result);
            REQUIRE(result->result().matched_count() == 2);
        }

        SECTION("unacknowledged write concern returns disengaged optional") {
            if (test_util::get_max_wire_version(mongodb_client) > 13) {
                WARN("Skipping - getLastError removed in SERVER-57390");
                return;
            }
            collection coll = db["update_many_unack_write"];
            coll.drop();

            coll.insert_one(b1.view());
            coll.insert_one(b1.view());
            options::update opts{};
            opts.write_concern(noack);

            auto result = coll.update_many(b1.view(), update_doc.view(), opts);
            REQUIRE(!result);

            // Block until server has received the write request, to prevent
            // this unacknowledged write from racing with writes to this
            // collection from other sections.
            db.run_command(make_document(kvp("getLastError", 1)));
        }

        SECTION("bypass_document_validation ignores validation_criteria", "[collection]") {
            std::string collname = "update_many_bypass_document_validation";
            db[collname].drop();
            collection coll = db.create_collection(
                collname,
                make_document(kvp(
                    "validator", make_document(kvp("changed", make_document(kvp("$eq", false)))))));

            options::update options;
            options.bypass_document_validation(true);

            auto doc = make_document(kvp("x", 1), kvp("changed", false));

            coll.insert_one(doc.view());
            coll.insert_one(doc.view());

            stdx::optional<result::update> result;
            REQUIRE_NOTHROW(result = coll.update_many(doc.view(), update_doc.view(), options));

            REQUIRE(result);
            REQUIRE(result->result().matched_count() == 2);
        }
    }

    SECTION("update_many with collation", "[collection]") {
        collection coll = db["update_many_with_collation"];
        coll.drop();
        auto b = make_document(kvp("x", "foo"));
        REQUIRE(coll.insert_one(b.view()));

        auto predicate = make_document(kvp("x", "FOO"));

        bsoncxx::builder::basic::document update_doc;
        update_doc.append(kvp("$set", make_document(kvp("changed", true))));

        auto update_opts = options::update{}.collation(case_insensitive_collation.view());
        if (test_util::supports_collation(mongodb_client)) {
            INFO("unacknowledged write concern fails");
            update_opts.write_concern(noack);
            REQUIRE_THROWS_AS(coll.update_many(predicate.view(), update_doc.view(), update_opts),
                              operation_exception);

            INFO("default write concern succeeds");
            update_opts.write_concern(default_wc);
            auto result = coll.update_many(predicate.view(), update_doc.view(), update_opts);
            REQUIRE(result);
            REQUIRE(result->modified_count() == 1);

        } else {
            REQUIRE_THROWS_AS(coll.update_many(predicate.view(), update_doc.view(), update_opts),
                              bulk_write_exception);
        }
    }

    SECTION("replace document replaces only one document", "[collection]") {
        collection coll = db["replace_one_only_one"];
        coll.drop();

        auto doc = make_document(kvp("x", 1));

        coll.insert_one(doc.view());
        coll.insert_one(doc.view());

        REQUIRE(coll.count_documents(doc.view()) == 2);

        auto replacement = make_document(kvp("x", 2));

        coll.replace_one(doc.view(), replacement.view());
        REQUIRE(coll.count_documents(doc.view()) == 1);
        REQUIRE(coll.count_documents(replacement.view()) == 1);
    }

    SECTION("non-matching upsert creates document", "[collection]") {
        collection coll = db["non_match_upsert_creates_doc"];
        coll.drop();
        document b1;
        b1.append(kvp("_id", 1));

        bsoncxx::builder::basic::document update_doc;
        update_doc.append(kvp("$set", make_document(kvp("changed", true))));

        options::update options;
        options.upsert(true);

        auto result = coll.update_one(b1.view(), update_doc.view(), options);
        REQUIRE(result->upserted_id());

        auto updated = coll.find_one({});

        REQUIRE(updated);
        REQUIRE(updated->view()["changed"].get_bool() == true);
        REQUIRE(coll.count_documents({}) == (std::int64_t)1);
    }

    SECTION("matching upsert updates document", "[collection]") {
        collection coll = db["match_upsert_updates_doc"];
        coll.drop();

        auto b1 = make_document(kvp("_id", 1));

        coll.insert_one(b1.view());

        bsoncxx::builder::basic::document update_doc;
        update_doc.append(kvp("$set", make_document(kvp("changed", true))));

        options::update options;
        options.upsert(true);

        auto result = coll.update_one(b1.view(), update_doc.view(), options);
        REQUIRE(!(result->upserted_id()));

        auto updated = coll.find_one({});

        REQUIRE(updated);
        REQUIRE(updated->view()["changed"].get_bool() == true);
        REQUIRE(coll.count_documents({}) == 1);
    }

    SECTION("count with hint", "[collection]") {
        collection coll = db["count_with_hint"];
        coll.drop();
        options::count count_opts;
        count_opts.hint(hint{"index_doesnt_exist"});

        auto doc = make_document(kvp("x", 1));
        coll.insert_one(doc.view());

        REQUIRE_THROWS_AS(coll.count_documents(doc.view(), count_opts), operation_exception);
    }

    SECTION("count with collation", "[collection]") {
        collection coll = db["count_with_collation"];
        coll.drop();
        auto doc = make_document(kvp("x", "foo"));
        REQUIRE(coll.insert_one(doc.view()));

        auto predicate = make_document(kvp("x", "FOO"));
        auto count_opts = options::count{}.collation(case_insensitive_collation.view());
        if (test_util::supports_collation(mongodb_client)) {
            REQUIRE(coll.count_documents(predicate.view(), count_opts) == 1);
        } else {
            REQUIRE_THROWS_AS(coll.count_documents(predicate.view(), count_opts), query_exception);
        }
    }

    SECTION("replace_one returns correct result object", "[collection]") {
        auto b1 = make_document(kvp("x", 1));
        auto b2 = make_document(kvp("x", 2));

        SECTION("default write concern returns result") {
            collection coll = db["replace_one_default_write"];
            coll.drop();

            coll.insert_one(b1.view());

            auto result = coll.replace_one(b1.view(), b2.view());
            REQUIRE(result);
            REQUIRE(result->result().matched_count() == 1);
        }

        SECTION("unacknowledged write concern returns disengaged optional") {
            if (test_util::get_max_wire_version(mongodb_client) > 13) {
                WARN("Skipping - getLastError removed in SERVER-57390");
                return;
            }
            collection coll = db["replace_one_unack_write"];
            coll.drop();

            coll.insert_one(b1.view());
            options::replace opts{};
            opts.write_concern(noack);

            auto result = coll.replace_one(b1.view(), b2.view(), opts);
            REQUIRE(!result);

            // Block until server has received the write request, to prevent
            // this unacknowledged write from racing with writes to this
            // collection from other sections.
            db.run_command(make_document(kvp("getLastError", 1)));
        }

        SECTION("bypass_document_validation ignores validation_criteria", "[collection]") {
            std::string collname = "replace_one_bypass_document_validation";
            db[collname].drop();
            collection coll = db.create_collection(
                collname,
                make_document(
                    kvp("validator", make_document(kvp("x", make_document(kvp("$eq", 1)))))));

            options::replace options;
            options.bypass_document_validation(true);

            coll.insert_one(b1.view());

            stdx::optional<result::replace_one> result;
            REQUIRE_NOTHROW(result = coll.replace_one(b1.view(), b2.view(), options));
            REQUIRE(result);
            REQUIRE(result->result().matched_count() == 1);
        }
    }

    SECTION("replace_one with collation", "[collection]") {
        collection coll = db["replace_one_with_collation"];
        coll.drop();
        document doc;
        doc.append(kvp("x", "foo"));
        REQUIRE(coll.insert_one(doc.view()));

        document predicate;
        predicate.append(kvp("x", "FOO"));

        document replacement_doc;
        replacement_doc.append(kvp("x", "bar"));

        auto replace_opts = options::replace{}.collation(case_insensitive_collation.view());
        if (test_util::supports_collation(mongodb_client)) {
            INFO("unacknowledged write concern fails");
            replace_opts.write_concern(noack);
            REQUIRE_THROWS_AS(
                coll.replace_one(predicate.view(), replacement_doc.view(), replace_opts),
                operation_exception);

            INFO("default write concern succeeds");
            replace_opts.write_concern(default_wc);
            auto result = coll.replace_one(predicate.view(), replacement_doc.view(), replace_opts);
            REQUIRE(result);
            REQUIRE(result->modified_count() == 1);

        } else {
            REQUIRE_THROWS_AS(
                coll.replace_one(predicate.view(), replacement_doc.view(), replace_opts),
                bulk_write_exception);
        }
    }

    SECTION("filtered document delete one works", "[collection]") {
        collection coll = db["filtered_doc_delete_one"];
        coll.drop();

        auto b1 = make_document(kvp("x", 1));

        coll.insert_one(b1.view());

        auto b2 = make_document(kvp("x", 2));

        coll.insert_one(b2.view());
        coll.insert_one(b2.view());

        REQUIRE(coll.count_documents({}) == 3);

        coll.delete_one(b2.view());

        REQUIRE(coll.count_documents({}) == (std::int64_t)2);

        auto cursor = coll.find({});

        std::int32_t seen = 0;
        for (auto&& x : cursor) {
            seen |= x["x"].get_int32();
        }

        REQUIRE(seen == 3);

        coll.delete_one(b2.view());

        REQUIRE(coll.count_documents({}) == 1);

        cursor = coll.find({});

        seen = 0;
        for (auto&& x : cursor) {
            seen |= x["x"].get_int32();
        }

        REQUIRE(seen == 1);

        coll.delete_one(b2.view());

        REQUIRE(coll.count_documents({}) == 1);

        cursor = coll.find({});

        seen = 0;
        for (auto&& x : cursor) {
            seen |= x["x"].get_int32();
        }

        REQUIRE(seen == 1);
    }

    SECTION("delete_one returns correct result object", "[collection]") {
        auto b1 = make_document(kvp("x", 1));

        SECTION("default write concern returns result") {
            collection coll = db["delete_one_default_write"];
            coll.drop();

            coll.insert_one(b1.view());

            auto result = coll.delete_one(b1.view());
            REQUIRE(result);
            REQUIRE(result->result().deleted_count() == 1);
        }

        SECTION("unacknowledged write concern returns disengaged optional") {
            if (test_util::get_max_wire_version(mongodb_client) > 13) {
                WARN("Skipping - getLastError removed in SERVER-57390");
                return;
            }
            collection coll = db["delete_one_unack_write"];
            coll.drop();

            coll.insert_one(b1.view());
            options::delete_options opts{};
            opts.write_concern(noack);

            auto result = coll.delete_one(b1.view(), opts);
            REQUIRE(!result);

            // Block until server has received the write request, to prevent
            // this unacknowledged write from racing with writes to this
            // collection from other sections.
            db.run_command(make_document(kvp("getLastError", 1)));
        }
    }

    SECTION("delete_one with collation", "[collection]") {
        collection coll = db["delete_one_with_collation"];
        coll.drop();
        auto b1 = make_document(kvp("x", "foo"));

        REQUIRE(coll.insert_one(b1.view()));

        auto predicate = make_document(kvp("x", "FOO"));

        auto delete_opts = options::delete_options{}.collation(case_insensitive_collation.view());
        if (test_util::supports_collation(mongodb_client)) {
            INFO("unacknowledged write concern fails");
            delete_opts.write_concern(noack);
            REQUIRE_THROWS_AS(coll.delete_one(predicate.view(), delete_opts), operation_exception);

            INFO("default write concern succeeds");
            delete_opts.write_concern(default_wc);
            auto result = coll.delete_one(predicate.view(), delete_opts);
            REQUIRE(result);
            REQUIRE(result->deleted_count() == 1);
        } else {
            REQUIRE_THROWS_AS(coll.delete_one(predicate.view(), delete_opts), bulk_write_exception);
        }
    }

    SECTION("delete many works", "[collection]") {
        collection coll = db["delete_many"];
        coll.drop();

        auto b1 = make_document(kvp("x", 1));

        coll.insert_one(b1.view());

        auto b2 = make_document(kvp("x", 2));

        coll.insert_one(b2.view());
        coll.insert_one(b2.view());

        REQUIRE(coll.count_documents({}) == 3);

        coll.delete_many(b2.view());

        REQUIRE(coll.count_documents({}) == 1);

        auto cursor = coll.find({});

        std::int32_t seen = 0;
        for (auto&& x : cursor) {
            seen |= x["x"].get_int32();
        }

        REQUIRE(seen == 1);

        coll.delete_many(b2.view());

        REQUIRE(coll.count_documents({}) == 1);

        cursor = coll.find({});

        seen = 0;
        for (auto&& x : cursor) {
            seen |= x["x"].get_int32();
        }

        REQUIRE(seen == 1);
    }

    SECTION("delete_many returns correct result object", "[collection]") {
        auto b1 = make_document(kvp("x", 1));

        SECTION("default write concern returns result") {
            collection coll = db["delete_many_default_write"];
            coll.drop();

            coll.insert_one(b1.view());
            coll.insert_one(b1.view());
            coll.insert_one(b1.view());

            auto result = coll.delete_many(b1.view());
            REQUIRE(result);
            REQUIRE(result->result().deleted_count() > 1);
        }

        SECTION("unacknowledged write concern returns disengaged optional") {
            if (test_util::get_max_wire_version(mongodb_client) > 13) {
                WARN("Skipping - getLastError removed in SERVER-57390");
                return;
            }
            collection coll = db["delete_many_unack_write"];
            coll.drop();

            coll.insert_one(b1.view());
            coll.insert_one(b1.view());
            coll.insert_one(b1.view());
            options::delete_options opts{};
            opts.write_concern(noack);

            auto result = coll.delete_many(b1.view(), opts);
            REQUIRE(!result);

            // Block until server has received the write request, to prevent
            // this unacknowledged write from racing with writes to this
            // collection from other sections.
            db.run_command(make_document(kvp("getLastError", 1)));
        }
    }

    SECTION("delete_many with collation", "[collection]") {
        collection coll = db["delete_many_with_collation"];
        coll.drop();
        auto b1 = make_document(kvp("x", "foo"));

        REQUIRE(coll.insert_one(b1.view()));

        auto predicate = make_document(kvp("x", "FOO"));

        auto delete_opts = options::delete_options{}.collation(case_insensitive_collation.view());
        if (test_util::supports_collation(mongodb_client)) {
            INFO("unacknowledged write concern fails");
            delete_opts.write_concern(noack);
            REQUIRE_THROWS_AS(coll.delete_many(predicate.view(), delete_opts), operation_exception);

            INFO("default write concern succeeds");
            delete_opts.write_concern(default_wc);
            auto result = coll.delete_many(predicate.view(), delete_opts);
            REQUIRE(result);
            REQUIRE(result->deleted_count() == 1);
        } else {
            REQUIRE_THROWS_AS(coll.delete_many(predicate.view(), delete_opts),
                              bulk_write_exception);
        }
    }

    SECTION("find works with sort", "[collection]") {
        collection coll = db["find_with_sort"];
        coll.drop();

        auto b1 = make_document(kvp("x", 1));
        auto b2 = make_document(kvp("x", 2));
        auto b3 = make_document(kvp("x", 3));

        coll.insert_one(b1.view());
        coll.insert_one(b3.view());
        coll.insert_one(b2.view());

        SECTION("sort ascending") {
            auto sort = make_document(kvp("x", 1));
            options::find opts{};
            opts.sort(sort.view());

            auto cursor = coll.find({}, opts);

            std::int32_t x = 1;
            for (auto&& doc : cursor) {
                REQUIRE(x == doc["x"].get_int32());
                x++;
            }
        }

        SECTION("sort descending") {
            auto sort = make_document(kvp("x", -1));
            options::find opts{};
            opts.sort(sort.view());

            auto cursor = coll.find({}, opts);

            std::int32_t x = 3;
            for (auto&& doc : cursor) {
                REQUIRE(x == doc["x"].get_int32());
                x--;
            }
        }
    }

    SECTION("find_one_and_replace works", "[collection]") {
        auto b1 = make_document(kvp("x", "foo"));

        auto criteria = make_document(kvp("x", "foo"));
        auto replacement = make_document(kvp("x", "bar"));

        SECTION("without return replacement returns original") {
            collection coll = db["find_one_and_replace_no_return"];
            coll.drop();

            coll.insert_one(b1.view());
            coll.insert_one(b1.view());

            REQUIRE(coll.count_documents({}) == 2);

            auto doc = coll.find_one_and_replace(criteria.view(), replacement.view());
            REQUIRE(doc);
            REQUIRE(doc->view()["x"].get_string().value == stdx::string_view{"foo"});
        }

        SECTION("with return replacement returns new") {
            collection coll = db["find_one_and_replace_return"];
            coll.drop();

            coll.insert_one(b1.view());
            coll.insert_one(b1.view());

            REQUIRE(coll.count_documents({}) == 2);

            options::find_one_and_replace options;
            options.return_document(options::return_document::k_after);
            auto doc = coll.find_one_and_replace(criteria.view(), replacement.view(), options);
            REQUIRE(doc);
            REQUIRE(doc->view()["x"].get_string().value == stdx::string_view{"bar"});
        }

        SECTION("with collation") {
            collection coll = db["find_one_and_replace_with_collation"];
            coll.drop();

            coll.insert_one(b1.view());
            coll.insert_one(b1.view());

            REQUIRE(coll.count_documents({}) == 2);

            options::find_one_and_replace options;
            options.collation(case_insensitive_collation.view());

            auto collation_criteria = make_document(kvp("x", "FOO"));

            if (test_util::supports_collation(mongodb_client)) {
                INFO("unacknowledged write concern fails");
                options.write_concern(noack);
                REQUIRE_THROWS_AS(coll.find_one_and_replace(
                                      collation_criteria.view(), replacement.view(), options),
                                  logic_error);

                INFO("default write concern succeeds");
                options.write_concern(default_wc);
                auto doc = coll.find_one_and_replace(
                    collation_criteria.view(), replacement.view(), options);
                REQUIRE(doc);
                REQUIRE(doc->view()["x"].get_string().value == stdx::string_view{"foo"});
            } else {
                REQUIRE_THROWS_AS(coll.find_one_and_replace(
                                      collation_criteria.view(), replacement.view(), options),
                                  write_exception);
            }
        }

        SECTION("bad criteria returns negative optional") {
            collection coll = db["find_one_and_replace_bad_criteria"];
            coll.drop();

            coll.insert_one(b1.view());
            coll.insert_one(b1.view());

            REQUIRE(coll.count_documents({}) == 2);

            auto bad_criteria = make_document(kvp("x", "baz"));

            auto doc = coll.find_one_and_replace(bad_criteria.view(), replacement.view());

            REQUIRE(!doc);
        }

        SECTION("bypass_document_validation ignores validation_criteria", "[collection]") {
            std::string collname = "find_one_and_replace_bypass_document_validation";
            db[collname].drop();
            collection coll = db.create_collection(
                collname,
                make_document(
                    kvp("validator", make_document(kvp("x", make_document(kvp("$eq", "foo")))))));

            coll.insert_one(b1.view());

            options::find_one_and_replace options;
            options.return_document(options::return_document::k_after);
            options.bypass_document_validation(true);

            stdx::optional<bsoncxx::document::value> doc;
            REQUIRE_NOTHROW(
                doc = coll.find_one_and_replace(criteria.view(), replacement.view(), options));
            REQUIRE(doc);
            REQUIRE(doc->view()["x"].get_string().value == stdx::string_view{"bar"});
        }
    }

    SECTION("find_one_and_update works", "[collection]") {
        auto b1 = make_document(kvp("x", "foo"));
        auto criteria = make_document(kvp("x", "foo"));
        auto update = make_document(kvp("$set", make_document(kvp("x", "bar"))));

        SECTION("without return update returns original") {
            collection coll = db["find_one_and_update_no_return"];
            coll.drop();

            coll.insert_one(b1.view());
            coll.insert_one(b1.view());

            REQUIRE(coll.count_documents({}) == 2);

            auto doc = coll.find_one_and_update(criteria.view(), update.view());

            REQUIRE(doc);

            REQUIRE(doc->view()["x"].get_string().value == stdx::string_view{"foo"});
        }

        SECTION("with return update returns new") {
            collection coll = db["find_one_and_update_return"];
            coll.drop();

            coll.insert_one(b1.view());
            coll.insert_one(b1.view());

            REQUIRE(coll.count_documents({}) == 2);

            options::find_one_and_update options;
            options.return_document(options::return_document::k_after);
            auto doc = coll.find_one_and_update(criteria.view(), update.view(), options);
            REQUIRE(doc);
            REQUIRE(doc->view()["x"].get_string().value == stdx::string_view{"bar"});
        }

        SECTION("with collation") {
            collection coll = db["find_one_and_update_with collation"];
            coll.drop();

            coll.insert_one(b1.view());
            coll.insert_one(b1.view());

            REQUIRE(coll.count_documents({}) == 2);

            options::find_one_and_update options;
            options.collation(case_insensitive_collation.view());

            auto collation_criteria = make_document(kvp("x", "FOO"));

            if (test_util::supports_collation(mongodb_client)) {
                INFO("unacknowledged write concern fails");
                options.write_concern(noack);
                REQUIRE_THROWS_AS(
                    coll.find_one_and_update(collation_criteria.view(), update.view(), options),
                    logic_error);

                INFO("default write concern succeeds");
                options.write_concern(default_wc);
                auto doc =
                    coll.find_one_and_update(collation_criteria.view(), update.view(), options);
                REQUIRE(doc);
                REQUIRE(doc->view()["x"].get_string().value == stdx::string_view{"foo"});

            } else {
                REQUIRE_THROWS_AS(
                    coll.find_one_and_update(collation_criteria.view(), update.view(), options),
                    write_exception);
            }
        }

        SECTION("bad criteria returns negative optional") {
            collection coll = db["find_one_and_update_bad_criteria"];
            coll.drop();

            coll.insert_one(b1.view());
            coll.insert_one(b1.view());

            REQUIRE(coll.count_documents({}) == 2);

            auto bad_criteria = make_document(kvp("x", "baz"));

            auto doc = coll.find_one_and_update(bad_criteria.view(), update.view());

            REQUIRE(!doc);
        }

        SECTION("bypass_document_validation ignores validation_criteria", "[collection]") {
            std::string collname = "find_one_and_update_bypass_document_validation";
            db[collname].drop();
            collection coll = db.create_collection(
                collname,
                make_document(
                    kvp("validator", make_document(kvp("x", make_document(kvp("$eq", "foo")))))));

            coll.insert_one(b1.view());

            options::find_one_and_update options;
            options.return_document(options::return_document::k_after);
            options.bypass_document_validation(true);

            stdx::optional<bsoncxx::document::value> doc;
            REQUIRE_NOTHROW(doc =
                                coll.find_one_and_update(criteria.view(), update.view(), options));
            REQUIRE(doc);
            REQUIRE(doc->view()["x"].get_string().value == stdx::string_view{"bar"});
        }
    }

    SECTION("find_one_and_delete works", "[collection]") {
        auto b1 = make_document(kvp("x", "foo"));
        auto criteria = make_document(kvp("x", "foo"));

        SECTION("delete one deletes one and returns it") {
            collection coll = db["find_one_and_delete_one"];
            coll.drop();

            coll.insert_one(b1.view());
            coll.insert_one(b1.view());

            REQUIRE(coll.count_documents({}) == 2);

            auto doc = coll.find_one_and_delete(criteria.view());

            REQUIRE(doc);

            REQUIRE(doc->view()["x"].get_string().value == stdx::string_view{"foo"});
            REQUIRE(coll.count_documents({}) == 1);
        }

        SECTION("with collation") {
            collection coll = db["find_one_and_delete_with_collation"];
            coll.drop();

            coll.insert_one(b1.view());
            coll.insert_one(b1.view());

            REQUIRE(coll.count_documents({}) == 2);

            options::find_one_and_delete options;
            options.collation(case_insensitive_collation.view());

            auto collation_criteria = make_document(kvp("x", "FOO"));

            if (test_util::supports_collation(mongodb_client)) {
                INFO("unacknowledged write concern fails");
                options.write_concern(noack);
                REQUIRE_THROWS_AS(coll.find_one_and_delete(collation_criteria.view(), options),
                                  logic_error);

                INFO("default write concern succeeds");
                options.write_concern(default_wc);
                auto doc = coll.find_one_and_delete(collation_criteria.view(), options);
                REQUIRE(doc);
                REQUIRE(doc->view()["x"].get_string().value == stdx::string_view{"foo"});

            } else {
                REQUIRE_THROWS_AS(coll.find_one_and_delete(collation_criteria.view(), options),
                                  write_exception);
            }
        }
    }

    SECTION("aggregation", "[collection]") {
        pipeline pipeline;

        auto get_results = [](cursor&& cursor) {
            std::vector<bsoncxx::document::value> results;
            std::transform(cursor.begin(),
                           cursor.end(),
                           std::back_inserter(results),
                           [](bsoncxx::document::view v) { return bsoncxx::document::value{v}; });
            return results;
        };

        SECTION("add_fields") {
            collection coll = db["aggregation_add_fields"];
            coll.drop();

            coll.insert_one({});

            pipeline.add_fields(make_document(kvp("x", 1)));
            auto cursor = coll.aggregate(pipeline);

            if (test_util::get_max_wire_version(mongodb_client) >= 5) {
                // The server supports add_fields().
                auto results = get_results(std::move(cursor));
                REQUIRE(results.size() == 1);
                REQUIRE(results[0].view()["x"].get_int32() == 1);
            } else {
                // The server does not support add_fields().
                REQUIRE_THROWS_AS(get_results(std::move(cursor)), operation_exception);
            }
        }

        SECTION("bucket") {
            collection coll = db["aggregation_bucket"];
            coll.drop();

            coll.insert_one(make_document(kvp("x", 1)));
            coll.insert_one(make_document(kvp("x", 3)));
            coll.insert_one(make_document(kvp("x", 5)));

            pipeline.bucket(
                make_document(kvp("groupBy", "$x"), kvp("boundaries", make_array(0, 2, 6))));
            auto cursor = coll.aggregate(pipeline);

            if (test_util::get_max_wire_version(mongodb_client) >= 5) {
                // The server supports bucket().
                auto results = get_results(std::move(cursor));
                REQUIRE(results.size() == 2);

                REQUIRE(results[0].view()["_id"].get_int32() == 0);
                REQUIRE(results[0].view()["count"].get_int32() == 1);

                REQUIRE(results[1].view()["_id"].get_int32() == 2);
                REQUIRE(results[1].view()["count"].get_int32() == 2);
            } else {
                // The server does not support bucket().
                REQUIRE_THROWS_AS(get_results(std::move(cursor)), operation_exception);
            }
        }

        SECTION("bucket_auto") {
            collection coll = db["aggregation_bucket_auto"];
            coll.drop();

            coll.insert_one(make_document(kvp("x", 1)));
            coll.insert_one(make_document(kvp("x", 3)));
            coll.insert_one(make_document(kvp("x", 5)));

            pipeline.bucket_auto(make_document(kvp("groupBy", "$x"), kvp("buckets", 2)));
            auto cursor = coll.aggregate(pipeline);

            if (test_util::get_max_wire_version(mongodb_client) >= 5) {
                // The server supports bucket_auto().

                auto results = get_results(std::move(cursor));
                REQUIRE(results.size() == 2);
                // We check that the "count" field exists here, but we don't assert the exact count,
                // since the server doesn't guarantee what the exact boundaries (and thus the exact
                // counts) will be.
                REQUIRE(results[0].view()["count"]);
                REQUIRE(results[1].view()["count"]);
            } else {
                // The server does not support bucket_auto().
                REQUIRE_THROWS_AS(get_results(std::move(cursor)), operation_exception);
            }
        }

        SECTION("coll_stats") {
            collection coll = db["aggregation_coll_stats"];
            coll.drop();

            coll.insert_one(make_document(kvp("x", 1)));

            pipeline.coll_stats(make_document(kvp("latencyStats", make_document())));
            auto cursor = coll.aggregate(pipeline);

            if (test_util::get_max_wire_version(mongodb_client) >= 5) {
                // The server supports coll_stats().
                auto results = get_results(std::move(cursor));
                REQUIRE(results.size() == 1);
                REQUIRE(results[0].view()["ns"]);
                REQUIRE(results[0].view()["latencyStats"]);
            } else {
                // The server does not support coll_stats().
                REQUIRE_THROWS_AS(get_results(std::move(cursor)), operation_exception);
            }
        }

        SECTION("count") {
            collection coll = db["aggregation_count"];
            coll.drop();

            coll.insert_one({});
            coll.insert_one({});
            coll.insert_one({});

            pipeline.count("foo");
            auto cursor = coll.aggregate(pipeline);

            if (test_util::get_max_wire_version(mongodb_client) >= 5) {
                // The server supports count().
                auto results = get_results(std::move(cursor));
                REQUIRE(results.size() == 1);
                REQUIRE(results[0].view()["foo"].get_int32() == 3);
            } else {
                // The server does not support count().
                REQUIRE_THROWS_AS(get_results(std::move(cursor)), operation_exception);
            }
        }

        SECTION("facet") {
            collection coll = db["aggregation_facet"];
            coll.drop();

            coll.insert_one(make_document(kvp("x", 1)));
            coll.insert_one(make_document(kvp("x", 2)));
            coll.insert_one(make_document(kvp("x", 3)));

            pipeline.facet(make_document(kvp("foo", make_array(make_document(kvp("$limit", 2))))));
            auto cursor = coll.aggregate(pipeline);

            if (test_util::get_max_wire_version(mongodb_client) >= 5) {
                // The server supports facet().
                auto results = get_results(std::move(cursor));
                REQUIRE(results.size() == 1);
                auto foo_array = results[0].view()["foo"].get_array().value;
                REQUIRE(std::distance(foo_array.begin(), foo_array.end()) == 2);
            } else {
                // The server does not support facet().
                REQUIRE_THROWS_AS(get_results(std::move(cursor)), operation_exception);
            }
        }

        SECTION("geo_near") {
            collection coll = db["aggregation_geo_near"];
            coll.drop();

            coll.insert_one(make_document(kvp("_id", 0), kvp("x", make_array(0, 0))));
            coll.insert_one(make_document(kvp("_id", 1), kvp("x", make_array(1, 1))));
            coll.create_index(make_document(kvp("x", "2d")));

            pipeline.geo_near(
                make_document(kvp("near", make_array(0, 0)), kvp("distanceField", "d")));
            auto cursor = coll.aggregate(pipeline);

            auto results = get_results(std::move(cursor));
            REQUIRE(results.size() == 2);
            REQUIRE(results[0].view()["d"]);
            REQUIRE(results[0].view()["_id"].get_int32() == 0);
            REQUIRE(results[1].view()["d"]);
            REQUIRE(results[1].view()["_id"].get_int32() == 1);
        }

        SECTION("graph_lookup") {
            collection coll = db["aggregation_graph_lookup"];
            coll.drop();

            coll.insert_one(make_document(kvp("x", "bar")));
            coll.insert_one(make_document(kvp("x", "foo"), kvp("y", "bar")));

            pipeline.graph_lookup(make_document(kvp("from", coll.name()),
                                                kvp("startWith", "$y"),
                                                kvp("connectFromField", "y"),
                                                kvp("connectToField", "x"),
                                                kvp("as", "z")));
            // Add a sort to the pipeline, so below tests can make assumptions about result order.
            pipeline.sort(make_document(kvp("x", 1)));
            auto cursor = coll.aggregate(pipeline);

            if (test_util::get_max_wire_version(mongodb_client) >= 5) {
                // The server supports graph_lookup().
                auto results = get_results(std::move(cursor));
                REQUIRE(results.size() == 2);
                REQUIRE(results[0].view()["z"].get_array().value.empty());
                REQUIRE(!results[1].view()["z"].get_array().value.empty());
            } else {
                // The server does not support graph_lookup().
                REQUIRE_THROWS_AS(get_results(std::move(cursor)), operation_exception);
            }
        }

        SECTION("group") {
            collection coll = db["aggregation_group"];
            coll.drop();

            coll.insert_one(make_document(kvp("x", 1)));
            coll.insert_one(make_document(kvp("x", 1)));
            coll.insert_one(make_document(kvp("x", 2)));

            pipeline.group(make_document(kvp("_id", "$x")));
            // Add a sort to the pipeline, so below tests can make assumptions about result order.
            pipeline.sort(make_document(kvp("_id", 1)));
            auto cursor = coll.aggregate(pipeline);

            auto results = get_results(std::move(cursor));
            REQUIRE(results.size() == 2);
            REQUIRE(results[0].view()["_id"].get_int32() == 1);
            REQUIRE(results[1].view()["_id"].get_int32() == 2);
        }

        SECTION("index_stats") {
            collection coll = db["aggregation_index_stats"];
            coll.drop();

            coll.create_index(make_document(kvp("a", 1)));
            coll.create_index(make_document(kvp("b", 1)));
            coll.create_index(make_document(kvp("c", 1)));

            pipeline.index_stats();
            auto cursor = coll.aggregate(pipeline);

            if (test_util::get_max_wire_version(mongodb_client) >= 4) {
                // The server supports index_stats().
                auto results = get_results(std::move(cursor));
                REQUIRE(results.size() == 4);
            } else {
                // The server does not support index_stats().
                REQUIRE_THROWS_AS(get_results(std::move(cursor)), operation_exception);
            }
        }

        SECTION("limit") {
            collection coll = db["aggregation_limit"];
            coll.drop();

            coll.insert_one(make_document(kvp("x", 1)));
            coll.insert_one(make_document(kvp("x", 2)));
            coll.insert_one(make_document(kvp("x", 3)));

            // Add a sort to the pipeline, so below tests can make assumptions about result order.
            pipeline.sort(make_document(kvp("x", 1)));
            pipeline.limit(2);
            auto cursor = coll.aggregate(pipeline);

            auto results = get_results(std::move(cursor));
            REQUIRE(results.size() == 2);
            REQUIRE(results[0].view()["x"].get_int32() == 1);
            REQUIRE(results[1].view()["x"].get_int32() == 2);
        }

        SECTION("lookup") {
            collection coll = db["aggregation_lookup"];
            coll.drop();

            coll.insert_one(make_document(kvp("x", 0)));
            coll.insert_one(make_document(kvp("x", 1), kvp("y", 0)));

            pipeline.lookup(make_document(kvp("from", coll.name()),
                                          kvp("localField", "x"),
                                          kvp("foreignField", "y"),
                                          kvp("as", "z")));
            // Add a sort to the pipeline, so below tests can make assumptions about result order.
            pipeline.sort(make_document(kvp("x", 1)));
            auto cursor = coll.aggregate(pipeline);

            if (test_util::get_max_wire_version(mongodb_client) >= 4) {
                // The server supports lookup().
                auto results = get_results(std::move(cursor));
                REQUIRE(results.size() == 2);
                REQUIRE(!results[0].view()["z"].get_array().value.empty());
                REQUIRE(results[1].view()["z"].get_array().value.empty());
            } else {
                // The server does not support lookup().
                REQUIRE_THROWS_AS(get_results(std::move(cursor)), operation_exception);
            }
        }

        SECTION("match") {
            collection coll = db["aggregation_match"];
            coll.drop();

            coll.insert_one(make_document(kvp("x", 1)));
            coll.insert_one(make_document(kvp("x", 1)));
            coll.insert_one(make_document(kvp("x", 2)));

            pipeline.match(make_document(kvp("x", 1)));
            auto cursor = coll.aggregate(pipeline);

            auto results = get_results(std::move(cursor));
            REQUIRE(results.size() == 2);
        }

        SECTION("merge") {
            auto merge_version = "4.1.11";
            auto server_version = test_util::get_server_version(mongodb_client);
            if (test_util::compare_versions(server_version, merge_version) < 0) {
                // The server does not support $merge.
                return;
            }

            collection coll = db["aggregation_merge"];
            collection coll_out = db["aggregation_merge_out"];
            coll.drop();
            coll_out.drop();

            coll.insert_one(make_document(kvp("a", 1)));

            pipeline.match(make_document(kvp("a", 1)));
            pipeline.merge(make_document(kvp("into", "aggregation_merge_out")));

            auto cursor = coll.aggregate(pipeline);

            auto results = get_results(std::move(cursor));
            REQUIRE(results.empty());

            auto doc = coll_out.find_one({});
            REQUIRE(doc);
        }

        SECTION("out") {
            collection coll = db["aggregation_out"];
            coll.drop();

            coll.insert_one(make_document(kvp("x", 1), kvp("y", 1)));

            pipeline.project(make_document(kvp("x", 1)));
            pipeline.out(bsoncxx::string::to_string(coll.name()));
            auto cursor = coll.aggregate(pipeline);

            // The server supports out().
            auto results = get_results(std::move(cursor));
            REQUIRE(results.empty());

            auto collection_contents = get_results(coll.find({}));
            REQUIRE(collection_contents.size() == 1);
            REQUIRE(collection_contents[0].view()["x"].get_int32() == 1);
            REQUIRE(!collection_contents[0].view()["y"]);
        }

        SECTION("out with bypass_document_validation", "[collection]") {
            collection coll_in = db["aggregation_out_bypass_document_validation_in"];
            coll_in.drop();
            coll_in.insert_one(make_document(kvp("x", 1), kvp("y", 1)));

            std::string collname = "aggregation_out_bypass_document_validation_out";
            db[collname].drop();
            collection coll_out = db.create_collection(
                collname,
                make_document(
                    kvp("validator", make_document(kvp("x", make_document(kvp("$eq", 2)))))));

            options::aggregate options;
            options.bypass_document_validation(true);

            pipeline.project(make_document(kvp("x", 1)));
            pipeline.out(bsoncxx::string::to_string(coll_out.name()));
            stdx::optional<cursor> cursor;
            REQUIRE_NOTHROW(cursor = coll_in.aggregate(pipeline, options));

            if (test_util::get_max_wire_version(mongodb_client) >= 1) {
                // The server supports out().
                auto results = get_results(std::move(*cursor));
                REQUIRE(results.empty());

                auto collection_contents = get_results(coll_out.find({}));
                REQUIRE(collection_contents.size() == 1);
                REQUIRE(collection_contents[0].view()["x"].get_int32() == 1);
                REQUIRE(!collection_contents[0].view()["y"]);
            } else {
                // The server does not support out().
                REQUIRE_THROWS_AS(get_results(std::move(*cursor)), operation_exception);
            }
        }

        SECTION("out fails when not last") {
            collection coll = db["aggregation_out_fails"];
            coll.insert_one(make_document(kvp("x", 1)));

            pipeline.project(make_document(kvp("x", 1)));
            pipeline.out("aggregation_out_fails");
            pipeline.sample(1);

            auto cursor = coll.aggregate(pipeline);
            REQUIRE_THROWS_AS(get_results(std::move(cursor)), operation_exception);
        }

        SECTION("project") {
            collection coll = db["aggregation_project"];
            coll.drop();

            coll.insert_one(make_document(kvp("x", 1), kvp("y", 1)));

            pipeline.project(make_document(kvp("x", 1)));
            auto cursor = coll.aggregate(pipeline);

            auto results = get_results(std::move(cursor));
            REQUIRE(results.size() == 1);
            REQUIRE(results[0].view()["x"].get_int32() == 1);
            REQUIRE(!results[0].view()["y"]);
        }

        SECTION("redact") {
            collection coll = db["aggregation_redact"];
            coll.drop();

            coll.insert_one(make_document(kvp("x", make_document(kvp("secret", 1))), kvp("y", 1)));

            pipeline.redact(make_document(
                kvp("$cond",
                    make_document(kvp("if", make_document(kvp("$eq", make_array("$secret", 1)))),
                                  kvp("then", "$$PRUNE"),
                                  kvp("else", "$$DESCEND")))));
            auto cursor = coll.aggregate(pipeline);

            // The server supports redact().
            auto results = get_results(std::move(cursor));
            REQUIRE(results.size() == 1);
            REQUIRE(!results[0].view()["x"]);
            REQUIRE(results[0].view()["y"].get_int32() == 1);
        }

        SECTION("replace_root") {
            collection coll = db["aggregation_replace_root"];
            coll.drop();

            coll.insert_one(make_document(kvp("x", make_document(kvp("y", 1)))));

            pipeline.replace_root(make_document(kvp("newRoot", "$x")));
            auto cursor = coll.aggregate(pipeline);

            if (test_util::get_max_wire_version(mongodb_client) >= 5) {
                // The server supports replace_root().
                auto results = get_results(std::move(cursor));
                REQUIRE(results.size() == 1);
                REQUIRE(results[0].view()["y"]);
            } else {
                // The server does not support replace_root().
                REQUIRE_THROWS_AS(get_results(std::move(cursor)), operation_exception);
            }
        }

        SECTION("sample") {
            collection coll = db["aggregation_sample"];
            coll.drop();

            coll.insert_one({});
            coll.insert_one({});
            coll.insert_one({});
            coll.insert_one({});

            pipeline.sample(3);
            auto cursor = coll.aggregate(pipeline);

            if (test_util::get_max_wire_version(mongodb_client) >= 4) {
                // The server supports sample().
                auto results = get_results(std::move(cursor));
                REQUIRE(results.size() == 3);
            } else {
                // The server does not support sample().
                REQUIRE_THROWS_AS(get_results(std::move(cursor)), operation_exception);
            }
        }

        SECTION("skip") {
            collection coll = db["aggregation_skip"];
            coll.drop();

            coll.insert_one(make_document(kvp("x", 1)));
            coll.insert_one(make_document(kvp("x", 2)));
            coll.insert_one(make_document(kvp("x", 3)));

            // Add a sort to the pipeline, so below tests can make assumptions about result order.
            pipeline.sort(make_document(kvp("x", 1)));
            pipeline.skip(1);
            auto cursor = coll.aggregate(pipeline);

            auto results = get_results(std::move(cursor));
            REQUIRE(results.size() == 2);
            REQUIRE(results[0].view()["x"].get_int32() == 2);
            REQUIRE(results[1].view()["x"].get_int32() == 3);
        }

        SECTION("sort") {
            collection coll = db["aggregation_sort"];
            coll.drop();

            coll.insert_one(make_document(kvp("x", 1)));
            coll.insert_one(make_document(kvp("x", 2)));
            coll.insert_one(make_document(kvp("x", 3)));

            pipeline.sort(make_document(kvp("x", -1)));
            auto cursor = coll.aggregate(pipeline);

            auto results = get_results(std::move(cursor));
            REQUIRE(results.size() == 3);
            REQUIRE(results[0].view()["x"].get_int32() == 3);
            REQUIRE(results[1].view()["x"].get_int32() == 2);
            REQUIRE(results[2].view()["x"].get_int32() == 1);
        }

        SECTION("sort_by_count") {
            std::vector<bsoncxx::document::value> inserts{
                make_document(kvp("x", 1)), make_document(kvp("x", 2)), make_document(kvp("x", 2))};

            SECTION("with string") {
                collection coll = db["aggregation_sort_by_count_with_string"];
                coll.drop();

                coll.insert_many(inserts, options::insert{});

                pipeline.sort_by_count("$x");
                auto cursor = coll.aggregate(pipeline);

                if (test_util::get_max_wire_version(mongodb_client) >= 5) {
                    // The server supports sort_by_count().
                    auto results = get_results(std::move(cursor));
                    REQUIRE(results.size() == 2);
                    REQUIRE(results[0].view()["_id"].get_int32() == 2);
                    REQUIRE(results[1].view()["_id"].get_int32() == 1);
                } else {
                    // The server does not support sort_by_count().
                    REQUIRE_THROWS_AS(get_results(std::move(cursor)), operation_exception);
                }
            }

            SECTION("with document") {
                collection coll = db["aggregation_sort_by_count_with_document"];
                coll.drop();

                coll.insert_many(inserts, options::insert{});

                pipeline.sort_by_count(make_document(kvp("$mod", make_array("$x", 2))));
                auto cursor = coll.aggregate(pipeline);

                if (test_util::get_max_wire_version(mongodb_client) >= 5) {
                    // The server supports sort_by_count().
                    auto results = get_results(std::move(cursor));
                    REQUIRE(results.size() == 2);
                    REQUIRE(results[0].view()["_id"].get_int32() == 0);
                    REQUIRE(results[1].view()["_id"].get_int32() == 1);
                } else {
                    // The server does not support sort_by_count().
                    REQUIRE_THROWS_AS(get_results(std::move(cursor)), operation_exception);
                }
            }
        }

        SECTION("unwind with string") {
            collection coll = db["aggregation_unwind_with_string"];
            coll.drop();

            coll.insert_one(make_document(kvp("x", make_array(1, 2, 3, 4, 5))));
            pipeline.unwind("$x");
            auto cursor = coll.aggregate(pipeline);

            auto results = get_results(std::move(cursor));
            REQUIRE(results.size() == 5);
        }

        SECTION("unwind with document") {
            collection coll = db["aggregation_unwind_with_doc"];
            coll.drop();

            coll.insert_one(make_document(kvp("x", make_array(1, 2, 3, 4, 5))));

            pipeline.unwind(make_document(kvp("path", "$x")));
            auto cursor = coll.aggregate(pipeline);

            if (test_util::get_max_wire_version(mongodb_client) >= 4) {
                // The server supports unwind() with a document.
                auto results = get_results(std::move(cursor));
                REQUIRE(results.size() == 5);
            } else {
                // The server does not support unwind() with a document.
                REQUIRE_THROWS_AS(get_results(std::move(cursor)), operation_exception);
            }
        }
    }

    SECTION("aggregation with collation", "[collection]") {
        collection coll = db["aggregation_with_collation"];
        coll.drop();

        auto b1 = make_document(kvp("x", "foo"));

        coll.insert_one(b1.view());

        auto predicate = make_document(kvp("x", "FOO"));

        pipeline p;
        p.match(predicate.view());

        auto agg_opts = options::aggregate{}.collation(case_insensitive_collation.view());
        auto results = coll.aggregate(p, agg_opts);

        if (test_util::supports_collation(mongodb_client)) {
            REQUIRE(std::distance(results.begin(), results.end()) == 1);
        } else {
            // The server does not support collation.
            REQUIRE_THROWS_AS(std::distance(results.begin(), results.end()), operation_exception);
        }
    }

    SECTION("bulk_write returns correct result object") {
        auto doc1 = make_document(kvp("foo", 1));
        auto doc2 = make_document(kvp("foo", 2));

        options::bulk_write bulk_opts;
        bulk_opts.ordered(false);

        SECTION("default write concern returns result") {
            collection coll = db["bulk_write_default_write"];
            coll.drop();

            auto abulk = coll.create_bulk_write(bulk_opts);
            abulk.append(model::insert_one{std::move(doc1)});
            abulk.append(model::insert_one{std::move(doc2)});
            auto result = abulk.execute();

            REQUIRE(result);
            REQUIRE(result->inserted_count() == 2);
        }

        SECTION("unacknowledged write concern returns disengaged optional", "[collection]") {
            if (test_util::get_max_wire_version(mongodb_client) > 13) {
                WARN("Skipping - getLastError removed in SERVER-57390");
                return;
            }
            collection coll = db["bulk_write_unack_write"];
            coll.drop();

            bulk_opts.write_concern(noack);
            auto bbulk = coll.create_bulk_write(bulk_opts);
            bbulk.append(model::insert_one{std::move(doc1)});
            bbulk.append(model::insert_one{std::move(doc2)});
            auto result = bbulk.execute();

            REQUIRE(!result);

            // Block until server has received the write request, to prevent
            // this unacknowledged write from racing with writes to this
            // collection from other sections.
            db.run_command(make_document(kvp("getLastError", 1)));
        }

        SECTION("write wrapper returns correct result") {
            collection coll = db["bulk_write_write_wrapper"];
            coll.drop();

            auto doc3 = make_document(kvp("foo", 3));
            auto result = coll.write(model::insert_one{std::move(doc3)});
            REQUIRE(result);
            REQUIRE(result->inserted_count() == 1);
        }

        SECTION("fail if server has maxWireVersion < 5 and write has collation") {
            if (test_util::get_max_wire_version(mongodb_client) < 5) {
                collection coll = db["bulk_write_collation"];
                coll.drop();

                auto collation =
                    make_document(kvp("collation", make_document(kvp("locale", "en_US"))));

                model::delete_one first{std::move(doc1)};
                model::delete_one second{std::move(doc2)};

                second.collation(collation.view());

                auto bulk = coll.create_bulk_write(bulk_opts);
                bulk.append(first);
                bulk.append(second);

                REQUIRE_THROWS_AS(bulk.execute(), operation_exception);
            }
        }

        SECTION("bypass_document_validation ignores validation_criteria", "[collection]") {
            std::string collname = "bulk_write_bypass_document_validation";
            db[collname].drop();
            collection coll = db.create_collection(
                collname,
                make_document(
                    kvp("validator", make_document(kvp("foo", make_document(kvp("$eq", 1)))))));

            bulk_opts.bypass_document_validation(true);
            auto cbulk = coll.create_bulk_write(bulk_opts);
            cbulk.append(model::insert_one{std::move(doc1)});
            cbulk.append(model::insert_one{std::move(doc2)});

            stdx::optional<result::bulk_write> result;
            REQUIRE_NOTHROW(result = cbulk.execute());

            REQUIRE(result);
            REQUIRE(result->inserted_count() == 2);
        }
    }

    SECTION("distinct works", "[collection]") {
        collection coll = db["distinct"];
        coll.drop();
        auto doc1 = make_document(kvp("foo", "baz"), kvp("garply", 1));
        auto doc2 = make_document(kvp("foo", "bar"), kvp("garply", 2));
        auto doc3 = make_document(kvp("foo", "baz"), kvp("garply", 2));
        auto doc4 = make_document(kvp("foo", "quux"), kvp("garply", 9));

        options::bulk_write bulk_opts;
        bulk_opts.ordered(false);
        auto bulk = coll.create_bulk_write(bulk_opts);

        bulk.append(model::insert_one{std::move(doc1)});
        bulk.append(model::insert_one{std::move(doc2)});
        bulk.append(model::insert_one{std::move(doc3)});
        bulk.append(model::insert_one{std::move(doc4)});

        bulk.execute();

        REQUIRE(coll.count_documents({}) == 4);

        auto distinct_results = coll.distinct("foo", {});

        // copy into a vector.
        std::vector<bsoncxx::document::value> results;
        for (auto&& result : distinct_results) {
            results.emplace_back(result);
        }

        REQUIRE(results.size() == std::size_t{1});

        auto res_doc = results[0].view();
        auto values_array = res_doc["values"].get_array().value;

        std::vector<stdx::string_view> distinct_values;
        for (auto&& value : values_array) {
            distinct_values.push_back(value.get_string().value);
        }

        const auto assert_contains_one = [&](stdx::string_view val) {
            REQUIRE(std::count(distinct_values.begin(), distinct_values.end(), val) == 1);
        };

        assert_contains_one("baz");
        assert_contains_one("bar");
        assert_contains_one("quux");
    }

    SECTION("distinct with collation", "[collection]") {
        collection coll = db["distinct_with_collation"];
        coll.drop();
        auto doc = make_document(kvp("x", "foo"));

        coll.insert_one(doc.view());

        auto predicate = make_document(kvp("x", "FOO"));

        auto distinct_opts = options::distinct{}.collation(case_insensitive_collation.view());

        if (test_util::supports_collation(mongodb_client)) {
            auto distinct_results = coll.distinct("x", predicate.view(), distinct_opts);
            auto iter = distinct_results.begin();
            REQUIRE(iter != distinct_results.end());
            auto result = *iter;
            auto values = result["values"].get_array().value;
            REQUIRE(std::distance(values.begin(), values.end()) == 1);
            REQUIRE(values[0].get_string().value == stdx::string_view{"foo"});
        } else {
            // The server does not support collation.
            REQUIRE_THROWS_AS(coll.distinct("x", predicate.view(), distinct_opts),
                              operation_exception);
        }
    }
}

TEST_CASE("read_concern is inherited from parent", "[collection]") {
    client mongo_client{uri{}, test_util::add_test_server_api()};
    database db = mongo_client["collection_read_concern_inheritance"];

    read_concern::level majority = read_concern::level::k_majority;
    read_concern::level local = read_concern::level::k_local;

    read_concern rc{};
    rc.acknowledge_level(majority);
    db.read_concern(rc);

    SECTION("when parent is a database") {
        collection coll = db["database_parent"];
        REQUIRE(coll.read_concern().acknowledge_level() == read_concern::level::k_majority);
    }

    SECTION("except when read_concern is explicitly set") {
        collection coll = db["explicitly_set"];
        read_concern set_rc{};
        set_rc.acknowledge_level(read_concern::level::k_local);
        coll.read_concern(set_rc);

        REQUIRE(coll.read_concern().acknowledge_level() == local);
    }
}

void find_index_and_validate(collection& coll,
                             stdx::string_view index_name,
                             const std::function<void(bsoncxx::document::view)>& validate =
                                 [](bsoncxx::document::view) {}) {
    auto cursor = coll.list_indexes();

    for (auto&& index : cursor) {
        auto name_ele = index["name"];
        REQUIRE(name_ele);
        REQUIRE(name_ele.type() == bsoncxx::type::k_string);

        if (name_ele.get_string().value != index_name) {
            continue;
        }

        validate(index);
        return;
    }
    REQUIRE(false);  // index of given name not found
}

TEST_CASE("create_index tests", "[collection]") {
    using namespace bsoncxx;

    instance::current();

    client mongodb_client{uri{}, test_util::add_test_server_api()};
    database db = mongodb_client["collection_create_index"];

    SECTION("returns index name") {
        collection coll = db["create_index_return_name"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.

        bsoncxx::document::value index = make_document(kvp("a", 1));

        std::string indexName{"myName"};
        options::index options{};
        options.name(indexName);

        auto response = coll.create_index(index.view(), options);
        REQUIRE(response.view()["name"].get_string().value ==
                bsoncxx::stdx::string_view(indexName));

        find_index_and_validate(coll, indexName);

        bsoncxx::document::value index2 = make_document(kvp("b", 1), kvp("c", -1));

        auto response2 = coll.create_index(index2.view(), options::index{});
        REQUIRE(response2.view()["name"].get_string().value ==
                bsoncxx::stdx::string_view{"b_1_c_-1"});

        find_index_and_validate(coll, "b_1_c_-1");
    }

    SECTION("with collation") {
        collection coll = db["create_index_with_collation"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.

        bsoncxx::document::value keys = make_document(kvp("a", 1));
        auto collation = make_document(kvp("locale", "en_US"));

        options::index options{};
        options.collation(collation.view());

        coll.create_index(keys.view(), options);

        auto validate = [](bsoncxx::document::view index) {
            bsoncxx::types::bson_value::view locale{types::b_string{"en_US"}};
            auto locale_ele = index["collation"]["locale"];
            REQUIRE(locale_ele);
            REQUIRE(locale_ele.type() == type::k_string);
            REQUIRE((locale_ele.get_string() == locale));
        };

        find_index_and_validate(coll, "a_1", validate);
    }

    SECTION("fails") {
        collection coll = db["create_index_fails"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.

        bsoncxx::document::value keys1 = make_document(kvp("a", 1));
        bsoncxx::document::value keys2 = make_document(kvp("a", -1));

        options::index options{};
        options.name("a");

        REQUIRE_NOTHROW(coll.create_index(keys1.view(), options));
        REQUIRE_THROWS_AS(coll.create_index(keys2.view(), options), operation_exception);
    }

    SECTION("succeeds with options") {
        collection coll = db["create_index_with_options"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.

        mongocxx::stdx::string_view index_name{"succeeds_with_options"};

        bsoncxx::document::value keys = make_document(kvp("cccc", 1));

        options::index options{};
        options.unique(true);
        if (test_util::newer_than(mongodb_client, "4.4"))
            options.hidden(true);
        options.expire_after(std::chrono::seconds(500));
        options.name(index_name);

        REQUIRE_NOTHROW(coll.create_index(keys.view(), options));
        auto validate = [&](bsoncxx::document::view index) {
            auto expire_after = index["expireAfterSeconds"];
            REQUIRE(expire_after);
            REQUIRE(expire_after.type() == type::k_int32);
            REQUIRE(expire_after.get_int32().value == 500);

            auto unique_ele = index["unique"];
            REQUIRE(unique_ele);
            REQUIRE(unique_ele.type() == type::k_bool);
            REQUIRE(unique_ele.get_bool() == options.unique().value());

            if (test_util::newer_than(mongodb_client, "4.4")) {
                auto hidden_ele = index["hidden"];
                REQUIRE(hidden_ele);
                REQUIRE(hidden_ele.type() == type::k_bool);
                REQUIRE(hidden_ele.get_bool() == options.hidden().value());
            }
        };

        find_index_and_validate(coll, index_name, validate);
    }

    SECTION("fails with options") {
        collection coll = db["create_index_fails_with_options"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.

        bsoncxx::document::value keys = make_document(kvp("c", 1));
        options::index options{};

        auto expire_after =
            std::chrono::seconds(static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1);
        options.expire_after(expire_after);
        REQUIRE_THROWS_AS(coll.create_index(keys.view(), options), logic_error);

        expire_after = std::chrono::seconds(-1);
        options.expire_after(expire_after);
        REQUIRE_THROWS_AS(coll.create_index(keys.view(), options), logic_error);
    }

    SECTION("succeeds with storage engine options") {
        collection coll = db["create_index_succeeds_with_storage_options"];
        coll.drop();
        coll.insert_one({});  // Ensure that the collection exists.

        bsoncxx::stdx::string_view index_name{"storage_options_test"};
        bsoncxx::document::value keys = make_document(kvp("c", 1));

        options::index options{};
        options.name(index_name);

        std::unique_ptr<options::index::wiredtiger_storage_options> wt_options =
            bsoncxx::stdx::make_unique<options::index::wiredtiger_storage_options>();
        wt_options->config_string("block_allocation=first");

        REQUIRE_NOTHROW(options.storage_options(std::move(wt_options)));
        REQUIRE_NOTHROW(coll.create_index(keys.view(), options));

        auto validate = [](bsoncxx::document::view index) {
            auto config_string_ele = index["storageEngine"]["wiredTiger"]["configString"];
            REQUIRE(config_string_ele);
            REQUIRE(config_string_ele.type() == type::k_string);
            REQUIRE(config_string_ele.get_string() == types::b_string{"block_allocation=first"});
        };

        find_index_and_validate(coll, index_name, validate);
    }
}

TEST_CASE("list_indexes", "[collection]") {
    instance::current();

    client mongodb_client{uri{}, test_util::add_test_server_api()};
    database db = mongodb_client["collection_list_indexes"];

    collection coll = db["list_indexes_works"];
    coll.drop();
    coll.insert_one({});  // Ensure that the collection exists.

    options::index options{};
    options.unique(true);

    coll.create_index(make_document(kvp("a", 1)), options);
    coll.create_index(make_document(kvp("b", 1), kvp("c", -1)));
    coll.create_index(make_document(kvp("c", -1)));

    auto cursor = coll.list_indexes();

    std::vector<std::string> expected_names{"a_1", "b_1_c_-1", "c_-1"};
    std::int8_t found = 0;

    for (auto&& index : cursor) {
        auto name = index["name"].get_string();

        for (auto&& expected : expected_names) {
            if (bsoncxx::stdx::string_view(expected) == name.value) {
                found++;
                if (expected == "a_1") {
                    REQUIRE(index["unique"]);
                } else {
                    REQUIRE(!index["unique"]);
                }
            }
        }
    }
    REQUIRE(found == 3);
}

// We use a capped collection for this test case so we can
// use it with all three cursor types.
TEST_CASE("Cursor iteration", "[collection][cursor]") {
    instance::current();
    client mongodb_client{uri{}, test_util::add_test_server_api()};
    database db = mongodb_client["collection_cursor_iteration"];

    auto capped_name = std::string("mongo_cxx_driver_capped");
    collection coll = db[capped_name];

    // Drop and (re)create the capped collection.
    coll.drop();
    db.create_collection(capped_name, make_document(kvp("capped", true), kvp("size", 1024 * 1024)));

    // Tests will use all three cursor types.
    options::find opts;
    std::string type_str = "no cursor type set";

    auto run_test = [&]() {
        INFO(type_str);

        // Insert 3 documents.
        for (int32_t n : {1, 2, 3}) {
            coll.insert_one(make_document(kvp("x", n)));
        }

        auto cursor = coll.find({}, opts);
        auto iter = cursor.begin();

        REQUIRE(iter == cursor.begin());

        // Check that the cursor finds three documents and that the iterator
        // stays in lockstep.
        auto expected = 1;

        for (auto&& doc : cursor) {
            REQUIRE(doc["x"].get_int32() == expected);

            // Lockstep requires that iter matches both the current document
            // and cursor.begin() (current doc before cursor increment).
            // It must not match cursor.end(), since a document exists.
            REQUIRE(iter == cursor.begin());
            REQUIRE(iter != cursor.end());
            REQUIRE((*iter)["x"].get_int32() == expected);

            expected++;
        }

        // Check that iteration covered all three documents.
        REQUIRE(expected == 4);

        // As no document is available, iterator now must match cursor.end().
        // We check both LHS and RHS for coverage.
        REQUIRE(iter == cursor.end());
        REQUIRE(cursor.end() == iter);

        // Because there are no more documents available from this query,
        // cursor.begin() must equal cursor.end().  Transitively, this means
        // that iter must also match cursor.begin().
        REQUIRE(cursor.begin() == cursor.end());
        REQUIRE(iter == cursor.begin());

        // For tailable cursors, if more documents are inserted, the next
        // call to cursor.begin() should find more documents and the existing iterator
        // should no longer be exhausted.
        if (opts.cursor_type() != cursor::type::k_non_tailable) {
            // Insert 3 more documents.
            for (int32_t n : {4, 5, 6}) {
                coll.insert_one(make_document(kvp("x", n)));
            }

            // More documents are available, but until the next call to
            // cursor.begin(), the existing iterator still appears exhausted.
            REQUIRE(iter == cursor.end());

            // After calling cursor.begin(), the existing iterator is revived.
            cursor.begin();
            REQUIRE(iter != cursor.end());
            REQUIRE(iter == cursor.begin());

            // Check that the cursor finds the next three documents and that the
            // iterator stays in lockstep.
            for (auto&& doc : cursor) {
                REQUIRE(doc["x"].get_int32() == expected);

                REQUIRE(iter == cursor.begin());
                REQUIRE(iter != cursor.end());
                REQUIRE((*iter)["x"].get_int32() == expected);

                expected++;
            }

            // Check that iteration has covered all six documents.
            REQUIRE(expected == 7);

            // As before: iter, cursor.begin() and cursor.end() must all
            // transitively agree that the cursor is currently exhausted.
            REQUIRE(iter == cursor.end());
            REQUIRE(cursor.begin() == cursor.end());
            REQUIRE(iter == cursor.begin());
        }
    };

    SECTION("k_non_tailable") {
        opts.cursor_type(cursor::type::k_non_tailable);
        type_str = "k_non_tailable";
        run_test();
    }

    SECTION("k_tailable") {
        opts.cursor_type(cursor::type::k_tailable);
        type_str = "k_tailable";
        run_test();
    }

    SECTION("k_tailable_await") {
        opts.cursor_type(cursor::type::k_tailable_await);
        type_str = "k_tailable_await";

        // Improve execution time by reducing the amount of time the server waits for new
        // results for this cursor. Note: may cause flaky test failures if the duration is too
        // short.
        opts.max_await_time(std::chrono::milliseconds{10});

        run_test();
    }
}

TEST_CASE("regressions", "CXX-986") {
    instance::current();
    mongocxx::uri mongo_uri{"mongodb://non-existent-host.invalid/"};
    mongocxx::client client{mongo_uri, test_util::add_test_server_api()};
    REQUIRE_THROWS(client.database("irrelevant")["irrelevant"].find_one_and_update(
        make_document(kvp("irrelevant", 1)), make_document(kvp("irrelevant", 2))));
}

TEST_CASE("bulk_write with container", "[collection]") {
    instance::current();
    mongocxx::client client{uri{}, test_util::add_test_server_api()};

    std::vector<model::write> vec;
    for (int32_t i = 0; i != 10; ++i) {
        vec.emplace_back(model::insert_one{make_document(kvp("_id", i))});
    }

    auto collection = client["bulk_write_container"]["collection"];
    collection.drop();
    auto result = collection.bulk_write(vec);
    // The optional result is engaged.
    REQUIRE(static_cast<bool>(result));
    REQUIRE(result->inserted_count() == 10);
    REQUIRE(collection.count_documents({}) == 10);
}

/* Regression test for CXX-2028. */
TEST_CASE("find_and_x operations append write concern correctly", "[collection]") {
    instance::current();
    mongocxx::client client{uri{}, test_util::add_test_server_api()};
    mongocxx::write_concern wc;
    wc.acknowledge_level(mongocxx::write_concern::level::k_acknowledged);

    auto collection = client["fam_wc"]["collection"];
    collection.drop();
    collection.insert_one(make_document(kvp("x", 1)));

    stdx::optional<bsoncxx::document::value> doc;
    /* 4.4. servers will reply with an error, causing an exception. */
    /* find_one_and_update */
    mongocxx::options::find_one_and_update find_one_and_update_opts;
    find_one_and_update_opts.write_concern(wc);
    doc = collection.find_one_and_update(
        {}, make_document(kvp("$set", make_document(kvp("x", 2)))), find_one_and_update_opts);
    REQUIRE(doc);

    /* find_one_and_replace */
    mongocxx::options::find_one_and_replace find_one_and_replace_opts;
    find_one_and_replace_opts.write_concern(wc);
    doc = collection.find_one_and_replace(
        make_document(), make_document(kvp("x", 2)), find_one_and_replace_opts);
    REQUIRE(doc);

    /* find_one_and_delete */
    mongocxx::options::find_one_and_delete find_one_and_delete_opts;
    find_one_and_delete_opts.write_concern(wc);
    doc = collection.find_one_and_delete({}, find_one_and_delete_opts);
    REQUIRE(doc);

    /* < 4.4 servers will not return an error for unexpected fields. Add a visitor function to check
     * manually. */
    bool called = false;
    auto visitor = libmongoc::find_and_modify_opts_append.create_instance();
    visitor->visit([&](mongoc_find_and_modify_opts_t*, const bson_t* extra) {
        bsoncxx::document::value expected =
            make_document(kvp("writeConcern", make_document(kvp("w", 1))));
        bsoncxx::document::view extra_view{bson_get_data(extra), extra->len};

        called = true;
        REQUIRE(extra_view == expected.view());
    });

    /* Insert a new document. */
    collection.insert_one(make_document(kvp("x", 1)));
    doc = collection.find_one_and_delete({}, find_one_and_delete_opts);
    REQUIRE(doc);
    REQUIRE(called);
}

TEST_CASE("Ensure that the WriteConcernError 'errInfo' object is propagated", "[collection]") {
    using namespace bsoncxx;
    instance::current();

    client mongodb_client{uri{}, test_util::add_test_server_api()};

    if (test_util::get_topology(mongodb_client) == "sharded" &&
        test_util::compare_versions(test_util::get_server_version(mongodb_client), "4.1.0") < 0) {
        WARN("Skipping - failCommand on mongos requires 4.1+");
        return;
    }

    using bsoncxx::builder::basic::sub_document;
    auto err_info = builder::basic::document{};
    err_info.append(kvp("writeConcern", [](sub_document sub_doc) {
        sub_doc.append(kvp("w", types::b_int32{2}));
        sub_doc.append(kvp("wtimeout", types::b_int32{0}));
        sub_doc.append(kvp("provenance", "clientSupplied"));
    }));

    auto fail_point = builder::basic::document{};
    fail_point.append(kvp("configureFailPoint", "failCommand"));

    using bsoncxx::builder::basic::sub_array;
    fail_point.append(kvp("data", [&err_info](sub_document sub_doc) {
        sub_doc.append(kvp("failCommands", [](sub_array sub_arr) { sub_arr.append("insert"); }));
        sub_doc.append(kvp("writeConcernError", [&err_info](sub_document sub_doc) {
            sub_doc.append(kvp("code", types::b_int32{100}));
            sub_doc.append(kvp("codeName", "UnsatisfiableWriteConcern"));
            sub_doc.append(kvp("errmsg", "Not enough data-bearing nodes"));
            sub_doc.append(kvp("errInfo", types::b_document{err_info}));
        }));
    }));

    fail_point.append(
        kvp("mode", [](sub_document sub_doc) { sub_doc.append(kvp("times", types::b_int32{1})); }));

    mongodb_client["admin"].run_command(fail_point.view());
    collection coll = mongodb_client["test"]["errInfo"];

    coll.drop();
    auto doc = make_document(kvp("x", types::b_int32{1}));

    bool contains_err_info{false};
    try {
        coll.insert_one(doc.view());
    } catch (const operation_exception& e) {
        auto error = e.raw_server_error()->view();
        auto result = error["writeConcernErrors"][0]["errInfo"];
        contains_err_info = (err_info == result.get_document().view());
    }

    REQUIRE(contains_err_info);
}

TEST_CASE("expose writeErrors[].errInfo", "[collection]") {
    // A helper for checking that an error document is well-formed according to our requirements:
    auto writeErrors_well_formed = [](const bsoncxx::document::view& reply_view) -> bool {
        if (!reply_view["writeErrors"]) {
            return false;
        }

        const auto& errdoc = reply_view["writeErrors"][0];

        auto error_code = errdoc["code"].get_int32();

        // The code should always be 121 (DocumentValidationFailure):
        if (121 != error_code) {
            std::ostringstream os;
            os << "writeErrors expected to have code 121, but had " << error_code << " instead";
            throw std::runtime_error(os.str());
        }

        // We require the "details" field be present:
        if (!errdoc["errInfo"]["details"]) {
            throw std::runtime_error("no \"details\" field in \"writeErrors\"");
        }

        return true;
    };

    // Set up our test environment:
    instance::current();

    mongocxx::options::apm apm_opts;

    auto client_opts = test_util::add_test_server_api();

    // We set this by side effect in on_command_succeeded to make sure the callback was actually
    // triggered:
    bool insert_succeeded = false;

    // Listen to the insertion-failed event: we want to get a copy of the server's
    // response so that we can compare it to the thrown exception later:
    apm_opts.on_command_succeeded([&writeErrors_well_formed, &insert_succeeded](
                                      const mongocxx::events::command_succeeded_event& ev) {
        if (0 != ev.command_name().compare("insert")) {
            return;
        }

        REQUIRE(writeErrors_well_formed(ev.reply()));

        // Make sure that "we" were actually called:
        insert_succeeded = true;
    });

    client_opts.apm_opts(apm_opts);

    auto mongodb_client = mongocxx::client(uri{}, client_opts);

    if (!test_util::newer_than(mongodb_client, "5.0")) {
        WARN("skip: test requires MongoDB server 5.0 or newer");
        return;
    }

    database db = mongodb_client["prose_test_expose_details"];

    const std::string collname{"mongo_cxx_driver-expose_details"};

    // Drop the existing collection, if any:
    db[collname].drop();

    // Make a new collection with validation checking:
    collection coll = db.create_collection(
        collname,
        make_document(kvp("validator",
                          make_document(kvp("field_x", make_document(kvp("$type", "string")))))));

    SECTION("cause a type violation on insert") {
        bsoncxx::builder::basic::document entry;

        entry.append(kvp("_id", bsoncxx::oid()), kvp("field_x", 42));

        try {
            coll.insert_one(entry.view());

            // We should not make it here (i.e. this is an error):
            CHECK(false);
        } catch (const operation_exception& e) {
            auto rse = e.raw_server_error();

            // We have no has_value() check:
            CHECK(rse);

            CHECK(writeErrors_well_formed(*rse));
        } catch (...) {
            // An exception was thrown, but of the wrong type:
            CHECK(false);
        }

        // Make sure that our callback was actually triggered and completed successfully:
        REQUIRE(insert_succeeded);
    }
}

}  // namespace
