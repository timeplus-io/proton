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

#include <atomic>
#include <iostream>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/private/libbson.hh>
#include <bsoncxx/test/catch.hh>
#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/insert.hpp>
#include <mongocxx/options/pool.hpp>
#include <mongocxx/pipeline.hpp>
#include <mongocxx/pool.hpp>
#include <mongocxx/private/libbson.hh>
#include <mongocxx/test/client_helpers.hh>
#include <mongocxx/write_concern.hpp>
#include <third_party/catch/include/helpers.hpp>

namespace {

using bsoncxx::builder::basic::document;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_array;
using bsoncxx::builder::basic::make_document;

using namespace mongocxx;

// Create a single-item document.
// E.g. doc("foo", 123) creates {"foo":123}.
template <typename T>
bsoncxx::document::value doc(std::string key, T val) {
    bsoncxx::builder::basic::document out{};
    out.append(kvp(key, val));
    return out.extract();
}

// Generates lambda/interpose for change_stream_next
//
// Phrased as a lambda instead of function because c++11 doesn't have decltype(auto) and the
// return-type is haunting.
const auto gen_next = [](bool has_next) {
    static mongocxx::libbson::scoped_bson_t next_bson{make_document(kvp("some", "doc"))};
    return [=](mongoc_change_stream_t*, const bson_t** bson) mutable -> bool {
        if (has_next) {
            *bson = next_bson.bson();
        }
        return has_next;
    };
};

bson_t err_doc;

// Generates lambda/interpose for change_stream_error_document.
const auto gen_error = [](bool has_error) {
    bson_init(&err_doc);  // Will fit on stack.
    bson_append_int32(&err_doc, "ok", -1, 0);
    return [=](const mongoc_change_stream_t*, bson_error_t* err, const bson_t** bson) -> bool {
        if (has_error) {
            bson_set_error(err,
                           MONGOC_ERROR_CURSOR,
                           MONGOC_ERROR_CHANGE_STREAM_NO_RESUME_TOKEN,
                           "expected error");
            *bson = &err_doc;
        } else {
            *bson = NULL;
        }
        return has_error;
    };
};

const auto watch_interpose = [](const mongoc_collection_t*,
                                const bson_t*,
                                const bson_t*) -> mongoc_change_stream_t* { return nullptr; };

const auto destroy_interpose = [](mongoc_change_stream_t*) -> void {};

TEST_CASE("Change stream options") {
    instance::current();
    client mongodb_client{uri{}, test_util::add_test_server_api()};

    if (!test_util::is_replica_set(mongodb_client)) {
        WARN("skip: change streams require replica set");
        return;
    }

    SECTION("Error if both resumeAfter and startAfter are set") {
        bsoncxx::document::value resume_after = make_document(kvp("resume", "token"));
        bsoncxx::document::value start_after = make_document(kvp("start", "token"));

        // Invalid to set both resumeAfter and startAfter.
        options::change_stream cs_opts;
        cs_opts.resume_after(resume_after.view());
        cs_opts.start_after(start_after.view());

        auto cs = mongodb_client.watch(cs_opts);
        REQUIRE_THROWS(cs.begin());
    }
}

TEST_CASE("Spec Prose Tests") {
    instance::current();
    client client{uri{}, test_util::add_test_server_api()};

    if (!test_util::is_replica_set(client)) {
        WARN("skip: change streams require replica set");
        return;
    }

    auto db = client["db"];
    auto coll = db["coll"];
    coll.drop();

    write_concern wc_majority;
    wc_majority.majority(std::chrono::seconds(30));

    // As a sanity check, we implement the first prose test. The behavior tested
    // by the prose tests is implemented and tested by the C driver, so we won't
    // replicate the full suite of prose tests here.

    SECTION("1. ChangeStream must continuously track the last seen resumeToken") {
        // Set the batch size to 1 so we read 1 doc at a time.
        options::change_stream opts;
        opts.batch_size(1);
        auto cs = client.watch(std::move(opts));

        // With WC majority, insert some documents to listen for.
        auto doc1 = make_document(kvp("a", 1));
        auto doc2 = make_document(kvp("b", 2));
        auto doc3 = make_document(kvp("c", 3));

        options::insert insert_opts{};
        insert_opts.write_concern(wc_majority);

        {
            auto res = coll.insert_one(doc1.view(), insert_opts);
            REQUIRE(res);
            REQUIRE(res->result().inserted_count() == 1);
        }
        {
            auto res = coll.insert_one(doc2.view(), insert_opts);
            REQUIRE(res);
            REQUIRE(res->result().inserted_count() == 1);
        }
        {
            auto res = coll.insert_one(doc3.view(), insert_opts);
            REQUIRE(res);
            REQUIRE(res->result().inserted_count() == 1);
        }

        // For each read, check the resume token is updated. We should
        // be reading the postBatchResumeToken on each read, since our
        // batch size is 1.
        auto it = cs.begin();
        REQUIRE(it != cs.end());
        REQUIRE(cs.get_resume_token());
        auto token1 = bsoncxx::document::value(*cs.get_resume_token());

        it++;
        REQUIRE(it != cs.end());
        REQUIRE(cs.get_resume_token());
        auto token2 = bsoncxx::document::value(*cs.get_resume_token());
        REQUIRE(token1 != token2);

        it++;
        REQUIRE(it != cs.end());
        REQUIRE(cs.get_resume_token());
        auto token3 = bsoncxx::document::value(*cs.get_resume_token());
        REQUIRE(token2 != token3);
        REQUIRE(token1 != token3);

        // When out of docs, check that the resume token is the same as the last doc.
        it++;
        REQUIRE(it == cs.end());
        REQUIRE(cs.get_resume_token());
        REQUIRE(*cs.get_resume_token() == token3);
    }
}

TEST_CASE("Mock streams and error-handling") {
    MOCK_CHANGE_STREAM

    instance::current();
    client mongodb_client{uri{}, test_util::add_test_server_api()};
    options::change_stream options{};
    database db = mongodb_client["streams"];
    collection events = db["events"];

    // nop watch and destroy
    collection_watch->interpose(watch_interpose).forever();
    change_stream_destroy->interpose(destroy_interpose).forever();
    auto stream = events.watch();

    SECTION("Default-constructed iterator move/copy/assign") {
        change_stream::iterator it1;
        change_stream::iterator it2;

        REQUIRE(it1 == it2);
        REQUIRE(it2 == it1);

        it2 = it1;

        REQUIRE(it1 == it2);
        REQUIRE(it2 == it1);

        change_stream::iterator it2copy{it2};
        REQUIRE(it1 == it2copy);
        REQUIRE(it2 == it2copy);

        change_stream::iterator it3 = it1;
        REQUIRE(it2 == it3);
        REQUIRE(it3 == it1);

        // Trivially-copyable so no effect, but prevent regressions
        // from deleting move:
        change_stream::iterator it4 = std::move(it3);
        REQUIRE(it4 == it1);
    }

    SECTION("One event") {
        change_stream_next->interpose(gen_next(true));
        auto it = stream.begin();
        REQUIRE(*it == make_document(kvp("some", "doc")).view());

        SECTION("Then no events forever") {
            // Mock no more events forever.
            change_stream_next->interpose(gen_next(false)).forever();
            change_stream_error_document->interpose(gen_error(false)).forever();
            // We've reached end.
            REQUIRE(++it == stream.end());

            SECTION("No error") {
                REQUIRE(it == stream.end());
                REQUIRE(++it == stream.end());
                REQUIRE(*it == make_document().view());
            }
            SECTION("Reached the end") {
                REQUIRE(std::distance(stream.begin(), stream.end()) == 0);
            }
        }

        SECTION("No events for an iteration, then an error") {
            // Mock no more events for one pass.
            change_stream_next->interpose(gen_next(false));
            change_stream_error_document->interpose(gen_error(false));
            // We've reached end.
            REQUIRE(++it == stream.end());

            // Mock an error
            change_stream_next->interpose(gen_next(false)).forever();
            change_stream_error_document->interpose(gen_error(true)).forever();
            SECTION("Throw on post-incr") {
                REQUIRE_THROWS(it++);
                REQUIRE(std::distance(stream.begin(), stream.end()) == 0);
            }
            SECTION("Throw on pre-incr") {
                REQUIRE_THROWS(++it);
                REQUIRE(std::distance(stream.begin(), stream.end()) == 0);
            }
            SECTION("Throw on .begin") {
                REQUIRE_THROWS(stream.begin());

                // Then nothing forever
                REQUIRE(std::distance(stream.begin(), stream.end()) == 0);
                REQUIRE(std::distance(stream.begin(), stream.end()) == 0);
            }
        }

        SECTION("Then error") {
            // next call indicates no next and an error
            change_stream_next->interpose(gen_next(false));
            change_stream_error_document->interpose(gen_error(true));

            SECTION("Throw on subsequent increment") {
                REQUIRE_THROWS(it++);

                SECTION("Then at end") {
                    REQUIRE(it == stream.end());
                }
                SECTION("Remain at error state") {
                    REQUIRE(std::distance(stream.begin(), stream.end()) == 0);
                    REQUIRE(std::distance(stream.begin(), stream.end()) == 0);
                }
                SECTION("Don't hold on to previous document") {
                    // Debatable if we want to require this behavior since it's
                    // inconsistent with other cases of dereferencing something
                    // that's == end(). Important thing is that we don't maintain
                    // a handle on the previous event.
                    REQUIRE(*it == make_document().view());
                }
            }
        }
    }

    SECTION("Pipeline and opts are passed for all watch helpers") {
        bsoncxx::types::b_timestamp ts{1, 2};
        std::int32_t batch_size = 3;
        std::chrono::milliseconds max_await_time_ms{4};
        bool collection_watch_called = false;
        bool database_watch_called = false;
        bool client_watch_called = true;
        bsoncxx::document::value collation = make_document(kvp("locale", "en"));
        std::string full_document = "updateLookup";
        bsoncxx::document::value resume_after = make_document(kvp("resume", "token"));
        mongocxx::pipeline cs_pipeline;
        cs_pipeline.match(make_document(kvp("x", 1)));

        options::change_stream cs_opts;
        cs_opts.batch_size(batch_size);
        cs_opts.start_at_operation_time(ts);
        cs_opts.max_await_time(max_await_time_ms);
        cs_opts.collation(collation.view());
        cs_opts.full_document(full_document);
        cs_opts.resume_after(resume_after.view());

        auto check_pipeline_and_opts = [&](const bson_t* passed_pipeline,
                                           const bson_t* passed_opts) {
            bsoncxx::document::view pipeline(bson_get_data(passed_pipeline), passed_pipeline->len);
            bsoncxx::array::value expected =
                make_array(make_document(kvp("$match", make_document(kvp("x", 1)))));
            REQUIRE(pipeline["pipeline"].get_array().value == expected);
            bsoncxx::document::view opts(bson_get_data(passed_opts), passed_opts->len);
            REQUIRE(opts["startAtOperationTime"].get_timestamp() == ts);
            REQUIRE(opts["batchSize"].get_int32() == batch_size);
            REQUIRE(opts["maxAwaitTimeMS"].get_int64() == 4);
            REQUIRE(opts["collation"].get_document().view() == collation);
            REQUIRE(opts["fullDocument"].get_string().value ==
                    bsoncxx::stdx::string_view{full_document});
            REQUIRE(opts["resumeAfter"].get_document().view() == resume_after);
        };

        collection_watch->interpose(
            [&](const mongoc_collection_t* coll, const bson_t* pipeline, const bson_t* opts) {
                std::string name =
                    mongoc_collection_get_name(const_cast<mongoc_collection_t*>(coll));
                REQUIRE(name == "collection");
                check_pipeline_and_opts(pipeline, opts);
                collection_watch_called = true;
                return nullptr;
            });

        database_watch->interpose(
            [&](const mongoc_database_t* db, const bson_t* pipeline, const bson_t* opts) {
                std::string name = mongoc_database_get_name(const_cast<mongoc_database_t*>(db));
                REQUIRE(name == "db");
                check_pipeline_and_opts(pipeline, opts);
                database_watch_called = true;
                return nullptr;
            });

        client_watch->interpose(
            [&](const mongoc_client_t* client, const bson_t* pipeline, const bson_t* opts) {
                (void)client;
                check_pipeline_and_opts(pipeline, opts);
                client_watch_called = true;
                return nullptr;
            });

        mongodb_client["db"]["collection"].watch(cs_pipeline, cs_opts);
        mongodb_client["db"].watch(cs_pipeline, cs_opts);
        mongodb_client.watch(cs_pipeline, cs_opts);

        // Ensure the interpose was called.
        REQUIRE(collection_watch_called);
        REQUIRE(database_watch_called);
        REQUIRE(client_watch_called);
    }
}

// Put this before other tests which assume the collections already exists.
TEST_CASE("Create streams.events and assert we can read a single event", "[min36]") {
    instance::current();
    client mongodb_client{uri{}, test_util::add_test_server_api()};
    if (!test_util::is_replica_set(mongodb_client)) {
        WARN("skip: change streams require replica set");
        return;
    }

    collection events = mongodb_client["streams"]["events"];
    events.drop();

    events.insert_one(make_document(kvp("dummy", "doc")));
    change_stream stream = events.watch();
    events.insert_one(make_document(kvp("another", "event")));
    REQUIRE(std::distance(stream.begin(), stream.end()) == 1);

    // because we watch events2 in a test
    auto events2 = mongodb_client["streams"]["events2"];
    events2.drop();
}

TEST_CASE("Give an invalid pipeline", "[min36]") {
    instance::current();
    client mongodb_client{uri{}, test_util::add_test_server_api()};
    if (!test_util::is_replica_set(mongodb_client)) {
        WARN("skip: change streams require replica set");
        return;
    }

    options::change_stream options{};
    collection events = mongodb_client["streams"]["events"];

    pipeline p;
    p.match(make_document(kvp("$foo", -1)));

    auto stream = events.watch(p);

    SECTION("Error on .begin() even if no events") {
        REQUIRE_THROWS(stream.begin());
    }
    SECTION("After error, begin == end repeatedly") {
        REQUIRE_THROWS(stream.begin());
        REQUIRE(stream.begin() == stream.end());
        REQUIRE(stream.begin() == stream.end());
        REQUIRE(stream.end() == stream.begin());
    }
    SECTION("No error on .end") {
        REQUIRE(stream.end() == stream.end());
    }
}

TEST_CASE("Documentation Examples", "[min36]") {
    instance::current();
    mongocxx::pool pool{uri{}, options::pool(test_util::add_test_server_api())};
    auto mongodb_client = pool.acquire();
    if (!test_util::is_replica_set(*mongodb_client)) {
        WARN("skip: change streams require replica set");
        return;
    }

    collection events = (*mongodb_client)["streams"]["events"];
    collection inventory = events;  // doc examples use this name

    std::atomic_bool insert_thread_done;
    insert_thread_done.store(false);
    // Start a thread to repeatedly insert documents to generate notifications.
    auto insert_thread = std::thread{[&pool, &insert_thread_done] {
        auto client = pool.acquire();
        auto inventory = (*client)["streams"]["events"];
        while (true) {
            auto doc = make_document(kvp("username", "alice"));
            inventory.insert_one(doc.view());
            if (insert_thread_done) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }};

    SECTION("Example 1") {
        // Start Changestream Example 1
        change_stream stream = inventory.watch();
        auto it = stream.begin();
        while (it == stream.end()) {
            // Server returned no new notifications. Restart iteration to poll server.
            it = stream.begin();
        }
        bsoncxx::document::view next = *it;
        // End Changestream Example 1
        REQUIRE(0 == next["operationType"].get_string().value.compare("insert"));
    }

    SECTION("Example 2") {
        // Start Changestream Example 2
        options::change_stream options;
        options.full_document(bsoncxx::string::view_or_value{"updateLookup"});
        change_stream stream = inventory.watch(options);
        auto it = stream.begin();
        while (it == stream.end()) {
            // Server returned no new notifications. Restart iteration to poll server.
            it = stream.begin();
        }
        bsoncxx::document::view next = *it;
        // End Changestream Example 2
        REQUIRE(0 == next["operationType"].get_string().value.compare("insert"));
    }

    SECTION("Example 3") {
        bsoncxx::stdx::optional<bsoncxx::document::value> next;
        // Get one notification to set `next`.
        {
            change_stream stream = inventory.watch();
            auto it = stream.begin();
            while (it == stream.end()) {
                // Server returned no new notifications. Restart iteration to poll server.
                it = stream.begin();
            }
            next = bsoncxx::document::value(*it);
        }
        // Start Changestream Example 3
        auto resume_token = (*next)["_id"].get_document().value;
        options::change_stream options;
        options.resume_after(resume_token);
        change_stream stream = inventory.watch(options);
        auto it = stream.begin();
        while (it == stream.end()) {
            // Server returned no new notifications. Restart iteration to poll server.
            it = stream.begin();
        }
        // End Changestream Example 3
        REQUIRE(0 == (*it)["operationType"].get_string().value.compare("insert"));
    }

    SECTION("Example 4") {
        // Create a pipeline with
        //  [{"$match": {"$or": [{"fullDocument.username": "alice"}, {"operationType": "delete"}]}}]

        // Start Changestream Example 4
        mongocxx::pipeline cs_pipeline;
        cs_pipeline.match(
            make_document(kvp("$or",
                              make_array(make_document(kvp("fullDocument.username", "alice")),
                                         make_document(kvp("operationType", "delete"))))));

        change_stream stream = inventory.watch(cs_pipeline);
        auto it = stream.begin();
        while (it == stream.end()) {
            // Server returned no new notifications. Restart iteration to poll server.
            it = stream.begin();
        }
        // End Changestream Example 4
        REQUIRE(0 == (*it)["operationType"].get_string().value.compare("insert"));
    }

    insert_thread_done = true;
    insert_thread.join();
    inventory.drop();
}

TEST_CASE("Watch 2 collections", "[min36]") {
    instance::current();
    client mongodb_client{uri{}, test_util::add_test_server_api()};
    if (!test_util::is_replica_set(mongodb_client)) {
        WARN("skip: change streams require replica set");
        return;
    }

    options::change_stream options{};

    collection events = mongodb_client["streams"]["events"];
    collection events2 = mongodb_client["streams"]["events2"];

    change_stream x = events.watch();
    change_stream x2 = events.watch();

    SECTION("Empty iterators not equal") {
        REQUIRE(x.begin() == x.begin());
        REQUIRE(x.begin() != x2.begin());
        REQUIRE(x.begin() == x.end());
    }
    SECTION("End iterators are not equal from different streams") {
        REQUIRE(x.end() != x2.end());
    }
    SECTION("End iterators equal themselves") {
        REQUIRE(x.end() == x.end());
        REQUIRE(x2.end() == x2.end());
    }
    SECTION(".begin() from separate streams not equal") {
        REQUIRE(x.begin() != x2.begin());
    }
    SECTION(".begin equal themselves") {
        REQUIRE(x.begin() == x.begin());
        REQUIRE(x2.begin() == x2.begin());
    }

    SECTION("Iterator transitivity") {
        auto one = x.begin();
        auto two = x2.begin();

        REQUIRE(one == x.end());
        REQUIRE(two == x2.end());

        REQUIRE(x.end() != x2.end());
        REQUIRE(one != two);
    }
}

TEST_CASE("Watch a Collection", "[min36]") {
    instance::current();
    client mongodb_client{uri{}, test_util::add_test_server_api()};
    if (!test_util::is_replica_set(mongodb_client)) {
        WARN("skip: change streams require replica set");
        return;
    }

    options::change_stream options{};
    collection events = mongodb_client["streams"]["events"];

    change_stream x = events.watch();

    SECTION("Can copy- and move-assign iterators") {
        REQUIRE(events.insert_one(doc("a", "b")));

        auto one = x.begin();
        REQUIRE(one != x.end());

        auto two = one;
        REQUIRE(two != x.end());

        REQUIRE(one == two);
        REQUIRE(two == one);

        // move-assign (although it's trivially-copyable)
        auto three = std::move(two);

        REQUIRE(three != x.end());
        REQUIRE(one == three);

        // two is in moved-from state. Technically `three == two` but that's not required.
    }

    SECTION("Default change stream and no events") {
        SECTION("Can move-assign it") {
            change_stream move_copy = std::move(x);
        }
        SECTION("Can move-construct it") {
            change_stream move_constructed = change_stream{std::move(x)};
        }
        SECTION(".end == .end") {
            REQUIRE(x.end() == x.end());

            auto e = x.end();
            REQUIRE(e == e);
        }
        SECTION("No events and iterator equality") {
            REQUIRE(x.begin() == x.end());

            // a bit pedantic
            auto b = x.begin();
            REQUIRE(b == b);
            auto e = x.end();
            REQUIRE(e == e);

            REQUIRE(e == b);
            REQUIRE(b == e);
        }
        SECTION("Empty iterator is not equivalent to user-constructed iterator") {
            REQUIRE(x.begin() != change_stream::iterator{});
            REQUIRE(x.end() != change_stream::iterator{});
            REQUIRE(change_stream::iterator{} == change_stream::iterator{});
        }
    }

    SECTION("No events => iterator distance is zero") {
        REQUIRE(std::distance(x.begin(), x.end()) == 0);
        REQUIRE(std::distance(x.begin(), x.end()) == 0);
    }

    SECTION("Single event") {
        REQUIRE(events.insert_one(doc("a", "b")));

        SECTION("end iterators equal") {
            REQUIRE(x.end() == x.end());
        }
        SECTION("begin iterators equal") {
            REQUIRE(x.begin() == x.begin());
        }
        SECTION("begin != end") {
            REQUIRE(x.begin() != x.end());
        }
        SECTION("end != begin") {
            REQUIRE(x.end() != x.begin());
        }

        SECTION("Can receive it") {
            auto it = *(x.begin());
            REQUIRE(it["fullDocument"]["a"].get_string().value == stdx::string_view("b"));
        }

        SECTION("iterator equals itself") {
            auto it = x.begin();
            REQUIRE(it == it);

            auto e = x.end();
            REQUIRE(e == e);

            REQUIRE(it != e);
            REQUIRE(e != it);
        }

        SECTION("Can deref iterator with value multiple times") {
            auto it = x.begin();
            auto a = *it;
            auto b = *it;
            REQUIRE(a["fullDocument"]["a"].get_string().value == stdx::string_view("b"));
            REQUIRE(b["fullDocument"]["a"].get_string().value == stdx::string_view("b"));
        }

        SECTION("Calling .begin multiple times doesn't advance state") {
            auto a = *(x.begin());
            auto b = *(x.begin());
            REQUIRE(a == b);
        }

        SECTION("No more events after the first one") {
            auto it = x.begin();
            it++;
            REQUIRE(it == x.end());
            REQUIRE(x.begin() == x.end());
        }

        SECTION("Past end is empty document") {
            auto it = x.begin();
            it++;
            REQUIRE(*it == bsoncxx::builder::basic::document{});
        }

        SECTION("Can dereference end()") {
            auto it = x.begin();
            it++;
            REQUIRE(*it == *it);
        }
    }

    SECTION("Multiple events") {
        REQUIRE(events.insert_one(doc("a", "b")));
        REQUIRE(events.insert_one(doc("c", "d")));

        SECTION("A range-based for loop iterates twice") {
            int count = 0;
            for (const auto& v : x) {
                (void)v;
                ++count;
            }
            REQUIRE(count == 2);
        }

        SECTION("distance is two") {
            auto dist = std::distance(x.begin(), x.end());
            REQUIRE(dist == 2);
        }

        SECTION("Can't advance the .end iterator") {
            auto e = x.end();
            e++;
            auto dist = std::distance(x.begin(), x.end());
            REQUIRE(dist == 2);
        }

        SECTION("Can advance two .begin iterators") {
            ++x.begin();
            REQUIRE(std::distance(x.begin(), x.end()) == 1);
        }

        SECTION("Can advance two iterators through the events") {
            auto one = x.begin();
            auto two = x.begin();

            REQUIRE(one != x.end());
            REQUIRE(two != x.end());

            one++;

            REQUIRE(one != x.end());
            REQUIRE(two != x.end());

            two++;

            REQUIRE(one == x.end());
            REQUIRE(two == x.end());
        }
    }

    SECTION("Have already advanced past the first set of events") {
        REQUIRE(events.insert_one(doc("a", "b")));
        REQUIRE(events.insert_one(doc("c", "d")));

        REQUIRE(std::distance(x.begin(), x.end()) == 2);

        SECTION("Try to look for more events") {
            REQUIRE(x.begin() == x.end());
        }

        SECTION("There are more events we can find them") {
            REQUIRE(events.insert_one(doc("e", "f")));
            REQUIRE(std::distance(x.begin(), x.end()) == 1);
        }
    }

    SECTION("Update-only pipeline") {
        // Get full doc and deltas but only for updates
        mongocxx::options::change_stream opts;
        opts.full_document(bsoncxx::string::view_or_value{"updateLookup"});

        mongocxx::pipeline pipeline;
        pipeline.match(make_document(kvp("operationType", "update")));

        mongocxx::change_stream stream = events.watch(pipeline, opts);

        // create a document and then update it
        events.insert_one(make_document(kvp("_id", "one"), kvp("a", "a")));
        events.update_one(make_document(kvp("_id", "one")),
                          make_document(kvp("$set", make_document(kvp("a", "A")))));
        events.delete_one(make_document(kvp("_id", "one")));

        SECTION("See single update and not updates or deletes") {
            auto n_events = std::distance(stream.begin(), stream.end());
            REQUIRE(n_events == 1);
        }
    }
}

}  // namespace
