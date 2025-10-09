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

#include <chrono>
#include <string>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/document/element.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/private/helpers.hh>
#include <bsoncxx/private/libbson.hh>
#include <bsoncxx/stdx/optional.hpp>
#include <bsoncxx/test/catch.hh>
#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/database.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/update.hpp>
#include <mongocxx/pipeline.hpp>
#include <mongocxx/private/conversions.hh>
#include <mongocxx/private/libbson.hh>
#include <mongocxx/private/libmongoc.hh>
#include <mongocxx/read_preference.hpp>
#include <third_party/catch/include/helpers.hpp>

namespace {
using namespace mongocxx;
using namespace bsoncxx;

using builder::basic::kvp;
using builder::basic::make_document;

TEST_CASE("A default constructed collection is false-ish", "[collection]") {
    instance::current();

    collection c;
    REQUIRE(!c);
}

TEST_CASE("Collection", "[collection]") {
    instance::current();

    // dummy_collection is the name the mocked collection_get_name returns
    const std::string collection_name("dummy_collection");
    const std::string database_name("mocked_collection");

    MOCK_CLIENT
    MOCK_DATABASE
    MOCK_COLLECTION
    MOCK_FAM
    MOCK_BULK
    MOCK_CURSOR

    client mongo_client{uri{}};
    write_concern concern;
    database mongo_db = mongo_client[database_name];
    collection mongo_coll = mongo_db[collection_name];
    REQUIRE(mongo_coll);

    SECTION("Read Concern", "[collection]") {
        auto collection_set_rc_called = false;
        read_concern rc{};
        rc.acknowledge_level(read_concern::level::k_majority);

        collection_set_read_concern->interpose(
            [&collection_set_rc_called](::mongoc_collection_t*,
                                        const ::mongoc_read_concern_t* rc_t) {
                REQUIRE(rc_t);
                const auto result = libmongoc::read_concern_get_level(rc_t);
                REQUIRE(result);
                REQUIRE(strcmp(result, "majority") == 0);
                collection_set_rc_called = true;
            });

        mongo_coll.read_concern(rc);
        REQUIRE(collection_set_rc_called);
    }

    auto filter_doc = make_document(kvp("_id", "wow"), kvp("foo", "bar"));

    SECTION("Aggregate", "[Collection::aggregate]") {
        const auto expected_allow_disk_use = true;
        const auto expected_batch_size = 5678;
        const auto expected_bypass_document_validation = true;
        const auto expected_collation = make_document(kvp("locale", "en_US"));
        const auto expected_comment = make_document(kvp("$comment", "some_comment"));
        const auto expected_hint = hint("some_hint");
        const auto expected_let = make_document(kvp("x", "foo"));
        const auto expected_max_time_ms = 1234;
        const auto expected_read_preference =
            read_preference{}.mode(read_preference::read_mode::k_secondary);

        const auto expected_read_concern = make_document(kvp("level", "majority"));
        const auto read_concern = [] {
            mongocxx::read_concern rc;
            rc.acknowledge_level(read_concern::level::k_majority);
            return rc;
        }();

        const auto expected_write_concern =
            make_document(kvp("w", "majority"), kvp("wtimeout", 100));
        const auto write_concern = [] {
            mongocxx::write_concern wc;
            wc.majority(std::chrono::milliseconds(100));
            return wc;
        }();

        pipeline pipe;
        options::aggregate opts;

        auto collection_aggregate_called = false;

        collection_aggregate->interpose(
            [&](mongoc_collection_t*,
                mongoc_query_flags_t flags,
                const bson_t* pipeline,
                const bson_t* options,
                const mongoc_read_prefs_t* read_preference) -> mongoc_cursor_t* {
                collection_aggregate_called = true;
                REQUIRE(flags == MONGOC_QUERY_NONE);

                bsoncxx::array::view p(bson_get_data(pipeline), pipeline->len);
                bsoncxx::document::view o(bson_get_data(options), options->len);

                bsoncxx::stdx::string_view bar(
                    p[0].get_document().value["$match"].get_document().value["foo"].get_string());
                std::int32_t one(
                    p[1].get_document().value["$sort"].get_document().value["foo"].get_int32());

                REQUIRE(bar == bsoncxx::stdx::string_view("bar"));
                REQUIRE(one == 1);

                if (opts.allow_disk_use()) {
                    REQUIRE(o["allowDiskUse"].get_bool().value == expected_allow_disk_use);
                } else {
                    REQUIRE(o.find("allowDiskUse") == o.end());
                }

                if (opts.batch_size()) {
                    REQUIRE(o["batchSize"].get_int32().value == expected_batch_size);
                } else {
                    REQUIRE(o.find("batchSize") == o.end());
                }

                if (opts.bypass_document_validation()) {
                    REQUIRE(o["bypassDocumentValidation"].get_bool().value ==
                            expected_bypass_document_validation);
                } else {
                    REQUIRE(!o["bypassDocumentValidation"]);
                }

                if (opts.collation()) {
                    REQUIRE(o["collation"].get_document().value == expected_collation);
                } else {
                    REQUIRE(o.find("collation") == o.end());
                }

                if (opts.comment()) {
                    REQUIRE(o["comment"].get_value() == expected_comment["$comment"].get_value());
                } else {
                    REQUIRE(o.find("comment") == o.end());
                }

                if (opts.hint()) {
                    REQUIRE(o["hint"].get_value() == expected_hint.to_value());
                } else {
                    REQUIRE(o.find("hint") == o.end());
                }

                if (opts.let()) {
                    REQUIRE(o["let"].get_document().value == expected_let);
                } else {
                    REQUIRE(o.find("let") == o.end());
                }

                if (opts.max_time()) {
                    REQUIRE(o["maxTimeMS"].get_int64().value == expected_max_time_ms);
                } else {
                    REQUIRE(o.find("maxTimeMS") == o.end());
                }

                if (opts.read_concern()) {
                    REQUIRE(o["readConcern"].get_document().value == expected_read_concern);
                } else {
                    REQUIRE(o.find("readConcern") == o.end());
                }

                if (opts.read_preference()) {
                    REQUIRE(mongoc_read_prefs_get_mode(read_preference) ==
                            static_cast<int>(opts.read_preference()->mode()));
                } else {
                    REQUIRE(mongoc_read_prefs_get_mode(read_preference) ==
                            libmongoc::conversions::read_mode_t_from_read_mode(
                                mongo_coll.read_preference().mode()));
                }

                if (opts.write_concern()) {
                    REQUIRE(o["writeConcern"].get_document().value == expected_write_concern);
                } else {
                    REQUIRE(o.find("writeConcern") == o.end());
                }

                return NULL;
            });

        pipe.match(make_document(kvp("foo", "bar")));
        pipe.sort(make_document(kvp("foo", 1)));

        SECTION("With default options") {}

        SECTION("With some options") {
            opts.allow_disk_use(expected_allow_disk_use);
            opts.batch_size(expected_batch_size);
            opts.bypass_document_validation(expected_bypass_document_validation);
            opts.collation(expected_collation.view());
            opts.comment(expected_comment["$comment"].get_value());
            opts.hint(expected_hint);
            opts.let(expected_let.view());
            opts.max_time(std::chrono::milliseconds{expected_max_time_ms});
            opts.read_concern(read_concern);
            opts.read_preference(expected_read_preference);
            opts.write_concern(write_concern);
        }

        mongo_coll.aggregate(pipe, opts);

        REQUIRE(collection_aggregate_called);
    }

    SECTION("Count Documents", "[collection::count_documents]") {
        auto collection_count_called = false;
        bool success = true;
        std::int64_t expected_skip = 0;
        std::int64_t expected_limit = 0;
        const bson_t* expected_opts = nullptr;

        collection_count_documents->interpose([&](mongoc_collection_t*,
                                                  const bson_t* filter,
                                                  const bson_t* opts,
                                                  const mongoc_read_prefs_t*,
                                                  bson_t* reply,
                                                  bson_error_t* error) {
            collection_count_called = true;
            bson_init(reply);
            REQUIRE(bson_get_data(filter) == filter_doc.view().data());
            bson_iter_t iter;
            if (expected_skip) {
                REQUIRE(bson_iter_init_find(&iter, opts, "skip"));
                REQUIRE(bson_iter_int64(&iter) == expected_skip);
            }
            if (expected_limit) {
                REQUIRE(bson_iter_init_find(&iter, opts, "limit"));
                REQUIRE(bson_iter_int64(&iter) == expected_limit);
            }
            if (expected_opts) {
                bson_t opts_without_skip_or_limit = BSON_INITIALIZER;
                bson_copy_to_excluding_noinit(
                    opts, &opts_without_skip_or_limit, "skip", "limit", NULL);
                REQUIRE(bson_equal(&opts_without_skip_or_limit, expected_opts));
                bson_destroy(&opts_without_skip_or_limit);
            }

            if (success)
                return 123;

            // The caller expects the bson_error_t to have been
            // initialized by the call to count in the event of an
            // error.
            bson_set_error(error,
                           MONGOC_ERROR_COMMAND,
                           MONGOC_ERROR_COMMAND_INVALID_ARG,
                           "expected error from mock");

            return -1;
        });

        SECTION("Succeeds with defaults") {
            REQUIRE_NOTHROW(mongo_coll.count_documents(filter_doc.view()));
            REQUIRE(collection_count_called);
        }

        SECTION("Succeeds with options") {
            options::count opts;
            opts.skip(expected_skip);
            opts.limit(expected_limit);
            REQUIRE_NOTHROW(mongo_coll.count_documents(filter_doc.view(), opts));
            REQUIRE(collection_count_called);
        }

        SECTION("Succeeds with hint") {
            options::count opts;
            hint index_hint("a_1");
            opts.hint(index_hint);

            // set our expected_opts so we check against that
            bsoncxx::document::value doc = make_document(kvp("hint", index_hint.to_value()));
            libbson::scoped_bson_t cmd_opts{std::move(doc)};
            expected_opts = cmd_opts.bson();

            REQUIRE_NOTHROW(mongo_coll.count_documents(filter_doc.view(), opts));
            REQUIRE(collection_count_called);
        }

        SECTION("Succeeds with read_prefs") {
            options::count opts;
            read_preference rp;
            rp.mode(read_preference::read_mode::k_secondary);
            opts.read_preference(rp);
            REQUIRE_NOTHROW(mongo_coll.count_documents(filter_doc.view(), opts));
            REQUIRE(collection_count_called);
        }

        SECTION("Fails") {
            success = false;
            REQUIRE_THROWS_AS(mongo_coll.count_documents(filter_doc.view()), operation_exception);
            REQUIRE(collection_count_called);
        }
    }

    SECTION("Estimated Document Count", "[collection::estimated_document_count]") {
        auto collection_estimated_document_count_called = false;
        bool success = true;
        const bson_t* expected_opts = nullptr;

        collection_estimated_document_count->interpose([&](mongoc_collection_t*,
                                                           const bson_t* opts,
                                                           const mongoc_read_prefs_t*,
                                                           bson_t* reply,
                                                           bson_error_t* error) {
            collection_estimated_document_count_called = true;
            bson_init(reply);
            if (expected_opts) {
                REQUIRE(bson_equal(opts, expected_opts));
            }

            if (success)
                return 123;

            // The caller expects the bson_error_t to have been
            // initialized by the call to count in the event of an
            // error.
            bson_set_error(error,
                           MONGOC_ERROR_COMMAND,
                           MONGOC_ERROR_COMMAND_INVALID_ARG,
                           "expected error from mock");

            return -1;
        });

        SECTION("Succeeds with defaults") {
            REQUIRE_NOTHROW(mongo_coll.estimated_document_count());
            REQUIRE(collection_estimated_document_count_called);
        }

        SECTION("Succeeds with options") {
            options::estimated_document_count opts;
            REQUIRE_NOTHROW(mongo_coll.estimated_document_count(opts));
            REQUIRE(collection_estimated_document_count_called);
        }

        SECTION("Succeeds with read_prefs") {
            options::estimated_document_count opts;
            read_preference rp;
            rp.mode(read_preference::read_mode::k_secondary);
            opts.read_preference(rp);
            REQUIRE_NOTHROW(mongo_coll.estimated_document_count(opts));
            REQUIRE(collection_estimated_document_count_called);
        }

        SECTION("Fails") {
            success = false;
            REQUIRE_THROWS_AS(mongo_coll.estimated_document_count(), operation_exception);
            REQUIRE(collection_estimated_document_count_called);
        }
    }

    SECTION("Find", "[collection::find]") {
        auto collection_find_called = false;
        auto find_doc = make_document(kvp("a", 1));
        auto doc = find_doc.view();
        mongocxx::stdx::optional<bool> expected_allow_partial_results;
        mongocxx::stdx::optional<bsoncxx::stdx::string_view> expected_comment{};
        mongocxx::stdx::optional<mongocxx::cursor::type> expected_cursor_type{};
        mongocxx::stdx::optional<bsoncxx::types::bson_value::view> expected_hint{};
        mongocxx::stdx::optional<bool> expected_no_cursor_timeout;
        mongocxx::stdx::optional<bsoncxx::document::view> expected_sort{};
        mongocxx::stdx::optional<read_preference> expected_read_preference{};

        collection_find_with_opts->interpose([&](mongoc_collection_t*,
                                                 const bson_t* filter,
                                                 const bson_t* opts,
                                                 const mongoc_read_prefs_t* read_prefs) {
            collection_find_called = true;

            bsoncxx::document::view filter_view{bson_get_data(filter), filter->len};
            bsoncxx::document::view opts_view{bson_get_data(opts), opts->len};

            REQUIRE(filter_view == doc);

            if (expected_allow_partial_results) {
                REQUIRE(opts_view["allowPartialResults"].get_bool().value ==
                        *expected_allow_partial_results);
            }
            if (expected_comment) {
                REQUIRE(opts_view["comment"].get_string().value == *expected_comment);
            }
            if (expected_cursor_type) {
                bsoncxx::document::element tailable = opts_view["tailable"];
                bsoncxx::document::element awaitData = opts_view["awaitData"];
                switch (*expected_cursor_type) {
                    case mongocxx::cursor::type::k_non_tailable:
                        REQUIRE(!tailable);
                        REQUIRE(!awaitData);
                        break;
                    case mongocxx::cursor::type::k_tailable:
                        REQUIRE(tailable.get_bool().value);
                        REQUIRE(!awaitData);
                        break;
                    case mongocxx::cursor::type::k_tailable_await:
                        REQUIRE(tailable.get_bool().value);
                        REQUIRE(awaitData.get_bool().value);
                        break;
                }
            }
            if (expected_hint) {
                REQUIRE(opts_view["hint"].get_string() == expected_hint->get_string());
            }
            if (expected_no_cursor_timeout) {
                REQUIRE(opts_view["noCursorTimeout"].get_bool().value ==
                        *expected_no_cursor_timeout);
            }
            if (expected_sort) {
                REQUIRE(opts_view["sort"].get_document() == *expected_sort);
            }

            if (expected_read_preference)
                REQUIRE(mongoc_read_prefs_get_mode(read_prefs) ==
                        static_cast<int>(expected_read_preference->mode()));
            else
                REQUIRE(mongoc_read_prefs_get_mode(read_prefs) ==
                        libmongoc::conversions::read_mode_t_from_read_mode(
                            mongo_coll.read_preference().mode()));

            mongoc_cursor_t* cursor = NULL;
            return cursor;
        });

        SECTION("find succeeds") {
            REQUIRE_NOTHROW(mongo_coll.find(doc));
            REQUIRE(collection_find_called);
        }

        SECTION("Succeeds with allow_partial_results") {
            options::find opts;
            expected_allow_partial_results = true;
            opts.allow_partial_results(*expected_allow_partial_results);

            REQUIRE_NOTHROW(mongo_coll.find(doc, opts));
            REQUIRE(collection_find_called);
        }

        SECTION("Succeeds with comment") {
            expected_comment.emplace("my comment");
            options::find opts;
            opts.comment(*expected_comment);

            REQUIRE_NOTHROW(mongo_coll.find(doc, opts));
            REQUIRE(collection_find_called);
        }

        SECTION("Succeeds with cursor type") {
            options::find opts;
            expected_cursor_type = mongocxx::cursor::type::k_tailable;
            opts.cursor_type(*expected_cursor_type);

            REQUIRE_NOTHROW(mongo_coll.find(doc, opts));
            REQUIRE(collection_find_called);
        }

        SECTION("Succeeds with hint") {
            options::find opts;
            hint index_hint("a_1");
            expected_hint = index_hint.to_value();
            opts.hint(index_hint);

            REQUIRE_NOTHROW(mongo_coll.find(doc, opts));
            REQUIRE(collection_find_called);
        }

        SECTION("Succeeds with no_cursor_timeout") {
            options::find opts;
            expected_no_cursor_timeout = true;
            opts.no_cursor_timeout(*expected_no_cursor_timeout);

            REQUIRE_NOTHROW(mongo_coll.find(doc, opts));
            REQUIRE(collection_find_called);
        }

        SECTION("Succeeds with sort") {
            options::find opts{};
            auto sort_doc = make_document(kvp("x", -1));
            expected_sort = sort_doc.view();
            opts.sort(*expected_sort);
            REQUIRE_NOTHROW(mongo_coll.find(doc, opts));
            REQUIRE(collection_find_called);
        }

        SECTION("Succeeds with read preference") {
            options::find opts{};
            expected_read_preference.emplace();
            expected_read_preference->mode(read_preference::read_mode::k_secondary);
            opts.read_preference(*expected_read_preference);

            REQUIRE_NOTHROW(mongo_coll.find(doc, opts));
            REQUIRE(collection_find_called);
        }
    }

    SECTION("Writes", "[collection::writes]") {
        auto expected_order_setting = false;
        auto expect_set_bypass_document_validation_called = false;
        auto expected_bypass_document_validation = false;
        mongocxx::stdx::optional<bsoncxx::types::bson_value::view> expected_hint{};

        auto modification_doc = make_document(kvp("cool", "wow"), kvp("foo", "bar"));

        auto perform_checks = [&]() {
            REQUIRE(collection_create_bulk_operation_called);
            REQUIRE(expect_set_bypass_document_validation_called ==
                    bulk_operation_set_bypass_document_validation_called);
            REQUIRE(bulk_operation_op_called);
            REQUIRE(bulk_operation_destroy_called);
        };

        collection_create_bulk_operation_with_opts->interpose(
            [&](mongoc_collection_t*, const bson_t* opts) -> mongoc_bulk_operation_t* {
                bson_iter_t iter;
                if (expected_order_setting) {
                    // If the write operation is expected to set "ordered": true, then it
                    // should *not* be included in the bulk operations, since that is the default.
                    REQUIRE(!bson_iter_init_find(&iter, opts, "ordered"));
                } else {
                    REQUIRE(bson_iter_init_find(&iter, opts, "ordered"));
                    REQUIRE(BSON_ITER_HOLDS_BOOL(&iter));
                    REQUIRE(!bson_iter_bool(&iter));
                }
                collection_create_bulk_operation_called = true;
                return nullptr;
            });

        bulk_operation_set_bypass_document_validation->interpose(
            [&](mongoc_bulk_operation_t*, bool bypass) {
                bulk_operation_set_bypass_document_validation_called = true;
                REQUIRE(expected_bypass_document_validation == bypass);
            });

        bulk_operation_execute->interpose(
            [&](mongoc_bulk_operation_t*, bson_t* reply, bson_error_t*) {
                bulk_operation_execute_called = true;
                bson_init(reply);
                return 1;
            });

        bulk_operation_destroy->interpose(
            [&](mongoc_bulk_operation_t*) { bulk_operation_destroy_called = true; });

        SECTION("Insert One", "[collection::insert_one]") {
            expected_order_setting = true;
            bulk_operation_insert_with_opts->interpose(
                [&](mongoc_bulk_operation_t*, const bson_t* doc, const bson_t*, bson_error_t*) {
                    bulk_operation_op_called = true;
                    REQUIRE(bson_get_data(doc) == filter_doc.view().data());
                    return true;
                });

            mongo_coll.insert_one(filter_doc.view());
            perform_checks();
        }

        SECTION("Insert One Bypassing Validation", "[collection::insert_one]") {
            expected_order_setting = true;
            bulk_operation_insert_with_opts->interpose(
                [&](mongoc_bulk_operation_t*, const bson_t* doc, const bson_t*, bson_error_t*) {
                    bulk_operation_op_called = true;
                    REQUIRE(bson_get_data(doc) == filter_doc.view().data());
                    return true;
                });

            expect_set_bypass_document_validation_called = true;
            SECTION("...set to false") {
                expected_bypass_document_validation = false;
            }
            SECTION("...set to true") {
                expected_bypass_document_validation = true;
            }
            options::insert opts{};
            opts.bypass_document_validation(expected_bypass_document_validation);
            mongo_coll.insert_one(filter_doc.view(), opts);
            perform_checks();
        }

        SECTION("Insert Many Ordered", "[collection::insert_many]") {
            bulk_operation_insert_with_opts->interpose(
                [&](mongoc_bulk_operation_t*, const bson_t* doc, const bson_t*, bson_error_t*) {
                    bulk_operation_op_called = true;
                    REQUIRE(bson_get_data(doc) == filter_doc.view().data());
                    return true;
                });

            // The interposed collection_create_bulk_operation_with_opts validates this setting.
            SECTION("...set to false") {
                expected_order_setting = false;
            }
            SECTION("...set to true") {
                expected_order_setting = true;
            }
            options::insert opts{};
            opts.ordered(expected_order_setting);
            std::vector<bsoncxx::document::view> docs{};
            docs.push_back(filter_doc.view());
            mongo_coll.insert_many(docs, opts);
            perform_checks();
        }

        SECTION("Update One", "[collection::update_one]") {
            bool upsert_option = false;
            expected_order_setting = true;

            bulk_operation_update_one_with_opts->interpose([&](mongoc_bulk_operation_t*,
                                                               const bson_t* query,
                                                               const bson_t* update,
                                                               const bson_t* options,
                                                               bson_error_t*) {
                bulk_operation_op_called = true;
                REQUIRE(bson_get_data(query) == filter_doc.view().data());
                REQUIRE(bson_get_data(update) == modification_doc.view().data());

                bsoncxx::document::view options_view{bson_get_data(options), options->len};

                bsoncxx::document::element upsert = options_view["upsert"];
                if (upsert_option) {
                    REQUIRE(upsert);
                    REQUIRE(upsert.type() == bsoncxx::type::k_bool);
                    REQUIRE(upsert.get_bool().value);
                } else {
                    // Allow either no "upsert" option, or an "upsert" option set to false.
                    if (upsert) {
                        REQUIRE(upsert);
                        REQUIRE(upsert.type() == bsoncxx::type::k_bool);
                        REQUIRE(!upsert.get_bool().value);
                    }
                }

                if (expected_hint) {
                    REQUIRE(options_view["hint"].get_string() == expected_hint->get_string());
                } else {
                    REQUIRE(!options_view["hint"]);
                }

                return true;
            });

            options::update options;

            SECTION("Default Options") {}

            SECTION("Upsert true") {
                upsert_option = true;
                expected_order_setting = true;
                options.upsert(upsert_option);
            }

            SECTION("Upsert false") {
                upsert_option = false;
                expected_order_setting = true;
                options.upsert(upsert_option);
            }

            SECTION("With hint") {
                hint index_hint("a_1");
                expected_hint = index_hint.to_value();
                options.hint(index_hint);
            }

            SECTION("With bypass_document_validation") {
                expect_set_bypass_document_validation_called = true;
                expected_bypass_document_validation = true;
                expected_order_setting = true;
                options.bypass_document_validation(expected_bypass_document_validation);
            }

            SECTION("Write Concern provided") {
                options.write_concern(concern);
            }

            mongo_coll.update_one(filter_doc.view(), modification_doc.view(), options);
            REQUIRE(bulk_operation_execute_called);
            perform_checks();
        }

        SECTION("Insert One Error", "[collection::insert_one]") {
            expected_order_setting = true;
            bulk_operation_insert_with_opts->interpose(
                [&](mongoc_bulk_operation_t*, const bson_t*, const bson_t*, bson_error_t* err) {
                    bulk_operation_op_called = true;
                    bson_set_error(err, MONGOC_ERROR_BSON, MONGOC_ERROR_BSON_INVALID, "err");
                    return false;
                });

            REQUIRE_THROWS_AS(mongo_coll.insert_one(filter_doc.view()), mongocxx::logic_error);
            REQUIRE(!bulk_operation_execute_called);
            perform_checks();
        }

        SECTION("Insert Many Error", "[collection::insert_many]") {
            expected_order_setting = true;
            bulk_operation_insert_with_opts->interpose(
                [&](mongoc_bulk_operation_t*, const bson_t*, const bson_t*, bson_error_t* err) {
                    bulk_operation_op_called = true;
                    bson_set_error(err, MONGOC_ERROR_BSON, MONGOC_ERROR_BSON_INVALID, "err");
                    return false;
                });

            std::vector<bsoncxx::document::view> docs{};
            docs.push_back(filter_doc.view());
            expected_order_setting = true;
            REQUIRE_THROWS_AS(mongo_coll.insert_many(docs), mongocxx::logic_error);
            REQUIRE(!bulk_operation_execute_called);
            perform_checks();
        }

        SECTION("Update One Error", "[collection::update_one]") {
            expected_order_setting = true;
            bulk_operation_update_one_with_opts->interpose([&](mongoc_bulk_operation_t*,
                                                               const bson_t*,
                                                               const bson_t*,
                                                               const bson_t*,
                                                               bson_error_t* err) {
                bulk_operation_op_called = true;
                bson_set_error(err, MONGOC_ERROR_BSON, MONGOC_ERROR_BSON_INVALID, "err");
                return false;
            });

            REQUIRE_THROWS_AS(mongo_coll.update_one(filter_doc.view(), modification_doc.view()),
                              mongocxx::logic_error);
            REQUIRE(!bulk_operation_execute_called);
            perform_checks();
        }

        SECTION("Update Many", "[collection::update_many]") {
            bool upsert_option = false;
            expected_order_setting = true;

            bulk_operation_update_many_with_opts->interpose([&](mongoc_bulk_operation_t*,
                                                                const bson_t* query,
                                                                const bson_t* update,
                                                                const bson_t* options,
                                                                bson_error_t*) {
                bulk_operation_op_called = true;
                REQUIRE(bson_get_data(query) == filter_doc.view().data());
                REQUIRE(bson_get_data(update) == modification_doc.view().data());

                bsoncxx::document::view options_view{bson_get_data(options), options->len};

                bsoncxx::document::element upsert = options_view["upsert"];
                if (upsert_option) {
                    REQUIRE(upsert);
                    REQUIRE(upsert.type() == bsoncxx::type::k_bool);
                    REQUIRE(upsert.get_bool().value);
                } else {
                    // Allow either no "upsert" option, or an "upsert" option set to false.
                    if (upsert) {
                        REQUIRE(upsert);
                        REQUIRE(upsert.type() == bsoncxx::type::k_bool);
                        REQUIRE(!upsert.get_bool().value);
                    }
                }

                if (expected_hint) {
                    REQUIRE(options_view["hint"].get_string() == expected_hint->get_string());
                } else {
                    REQUIRE(!options_view["hint"]);
                }

                return true;
            });

            options::update options;

            SECTION("Default Options") {
                upsert_option = false;
            }

            SECTION("Upsert true") {
                upsert_option = true;
                options.upsert(upsert_option);
            }

            SECTION("Upsert false") {
                upsert_option = false;
                options.upsert(upsert_option);
            }

            SECTION("With hint") {
                hint index_hint("a_1");
                expected_hint = index_hint.to_value();
                options.hint(index_hint);
            }

            mongo_coll.update_many(filter_doc.view(), modification_doc.view(), options);
            REQUIRE(bulk_operation_execute_called);
            perform_checks();
        }

        SECTION("Update Many Error", "[collection::update_many]") {
            expected_order_setting = true;
            bulk_operation_update_many_with_opts->interpose([&](mongoc_bulk_operation_t*,
                                                                const bson_t*,
                                                                const bson_t*,
                                                                const bson_t*,
                                                                bson_error_t* err) {
                bulk_operation_op_called = true;
                bson_set_error(err, MONGOC_ERROR_BSON, MONGOC_ERROR_BSON_INVALID, "err");
                return false;
            });

            REQUIRE_THROWS_AS(mongo_coll.update_many(filter_doc.view(), modification_doc.view()),
                              mongocxx::logic_error);
            REQUIRE(!bulk_operation_execute_called);
            perform_checks();
        }

        SECTION("Replace One", "[collection::replace_one]") {
            bool upsert_option = false;
            expected_order_setting = true;

            bulk_operation_replace_one_with_opts->interpose([&](mongoc_bulk_operation_t*,
                                                                const bson_t* query,
                                                                const bson_t* update,
                                                                const bson_t* options,
                                                                bson_error_t*) {
                bulk_operation_op_called = true;
                REQUIRE(bson_get_data(query) == filter_doc.view().data());
                REQUIRE(bson_get_data(update) == modification_doc.view().data());

                bsoncxx::document::view options_view{bson_get_data(options), options->len};

                bsoncxx::document::element upsert = options_view["upsert"];
                if (upsert_option) {
                    REQUIRE(upsert);
                    REQUIRE(upsert.type() == bsoncxx::type::k_bool);
                    REQUIRE(upsert.get_bool().value);
                } else {
                    // Allow either no "upsert" option, or an "upsert" option set to false.
                    if (upsert) {
                        REQUIRE(upsert);
                        REQUIRE(upsert.type() == bsoncxx::type::k_bool);
                        REQUIRE(!upsert.get_bool().value);
                    }
                }

                if (expected_hint) {
                    REQUIRE(options_view["hint"].get_string() == expected_hint->get_string());
                } else {
                    REQUIRE(!options_view["hint"]);
                }

                return true;
            });

            options::replace options;

            SECTION("Default Options") {
                upsert_option = false;
            }

            SECTION("Upsert true") {
                upsert_option = true;
                options.upsert(upsert_option);
            }

            SECTION("Upsert false") {
                upsert_option = false;
                options.upsert(upsert_option);
            }

            SECTION("With hint") {
                hint index_hint("a_1");
                expected_hint = index_hint.to_value();
                options.hint(index_hint);
            }

            mongo_coll.replace_one(filter_doc.view(), modification_doc.view(), options);
            REQUIRE(bulk_operation_execute_called);
            perform_checks();
        }

        SECTION("Replace One Error", "[collection::update_one]") {
            expected_order_setting = true;
            bulk_operation_replace_one_with_opts->interpose([&](mongoc_bulk_operation_t*,
                                                                const bson_t*,
                                                                const bson_t*,
                                                                const bson_t*,
                                                                bson_error_t* err) {
                bulk_operation_op_called = true;
                bson_set_error(err, MONGOC_ERROR_BSON, MONGOC_ERROR_BSON_INVALID, "err");
                return false;
            });

            REQUIRE_THROWS_AS(mongo_coll.replace_one(filter_doc.view(), modification_doc.view()),
                              mongocxx::logic_error);
            REQUIRE(!bulk_operation_execute_called);
            perform_checks();
        }

        SECTION("Delete One", "[collection::delete_one]") {
            expected_order_setting = true;
            bulk_operation_remove_one_with_opts->interpose([&](mongoc_bulk_operation_t*,
                                                               const bson_t* doc,
                                                               const bson_t* options,
                                                               bson_error_t*) {
                bulk_operation_op_called = true;
                REQUIRE(bson_get_data(doc) == filter_doc.view().data());

                bsoncxx::document::view options_view{bson_get_data(options), options->len};
                if (expected_hint) {
                    CAPTURE(to_json(options_view));
                    REQUIRE(options_view["hint"].get_string() == expected_hint->get_string());
                } else {
                    REQUIRE(!options_view["hint"]);
                }
                return true;
            });

            options::delete_options options;
            SECTION("With hint") {
                hint index_hint("a_1");
                expected_hint = index_hint.to_value();
                options.hint(index_hint);
            }

            mongo_coll.delete_one(filter_doc.view(), options);
            REQUIRE(bulk_operation_execute_called);
            perform_checks();
        }

        SECTION("Delete One Error", "[collection::delete_one]") {
            expected_order_setting = true;
            bulk_operation_remove_one_with_opts->interpose(
                [&](mongoc_bulk_operation_t*, const bson_t*, const bson_t*, bson_error_t* err) {
                    bulk_operation_op_called = true;
                    bson_set_error(err, MONGOC_ERROR_BSON, MONGOC_ERROR_BSON_INVALID, "err");
                    return false;
                });

            REQUIRE_THROWS_AS(mongo_coll.delete_one(filter_doc.view()), mongocxx::logic_error);
            REQUIRE(!bulk_operation_execute_called);
            perform_checks();
        }

        SECTION("Delete Many", "[collection::delete_many]") {
            expected_order_setting = true;
            bulk_operation_remove_many_with_opts->interpose([&](mongoc_bulk_operation_t*,
                                                                const bson_t* doc,
                                                                const bson_t* options,
                                                                bson_error_t*) {
                bulk_operation_op_called = true;
                REQUIRE(bson_get_data(doc) == filter_doc.view().data());

                bsoncxx::document::view options_view{bson_get_data(options), options->len};
                if (expected_hint) {
                    CAPTURE(to_json(options_view));
                    REQUIRE(options_view["hint"].get_string() == expected_hint->get_string());
                } else {
                    REQUIRE(!options_view["hint"]);
                }
                return true;
            });

            options::delete_options options;
            SECTION("With hint") {
                hint index_hint("a_1");
                expected_hint = index_hint.to_value();
                options.hint(index_hint);
            }

            mongo_coll.delete_many(filter_doc.view(), options);
            REQUIRE(bulk_operation_execute_called);
            perform_checks();
        }

        SECTION("Delete Many Error", "[collection::delete_one]") {
            expected_order_setting = true;
            bulk_operation_remove_many_with_opts->interpose(
                [&](mongoc_bulk_operation_t*, const bson_t*, const bson_t*, bson_error_t* err) {
                    bulk_operation_op_called = true;
                    bson_set_error(err, MONGOC_ERROR_BSON, MONGOC_ERROR_BSON_INVALID, "err");
                    return false;
                });

            REQUIRE_THROWS_AS(mongo_coll.delete_many(filter_doc.view()), mongocxx::logic_error);
            REQUIRE(!bulk_operation_execute_called);
            perform_checks();
        }
    }
}
}  // namespace
