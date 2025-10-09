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

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/test/catch.hh>
#include <bsoncxx/types.hpp>
#include <mongocxx/bulk_write.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/private/libmongoc.hh>
#include <mongocxx/write_concern.hpp>

namespace {
using namespace mongocxx;

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

TEST_CASE("a bulk_write will setup a mongoc bulk operation", "[bulk_write]") {
    instance::current();
    mongocxx::client client{mongocxx::uri{}};
    mongocxx::collection coll = client["db"]["coll"];
    auto construct = libmongoc::collection_create_bulk_operation_with_opts.create_instance();
    bool construct_called = false;
    bool ordered_value = false;

    construct->visit([&](mongoc_collection_t*, const bson_t* opts) {
        construct_called = true;
        bson_iter_t iter;
        bson_iter_init(&iter, opts);
        if (bson_iter_find(&iter, "ordered")) {
            REQUIRE(BSON_ITER_HOLDS_BOOL(&iter));
            ordered_value = bson_iter_bool(&iter);
        } else {
            /* if no "ordered" field is passed, libmongoc defaults to true. */
            ordered_value = true;
        }
    });

    SECTION("with an ordered bulk write") {
        { auto bw = coll.create_bulk_write(); }
        REQUIRE(construct_called);
        REQUIRE(ordered_value);
    }

    SECTION("with an unordered bulk write") {
        options::bulk_write bw_opts;
        bw_opts.ordered(false);
        { auto bw = coll.create_bulk_write(bw_opts); }
        REQUIRE(construct_called);
        REQUIRE(!ordered_value);
    }
}
TEST_CASE("destruction of a bulk_write will destroy mongoc operation", "[bulk_write]") {
    instance::current();
    mongocxx::client client{mongocxx::uri{}};
    mongocxx::collection coll = client["db"]["coll"];
    auto destruct = libmongoc::bulk_operation_destroy.create_instance();
    bool destruct_called = false;

    destruct->visit([&destruct_called](mongoc_bulk_operation_t*) { destruct_called = true; });

    { auto bw = coll.create_bulk_write(); }
    REQUIRE(destruct_called);
}
class insert_functor {
   public:
    insert_functor(bool* called, bsoncxx::document::view document)
        : _called{called}, _document{document} {}

    void operator()(mongoc_bulk_operation_t*,
                    const bson_t* document,
                    const bson_t*,
                    bson_error_t*) {
        *_called = true;
        REQUIRE(bson_get_data(document) == _document.data());
    }

   private:
    bool* _called;
    bsoncxx::document::view _document;
};

class update_functor {
   public:
    update_functor(bool* called, bsoncxx::document::view filter, bsoncxx::document::view update)
        : _called{called}, _filter{filter}, _update{update} {}

    void operator()(mongoc_bulk_operation_t*,
                    const bson_t* filter,
                    const bson_t* update,
                    const bson_t* options,
                    bson_error_t*) {
        *_called = true;
        REQUIRE(bson_get_data(filter) == _filter.data());
        REQUIRE(bson_get_data(update) == _update.data());

        bsoncxx::document::view options_view{bson_get_data(options), options->len};

        bsoncxx::document::element collation = options_view["collation"];
        if (_expected_collation) {
            REQUIRE(collation);
            REQUIRE(collation.type() == bsoncxx::type::k_document);
            REQUIRE(collation.get_document().value == *_expected_collation);
        } else {
            REQUIRE(!collation);
        }

        bsoncxx::document::element upsert = options_view["upsert"];
        if (_expected_upsert) {
            REQUIRE(upsert);
            REQUIRE(upsert.type() == bsoncxx::type::k_bool);
            REQUIRE(upsert.get_bool().value);
        } else {
            REQUIRE(!upsert);
        }
    }

    void collation(bsoncxx::document::view collation) {
        _expected_collation = collation;
    }

    void upsert(bool upsert) {
        _expected_upsert = upsert;
    }

   private:
    bool* _called;
    stdx::optional<bsoncxx::document::view> _expected_collation;
    bool _expected_upsert = false;
    bsoncxx::document::view _filter;
    bsoncxx::document::view _update;
};

class delete_functor {
   public:
    delete_functor(bool* called, bsoncxx::document::view filter)
        : _called{called}, _filter{filter} {}

    void operator()(mongoc_bulk_operation_t*,
                    const bson_t* filter,
                    const bson_t* options,
                    bson_error_t*) {
        *_called = true;
        REQUIRE(bson_get_data(filter) == _filter.data());

        bsoncxx::document::view options_view{bson_get_data(options), options->len};

        bsoncxx::document::element collation = options_view["collation"];
        if (_expected_collation) {
            REQUIRE(collation);
            REQUIRE(collation.type() == bsoncxx::type::k_document);
            REQUIRE(collation.get_document().value == *_expected_collation);
        } else {
            REQUIRE(!collation);
        }
    }

    void collation(bsoncxx::document::view collation) {
        _expected_collation = collation;
    }

   private:
    bool* _called;
    stdx::optional<bsoncxx::document::view> _expected_collation;
    bsoncxx::document::view _filter;
};

TEST_CASE("passing write operations to append calls corresponding C function", "[bulk_write]") {
    instance::current();
    mongocxx::client client{mongocxx::uri{}};
    auto bw = client["db"]["coll"].create_bulk_write();
    bsoncxx::builder::basic::document filter_builder, doc_builder, update_doc_builder,
        collation_builder;
    filter_builder.append(kvp("_id", 1));
    doc_builder.append(kvp("_id", 2));
    update_doc_builder.append(kvp("$set", make_document(kvp("_id", 2))));
    collation_builder.append(kvp("locale", "en_US"));

    bsoncxx::document::view filter = filter_builder.view();
    bsoncxx::document::view doc = doc_builder.view();
    bsoncxx::document::view update_doc = update_doc_builder.view();
    bsoncxx::document::view collation = collation_builder.view();

    bool called = false;
    insert_functor insert_func(&called, doc);
    update_functor update_func(&called, filter, update_doc);
    update_functor replace_func(&called, filter, doc);
    delete_functor delete_func(&called, doc);

    SECTION("insert_one invokes mongoc_bulk_operation_insert_with_opts") {
        auto bulk_insert = libmongoc::bulk_operation_insert_with_opts.create_instance();
        bulk_insert->visit(insert_func);

        bw.append(model::insert_one(doc));
        REQUIRE(called);
    }

    SECTION("update_one invokes mongoc_bulk_operation_update_one_with_opts") {
        auto bulk_update = libmongoc::bulk_operation_update_one_with_opts.create_instance();
        bulk_update->visit(update_func);

        bw.append(model::update_one(filter, update_doc));
        REQUIRE(called);
    }

    SECTION(
        "update_one with upsert invokes mongoc_bulk_operation_update_one_with_opts with upsert "
        "true") {
        auto bulk_update = libmongoc::bulk_operation_update_one_with_opts.create_instance();
        update_func.upsert(true);
        bulk_update->visit(update_func);

        model::update_one uo(filter, update_doc);
        uo.upsert(true);
        bw.append(uo);
        REQUIRE(called);
    }

    SECTION(
        "update_one with collation invokes mongoc_bulk_operation_update_one_with_opts with "
        "collation") {
        auto bulk_update = libmongoc::bulk_operation_update_one_with_opts.create_instance();
        update_func.collation(collation);
        bulk_update->visit(update_func);

        model::update_one uo(filter, update_doc);
        uo.collation(collation);
        bw.append(uo);
        REQUIRE(called);
    }

    SECTION("update_many invokes mongoc_bulk_operation_update_many_with_opts") {
        auto bulk_update = libmongoc::bulk_operation_update_many_with_opts.create_instance();
        bulk_update->visit(update_func);

        bw.append(model::update_many(filter, update_doc));
        REQUIRE(called);
    }

    SECTION(
        "update_many with upsert invokes mongoc_bulk_operation_update_many_with_opts with upsert "
        "true") {
        auto bulk_update = libmongoc::bulk_operation_update_many_with_opts.create_instance();
        update_func.upsert(true);
        bulk_update->visit(update_func);

        model::update_many um(filter, update_doc);
        um.upsert(true);
        bw.append(um);
        REQUIRE(called);
    }

    SECTION(
        "update_many with collation invokes mongoc_bulk_operation_update_many_with_opts with "
        "collation") {
        auto bulk_update = libmongoc::bulk_operation_update_many_with_opts.create_instance();
        update_func.collation(collation);
        bulk_update->visit(update_func);

        model::update_many um(filter, update_doc);
        um.collation(collation);
        bw.append(um);
        REQUIRE(called);
    }

    SECTION("delete_one invokes mongoc_bulk_operation_remove_one_with_opts") {
        auto bulk_delete = libmongoc::bulk_operation_remove_one_with_opts.create_instance();
        bulk_delete->visit(delete_func);

        bw.append(model::delete_one(doc));
        REQUIRE(called);
    }

    SECTION(
        "delete_one with collation invokes mongoc_bulk_operation_remove_one_with_opts with "
        "collation") {
        auto bulk_delete = libmongoc::bulk_operation_remove_one_with_opts.create_instance();
        delete_func.collation(collation);
        bulk_delete->visit(delete_func);

        model::delete_one delete_one(doc);
        delete_one.collation(collation);
        bw.append(delete_one);
        REQUIRE(called);
    }

    SECTION("delete_many invokes mongoc_bulk_operation_remove_many_with_opts") {
        auto bulk_delete = libmongoc::bulk_operation_remove_many_with_opts.create_instance();
        bulk_delete->visit(delete_func);

        bw.append(model::delete_many(doc));
        REQUIRE(called);
    }

    SECTION(
        "delete_many with collation invokes mongoc_bulk_operation_remove_many_with_opts with "
        "collation") {
        auto bulk_delete = libmongoc::bulk_operation_remove_many_with_opts.create_instance();
        delete_func.collation(collation);
        bulk_delete->visit(delete_func);

        model::delete_many dm(doc);
        dm.collation(collation);
        bw.append(dm);
        REQUIRE(called);
    }

    SECTION("replace_one invokes mongoc_bulk_operation_replace_one_with_opts") {
        auto bulk_replace = libmongoc::bulk_operation_replace_one_with_opts.create_instance();
        bulk_replace->visit(replace_func);

        bw.append(model::replace_one(filter, doc));
        REQUIRE(called);
    }

    SECTION(
        "replace_one with upsert invokes mongoc_bulk_operation_replace_one_with_opts with upsert "
        "true") {
        auto bulk_replace = libmongoc::bulk_operation_replace_one_with_opts.create_instance();
        replace_func.upsert(true);
        bulk_replace->visit(replace_func);

        model::replace_one ro(filter, doc);
        ro.upsert(true);
        bw.append(ro);
        REQUIRE(called);
    }

    SECTION(
        "replace_one with collation invokes mongoc_bulk_operation_replace_one_with_opts with "
        "collation") {
        auto bulk_replace = libmongoc::bulk_operation_replace_one_with_opts.create_instance();
        replace_func.collation(collation);
        bulk_replace->visit(replace_func);

        model::replace_one ro(filter, doc);
        ro.collation(collation);
        bw.append(ro);
        REQUIRE(called);
    }
}
}  // namespace
