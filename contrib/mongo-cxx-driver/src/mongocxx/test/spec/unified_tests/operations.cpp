// Copyright 2020 MongoDB Inc.
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

#include "operations.hh"

#include <sstream>
#include <vector>

#include <bsoncxx/document/value.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <bsoncxx/types/bson_value/value.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/test/spec/monitoring.hh>
#include <third_party/catch/include/catch.hpp>

#include <mongocxx/config/prelude.hpp>

namespace mongocxx {

using namespace bsoncxx;

using builder::basic::kvp;
using builder::basic::make_document;

int64_t as_int64(const document::element& el) {
    if (el.type() == type::k_int32)
        return static_cast<std::int64_t>(el.get_int32().value);
    return el.get_int64().value;
}

int32_t as_int32(const document::element& el) {
    if (el.type() == type::k_int64)
        return static_cast<std::int32_t>(el.get_int64().value);
    return el.get_int32().value;
}

options::find make_find_options(document::view arguments) {
    options::find options;

    if (const auto batch_size = arguments["batchSize"]) {
        options.batch_size(as_int32(batch_size));
    }

    if (const auto collation = arguments["collation"]) {
        options.collation(collation.get_document().value);
    }

    if (const auto limit = arguments["limit"]) {
        options.limit(as_int64(limit));
    }

    if (const auto skip = arguments["skip"]) {
        options.skip(as_int64(skip));
    }

    if (const auto sort = arguments["sort"]) {
        options.sort(sort.get_document().value);
    }

    if (const auto adu = arguments["allowDiskUse"]) {
        options.allow_disk_use(adu.get_bool().value);
    }

    if (const auto let = arguments["let"]) {
        options.let(let.get_document().value);
    }

    if (const auto comment = arguments["comment"]) {
        options.comment_option(comment.get_value());
    }

    if (const auto modifiers = arguments["modifiers"]) {
        if (const auto comment = modifiers["$comment"]) {
            options.comment(comment.get_string().value);
        }

        if (const auto hint = modifiers["$hint"]) {
            options.hint(mongocxx::hint(hint.get_document().value));
        }

        if (const auto max = modifiers["$max"]) {
            options.max(max.get_document().value);
        }

        if (const auto max_time_ms = modifiers["$maxTimeMS"]) {
            std::chrono::milliseconds my_millis(max_time_ms.get_int32().value);
            options.max_time(my_millis);
        }

        if (const auto min = modifiers["$min"]) {
            options.min(min.get_document().value);
        }

        if (const auto return_key = modifiers["$returnKey"]) {
            options.return_key(return_key.get_bool().value);
        }

        if (const auto show_disk_loc = modifiers["$showDiskLoc"]) {
            options.show_record_id(show_disk_loc.get_bool().value);
        }
    }

    return options;
}

document::value bulk_write_result_to_document(const mongocxx::result::bulk_write& result) {
    builder::basic::document upserted_ids_builder;
    for (auto&& index_and_id : result.upserted_ids()) {
        upserted_ids_builder.append(
            kvp(std::to_string(index_and_id.first), index_and_id.second.get_int32().value));
    }

    return make_document(kvp("matchedCount", result.matched_count()),
                         kvp("modifiedCount", result.matched_count()),
                         kvp("deletedCount", result.deleted_count()),
                         kvp("insertedCount", result.inserted_count()),
                         kvp("upsertedCount", result.upserted_count()),
                         kvp("insertedIds", make_document()),
                         kvp("upsertedIds", upserted_ids_builder.extract()));
}

document::value find(collection& coll, client_session* session, document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::value empty_filter = builder::basic::make_document();
    document::view filter;
    if (const auto f = arguments["filter"]) {
        filter = f.get_document().value;
    } else {
        filter = empty_filter.view();
    }

    options::find options = make_find_options(arguments);

    stdx::optional<cursor> result_cursor;

    if (session) {
        result_cursor.emplace(coll.find(*session, filter, options));
    } else {
        result_cursor.emplace(coll.find(filter, options));
    }

    auto result = builder::basic::document{};
    result.append(builder::basic::kvp("result", [&result_cursor](builder::basic::sub_array array) {
        for (auto&& document : *result_cursor) {
            array.append(document);
        }
    }));

    return result.extract();
}

document::value list_collections(entity::map& map,
                                 client_session* session,
                                 const std::string& object,
                                 document::view op) {
    const auto arguments = op["arguments"];
    const auto empty_doc = make_document();

    document::view filter;
    if (const auto f = arguments["filter"]) {
        filter = f.get_document().value;
    } else {
        filter = empty_doc;
    }

    auto cursor = session ? map.get_database(object).list_collections(*session, filter)
                          : map.get_database(object).list_collections(filter);

    builder::basic::document result;
    result.append(builder::basic::kvp("result", [&cursor](builder::basic::sub_array array) {
        for (auto&& document : cursor) {
            array.append(document);
        }
    }));
    return result.extract();
}

document::value list_collection_names(entity::map& map,
                                      client_session* session,
                                      const std::string& object,
                                      document::view op) {
    const auto arguments = op["arguments"];
    const auto empty_doc = make_document();

    document::view filter;
    if (const auto f = arguments["filter"]) {
        filter = f.get_document().value;
    } else {
        filter = empty_doc;
    }

    auto cursor = session ? map.get_database(object).list_collection_names(*session, filter)
                          : map.get_database(object).list_collection_names(filter);

    builder::basic::document result;
    result.append(builder::basic::kvp("result", [&cursor](builder::basic::sub_array array) {
        for (auto&& document : cursor) {
            array.append(document);
        }
    }));
    return result.extract();
}

document::value list_databases(entity::map& map,
                               client_session* session,
                               const std::string& object,
                               document::view op) {
    const auto arguments = op["arguments"];
    const auto empty_doc = make_document();
    const auto arguments_view = arguments ? arguments.get_document().value : empty_doc.view();

    auto cursor = session ? map.get_client(object).list_databases(*session, arguments_view)
                          : map.get_client(object).list_databases(arguments_view);

    builder::basic::document result;
    result.append(builder::basic::kvp("result", [&cursor](builder::basic::sub_array array) {
        for (auto&& document : cursor) {
            array.append(document);
        }
    }));
    return result.extract();
}

document::value list_database_names(entity::map& map,
                                    client_session* session,
                                    const std::string& object,
                                    document::view op) {
    const auto arguments = op["arguments"];
    const auto empty_doc = make_document();

    document::view filter;
    if (const auto f = arguments["filter"]) {
        filter = f.get_document().value;
    } else {
        filter = empty_doc;
    }

    auto cursor = session ? map.get_client(object).list_database_names(*session, filter)
                          : map.get_client(object).list_database_names(filter);

    builder::basic::document result;
    result.append(builder::basic::kvp("result", [&cursor](builder::basic::sub_array array) {
        for (auto&& document : cursor) {
            array.append(document);
        }
    }));
    return result.extract();
}

document::value list_indexes(entity::map& map, client_session* session, const std::string& object) {
    auto cursor = session ? map.get_collection(object).list_indexes(*session)
                          : map.get_collection(object).list_indexes();

    builder::basic::document result;
    result.append(builder::basic::kvp("result", [&cursor](builder::basic::sub_array array) {
        for (auto&& document : cursor) {
            array.append(document);
        }
    }));
    return result.extract();
}

template <typename Model>
void add_hint_to_model(Model model, document::view doc) {
    if (doc["hint"]) {
        if (doc["hint"].type() == bsoncxx::type::k_string)
            model.hint(hint{doc["hint"].get_string().value});
        else
            model.hint(hint{doc["hint"].get_document().value});
    }
}

pipeline build_pipeline(array::view pipeline_docs) {
    pipeline pipeline{};
    pipeline.append_stages(std::move(pipeline_docs));
    return pipeline;
}

template <typename T>
T _build_update_model(document::view arguments) {
    document::view filter = arguments["filter"].get_document().value;

    switch (arguments["update"].type()) {
        case bsoncxx::type::k_document: {
            return T(filter, arguments["update"].get_document().value);
        }
        case bsoncxx::type::k_array: {
            pipeline update = build_pipeline(arguments["update"].get_array().value);
            return T(filter, update);
        }
        default:
            throw std::logic_error{"update must be a document or an array"};
    }
}

template <typename Model>
void set_hint(Model& model, const mongocxx::document::element& hint) {
    if (hint.type() == bsoncxx::type::k_string) {
        model.hint(mongocxx::hint(hint.get_string().value));
    } else {
        model.hint(mongocxx::hint(hint.get_document().value));
    }
}

document::value bulk_write(collection& coll, client_session* session, document::view op) {
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;

    options::bulk_write opt;
    std::vector<model::write> writes;

    auto arguments = op["arguments"].get_document().value;
    if (auto options = arguments["options"]) {
        document::view options_doc = options.get_document().value;
        if (auto ordered = options_doc["ordered"]) {
            opt.ordered(ordered.get_bool().value);
        }
    }
    if (auto let = arguments["let"]) {
        opt.let(let.get_document().value);
    }
    if (auto comment = arguments["comment"]) {
        opt.comment(comment.get_value());
    }

    auto requests = arguments["requests"].get_array().value;
    for (auto&& request_element : requests) {
        auto operation_name = request_element.get_document().value.begin()->key();
        auto request_arguments = request_element[operation_name].get_document().value;
        CAPTURE(to_json(request_arguments), operation_name);

        if (operation_name.compare("updateOne") == 0) {
            auto update_one = _build_update_model<model::update_one>(request_arguments);

            add_hint_to_model(update_one, request_arguments);

            if (request_arguments["collation"]) {
                update_one.collation(request_arguments["collation"].get_document().value);
            }

            if (const auto hint = request_arguments["hint"]) {
                set_hint(update_one, hint);
            }

            if (request_arguments["upsert"]) {
                update_one.upsert(request_arguments["upsert"].get_bool().value);
            }

            if (request_arguments["arrayFilters"]) {
                update_one.array_filters(request_arguments["arrayFilters"].get_array().value);
            }

            writes.emplace_back(update_one);
        } else if (operation_name.compare("updateMany") == 0) {
            auto update_many = _build_update_model<model::update_many>(request_arguments);

            add_hint_to_model(update_many, request_arguments);

            if (request_arguments["collation"]) {
                update_many.collation(request_arguments["collation"].get_document().value);
            }

            if (const auto hint = request_arguments["hint"]) {
                set_hint(update_many, hint);
            }

            if (request_arguments["upsert"]) {
                update_many.upsert(request_arguments["upsert"].get_bool().value);
            }

            if (request_arguments["arrayFilters"]) {
                update_many.array_filters(request_arguments["arrayFilters"].get_array().value);
            }

            writes.emplace_back(update_many);
        } else if (operation_name.compare("replaceOne") == 0) {
            document::view filter = request_arguments["filter"].get_document().value;
            document::view replacement = request_arguments["replacement"].get_document().value;
            model::replace_one replace_one(filter, replacement);

            add_hint_to_model(replace_one, request_arguments);

            if (request_arguments["collation"]) {
                replace_one.collation(request_arguments["collation"].get_document().value);
            }

            if (const auto hint = request_arguments["hint"]) {
                set_hint(replace_one, hint);
            }

            if (request_arguments["upsert"]) {
                replace_one.upsert(request_arguments["upsert"].get_bool().value);
            }

            writes.emplace_back(replace_one);
        } else if (operation_name.compare("insertOne") == 0) {
            document::view document = request_arguments["document"].get_document().value;
            model::insert_one insert_one(document);
            writes.emplace_back(insert_one);
        } else if (operation_name.compare("deleteOne") == 0) {
            document::view filter = request_arguments["filter"].get_document().value;
            model::delete_one delete_one(filter);
            add_hint_to_model(delete_one, request_arguments);

            if (request_arguments["collation"]) {
                delete_one.collation(request_arguments["collation"].get_document().value);
            }

            if (const auto hint = request_arguments["hint"]) {
                set_hint(delete_one, hint);
            }

            writes.emplace_back(delete_one);
        } else if (operation_name.compare("deleteMany") == 0) {
            document::view filter = request_arguments["filter"].get_document().value;
            model::delete_many delete_many(filter);
            add_hint_to_model(delete_many, request_arguments);

            if (request_arguments["collation"]) {
                delete_many.collation(request_arguments["collation"].get_document().value);
            }

            if (const auto hint = request_arguments["hint"]) {
                set_hint(delete_many, hint);
            }

            writes.emplace_back(delete_many);
        } else {
            /* should not happen. */
            FAIL("unrecognized operation name: " + string::to_string(operation_name));
        }
    }

    bsoncxx::stdx::optional<result::bulk_write> bulk_write_result;

    if (session) {
        bulk_write_result = coll.bulk_write(*session, writes, opt);
    } else {
        bulk_write_result = coll.bulk_write(writes, opt);
    }

    builder::basic::document result;
    builder::basic::document upserted_ids_builder;

    // Construct the result document.
    // Note: insertedIds is currently hard coded as an empty document, because result::bulk_write
    // provides no way to see inserted ids. This is compliant with the CRUD spec, as insertedIds
    // are: "NOT REQUIRED: Drivers may choose to not provide this property." So just add an empty
    // document for insertedIds. There are no current bulk write tests testing insert operations.
    // The insertedIds field in current bulk write spec tests is always an empty document.
    result.append(builder::basic::kvp("result", [&](builder::basic::sub_document subdoc) {
        if (!bulk_write_result) {
            return;
        }

        subdoc.append(builder::basic::kvp("matchedCount", bulk_write_result->matched_count()));
        subdoc.append(builder::basic::kvp("modifiedCount", bulk_write_result->modified_count()));
        subdoc.append(builder::basic::kvp("upsertedCount", bulk_write_result->upserted_count()));
        subdoc.append(builder::basic::kvp("deletedCount", bulk_write_result->deleted_count()));
        subdoc.append(builder::basic::kvp("insertedCount", bulk_write_result->inserted_count()));

        for (auto&& index_and_id : bulk_write_result->upserted_ids()) {
            upserted_ids_builder.append(
                kvp(std::to_string(index_and_id.first), index_and_id.second.get_int32().value));
        }
        subdoc.append(builder::basic::kvp("upsertedIds", upserted_ids_builder.extract()));
    }));

    return result.extract();
}

document::value insert_many(collection& coll, client_session* session, document::view operation) {
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;

    document::view arguments = operation["arguments"].get_document().value;
    array::view documents = arguments["documents"].get_array().value;
    std::vector<document::view> documents_to_insert{};
    options::insert insert_options;

    if (const auto ordered = arguments["ordered"]) {
        insert_options.ordered(ordered.get_bool().value);
    }

    if (const auto comment = arguments["comment"]) {
        insert_options.comment(comment.get_value());
    }

    for (auto&& element : documents) {
        documents_to_insert.push_back(element.get_document().value);
    }

    stdx::optional<result::insert_many> insert_many_result;

    if (session) {
        insert_many_result = coll.insert_many(*session, documents_to_insert, insert_options);
    } else {
        insert_many_result = coll.insert_many(documents_to_insert, insert_options);
    }

    std::map<size_t, document::element> inserted_ids{};

    if (insert_many_result) {
        inserted_ids = insert_many_result->inserted_ids();
    }

    auto result = builder::basic::document{};
    bsoncxx::builder::basic::document inserted_ids_builder;

    for (auto&& id_pair : inserted_ids) {
        inserted_ids_builder.append(kvp(std::to_string(id_pair.first), id_pair.second.get_value()));
    }

    result.append(kvp("result",
                      make_document(kvp("insertedIds", inserted_ids_builder.extract()),
                                    kvp("insertedCount", insert_many_result->inserted_count()))));
    return result.extract();
}

document::value replace_one(collection& coll, client_session* session, document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::view filter = arguments["filter"].get_document().value;
    document::view replacement = arguments["replacement"].get_document().value;
    options::replace options{};

    if (const auto collation = arguments["collation"]) {
        options.collation(collation.get_document().value);
    }

    if (const auto hint = arguments["hint"]) {
        if (hint.type() == bsoncxx::type::k_string)
            options.hint(mongocxx::hint(hint.get_string().value));
        else
            options.hint(mongocxx::hint(hint.get_document().value));
    }

    if (const auto let = arguments["let"]) {
        options.let(let.get_document().value);
    }

    if (const auto comment = arguments["comment"]) {
        options.comment(comment.get_value());
    }

    if (const auto upsert = arguments["upsert"]) {
        options.upsert(upsert.get_bool().value);
    }

    std::int32_t matched_count = 0;
    bsoncxx::stdx::optional<std::int32_t> modified_count;
    std::int32_t upserted_count = 0;
    stdx::optional<result::replace_one> replace_result;

    if (session) {
        replace_result = coll.replace_one(*session, filter, replacement, options);
    } else {
        replace_result = coll.replace_one(filter, replacement, options);
    }

    bsoncxx::stdx::optional<types::bson_value::view> upserted_id{};

    if (replace_result) {
        matched_count = replace_result->matched_count();
        modified_count = replace_result->modified_count();

        upserted_count = replace_result->result().upserted_count();

        if (auto upserted_element = replace_result->upserted_id()) {
            upserted_id = upserted_element->get_value();
        }
    }

    auto result = builder::basic::document{};
    result.append(builder::basic::kvp(
        "result",
        [matched_count, modified_count, upserted_count, upserted_id](
            builder::basic::sub_document subdoc) {
            subdoc.append(builder::basic::kvp("matchedCount", matched_count));

            if (modified_count) {
                subdoc.append(builder::basic::kvp("modifiedCount", *modified_count));
            }

            subdoc.append(builder::basic::kvp("upsertedCount", upserted_count));

            if (upserted_id) {
                subdoc.append(builder::basic::kvp("upsertedId", *upserted_id));
            }
        }));

    return result.extract();
}

template <typename Entity>
document::value aggregate(Entity& entity, client_session* session, document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    pipeline pipeline = build_pipeline(arguments["pipeline"].get_array().value);
    options::aggregate options{};

    if (const auto batch_size = arguments["batchSize"]) {
        options.batch_size(batch_size.get_int32().value);
    }

    if (const auto collation = arguments["collation"]) {
        options.collation(collation.get_document().value);
    }

    if (const auto let = arguments["let"]) {
        options.let(let.get_document().value);
    }

    if (const auto comment = arguments["comment"]) {
        options.comment(comment.get_value());
    }

    if (const auto allow_disk_use = arguments["allowDiskUse"]) {
        options.allow_disk_use(allow_disk_use.get_bool());
    }

    stdx::optional<cursor> result_cursor;

    if (session) {
        result_cursor.emplace(entity.aggregate(*session, pipeline, options));
    } else {
        result_cursor.emplace(entity.aggregate(pipeline, options));
    }

    auto result = builder::basic::document{};
    result.append(builder::basic::kvp("result", [&result_cursor](builder::basic::sub_array array) {
        for (auto&& document : *result_cursor) {
            array.append(document);
        }
    }));

    return result.extract();
}

document::value insert_one(collection& coll, client_session* session, document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::view document = arguments["document"].get_document().value;

    options::insert opts{};

    if (const auto bdv = arguments["bypassDocumentValidation"]) {
        opts.bypass_document_validation(bdv.get_bool().value);
    }

    if (const auto comment = arguments["comment"]) {
        opts.comment(comment.get_value());
    }

    stdx::optional<result::insert_one> insert_one_result;
    if (session) {
        insert_one_result = coll.insert_one(*session, document, opts);
    } else {
        insert_one_result = coll.insert_one(document, opts);
    }

    types::bson_value::view inserted_id{types::b_null{}};

    if (insert_one_result) {
        inserted_id = insert_one_result->inserted_id();
    }

    auto result = builder::basic::document{};
    result.append(builder::basic::kvp("result", [inserted_id](builder::basic::sub_document subdoc) {
        subdoc.append(builder::basic::kvp("insertedId", inserted_id));
    }));

    return result.extract();
}

document::value create_change_stream(entity::map& map,
                                     client_session* session,
                                     const std::string& object,
                                     document::view operation) {
    auto args = operation["arguments"];
    auto pipeline = build_pipeline(args["pipeline"].get_array().value);

    options::change_stream options{};

    if (const auto batch_size = args["batchSize"]) {
        options.batch_size(batch_size.get_int32().value);
    }

    if (const auto comment = args["comment"]) {
        options.comment(comment.get_value());
    }

    if (const auto full_document = args["fullDocument"]) {
        options.full_document(full_document.get_string().value);
    }

    if (const auto full_document_before_change = args["fullDocumentBeforeChange"]) {
        options.full_document_before_change(full_document_before_change.get_string().value);
    }

    const auto save_result_as_entity = operation["saveResultAsEntity"];
    const auto key = save_result_as_entity
                         ? string::to_string(save_result_as_entity.get_string().value)
                         : std::string();
    CAPTURE(object, to_json(operation), key);
    auto stream = [&]() -> mongocxx::change_stream {
        const auto& type = map.type(object);
        if (type == typeid(mongocxx::database)) {
            if (session)
                return map.get_database(object).watch(*session, pipeline, options);
            return map.get_database(object).watch(pipeline, options);
        }

        if (type == typeid(mongocxx::collection)) {
            if (session)
                return map.get_collection(object).watch(*session, pipeline, options);
            return map.get_collection(object).watch(pipeline, options);
        }

        if (session)
            return map.get_client(object).watch(*session, pipeline, options);
        return map.get_client(object).watch(pipeline, options);
    }();

    if (!key.empty()) {
        auto res = map.insert(key, std::move(stream));
        REQUIRE(res);
    }

    return make_document();
}

document::value iterate_until_document_or_error(
    change_stream& stream,
    std::unordered_map<mongocxx::change_stream*, mongocxx::change_stream::iterator>& stream_iters) {
    const auto stream_iter = stream_iters.find(&stream);
    const auto is_first = stream_iter == stream_iters.end();

    auto& iter = is_first ? stream_iters.emplace(&stream, stream.begin()).first->second
                          : stream_iter->second;

    // `.begin()` performs iterator increment on first call for a given stream.
    // Ensure a double-increment does not take place on any given call to
    // iterate_until_document_or_error.
    if (!is_first) {
        ++iter;
    }

    // Loop until document or error.
    while (iter == stream.end()) {
        iter = stream.begin();
    }

    return make_document(kvp("result", iter != stream.end() ? *iter : make_document()));
}

document::value iterate_until_document_or_error(
    cursor& cursor,
    std::unordered_map<mongocxx::cursor*, mongocxx::cursor::iterator>& cursor_iters) {
    const auto cursor_iter = cursor_iters.find(&cursor);
    const auto is_first = cursor_iter == cursor_iters.end();

    auto& iter = is_first ? cursor_iters.emplace(&cursor, cursor.begin()).first->second
                          : cursor_iter->second;

    // `.begin()` performs iterator increment on first call for a given cursor.
    // Ensure a double-increment does not take place on any given call to
    // iterate_until_document_or_error.
    if (!is_first) {
        ++iter;
    }

    // Loop until document or error.
    while (iter == cursor.end()) {
        iter = cursor.begin();
    }

    return make_document(kvp("result", iter != cursor.end() ? *iter : make_document()));
}

document::value fail_point(entity::map& map, spec::apm_checker& apm, document::view op) {
    auto args = op["arguments"];
    auto client_name = string::to_string(args["client"].get_string().value);
    auto& client = map.get_client(client_name);

    // The test runner MUST also ensure that the `configureFailPoint` command is excluded from the
    // list of observed command monitoring events for this client (if applicable).
    apm.set_ignore_command_monitoring_event("configureFailPoint");

    client["admin"].run_command(args["failPoint"].get_document().value);

    return make_document(kvp("uri", client.uri().to_string()),
                         kvp("failPoint", args["failPoint"]["configureFailPoint"].get_string()));
}

document::value rename(mongocxx::collection& coll, document::view op) {
    const auto args = op["arguments"];
    const auto to = args["to"];

    coll.rename(to.get_string().value);

    return make_document();
}

document::value find_one_and_delete(collection& coll,
                                    client_session* session,
                                    document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::view filter = arguments["filter"].get_document().value;
    options::find_one_and_delete options{};

    if (const auto collation = arguments["collation"]) {
        options.collation(collation.get_document().value);
    }

    if (const auto hint = arguments["hint"]) {
        if (hint.type() == bsoncxx::type::k_string)
            options.hint(mongocxx::hint(hint.get_string().value));
        else
            options.hint(mongocxx::hint(hint.get_document().value));
    }

    if (const auto let = arguments["let"]) {
        options.let(let.get_document().value);
    }

    if (const auto comment = arguments["comment"]) {
        options.comment(comment.get_value());
    }

    if (const auto projection = arguments["projection"]) {
        options.projection(projection.get_document().value);
    }

    if (const auto sort = arguments["sort"]) {
        options.sort(sort.get_document().value);
    }

    stdx::optional<document::value> document;
    if (session) {
        document = coll.find_one_and_delete(*session, filter, options);
    } else {
        document = coll.find_one_and_delete(filter, options);
    }

    // Using an unacknowledged write concern returns an empty document rather than null when no
    // documents match.
    auto result = builder::basic::document{};
    if (document && !(document->view().empty())) {
        result.append(builder::basic::kvp("result", *document));
    } else {
        result.append(builder::basic::kvp("result", types::b_null{}));
    }

    return result.extract();
}

document::value find_one_and_replace(collection& coll,
                                     client_session* session,
                                     document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::view filter = arguments["filter"].get_document().value;
    options::find_one_and_replace options{};

    if (const auto collation = arguments["collation"]) {
        options.collation(collation.get_document().value);
    }

    if (const auto hint = arguments["hint"]) {
        if (hint.type() == bsoncxx::type::k_string)
            options.hint(mongocxx::hint(hint.get_string().value));
        else
            options.hint(mongocxx::hint(hint.get_document().value));
    }

    if (const auto let = arguments["let"]) {
        options.let(let.get_document().value);
    }

    if (const auto comment = arguments["comment"]) {
        options.comment(comment.get_value());
    }

    if (const auto projection = arguments["projection"]) {
        options.projection(projection.get_document().value);
    }

    if (const auto rd = arguments["returnDocument"]) {
        const auto return_document = string::to_string(rd.get_string().value);

        if (return_document == "After") {
            options.return_document(options::return_document::k_after);
        } else if (return_document == "Before") {
            options.return_document(options::return_document::k_before);
        } else {
            throw std::logic_error{"unrecognized value for returnDocument: " + return_document};
        }
    }

    if (const auto sort = arguments["sort"]) {
        options.sort(sort.get_document().value);
    }

    if (const auto upsert = arguments["upsert"]) {
        options.upsert(upsert.get_bool().value);
    }

    stdx::optional<document::value> document;
    document::view replacement = arguments["replacement"].get_document().value;
    if (session) {
        document = coll.find_one_and_replace(*session, filter, replacement, options);
    } else {
        document = coll.find_one_and_replace(filter, replacement, options);
    }

    // Using an unacknowledged write concern returns an empty document rather than null when no
    // documents match.
    auto result = builder::basic::document{};
    if (document && !(document->view().empty())) {
        result.append(builder::basic::kvp("result", *document));
    } else {
        result.append(builder::basic::kvp("result", types::b_null{}));
    }

    return result.extract();
}

document::value find_one_and_update(collection& coll,
                                    client_session* session,
                                    document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::view filter = arguments["filter"].get_document().value;
    options::find_one_and_update options{};

    if (const auto collation = arguments["collation"]) {
        options.collation(collation.get_document().value);
    }

    if (const auto hint = arguments["hint"]) {
        if (hint.type() == bsoncxx::type::k_string)
            options.hint(mongocxx::hint(hint.get_string().value));
        else
            options.hint(mongocxx::hint(hint.get_document().value));
    }

    if (const auto let = arguments["let"]) {
        options.let(let.get_document().value);
    }

    if (const auto comment = arguments["comment"]) {
        options.comment(comment.get_value());
    }

    if (const auto projection = arguments["projection"]) {
        options.projection(projection.get_document().value);
    }

    if (const auto array_filters = arguments["arrayFilters"]) {
        options.array_filters(array_filters.get_array().value);
    }

    if (const auto rd = arguments["returnDocument"]) {
        auto return_document = string::to_string(rd.get_string().value);

        if (return_document == "After") {
            options.return_document(options::return_document::k_after);
        }

        if (return_document == "Before") {
            options.return_document(options::return_document::k_before);
        }
    }

    if (const auto sort = arguments["sort"]) {
        options.sort(sort.get_document().value);
    }

    if (const auto upsert = arguments["upsert"]) {
        options.upsert(upsert.get_bool().value);
    }

    auto result = builder::basic::document{};
    stdx::optional<document::value> document;

    switch (arguments["update"].type()) {
        case bsoncxx::type::k_document: {
            document::view update = arguments["update"].get_document().value;
            if (session) {
                document = coll.find_one_and_update(*session, filter, update, options);
            } else {
                document = coll.find_one_and_update(filter, update, options);
            }
            break;
        }
        case bsoncxx::type::k_array: {
            pipeline update = build_pipeline(arguments["update"].get_array().value);
            if (session) {
                document = coll.find_one_and_update(*session, filter, update, options);
            } else {
                document = coll.find_one_and_update(filter, update, options);
            }
            break;
        }
        default:
            throw std::logic_error{"update must be a document or an array"};
    }

    // Server versions below 3.0 sometimes return an empty document rather than null when no
    // documents match.
    if (document && !(document->view().empty())) {
        result.append(builder::basic::kvp("result", *document));
    } else {
        result.append(builder::basic::kvp("result", types::b_null{}));
    }

    return result.extract();
}

bsoncxx::stdx::optional<read_concern> operations::lookup_read_concern(document::view doc) {
    if (doc["readConcern"] && doc["readConcern"]["level"]) {
        read_concern rc;
        rc.acknowledge_string(string::to_string(doc["readConcern"]["level"].get_string().value));
        return rc;
    }

    return {};
}

bsoncxx::stdx::optional<write_concern> operations::lookup_write_concern(document::view doc) {
    if (doc["writeConcern"] && doc["writeConcern"]["w"]) {
        write_concern wc;
        document::element w = doc["writeConcern"]["w"];
        if (w.type() == bsoncxx::type::k_string) {
            std::string level = string::to_string(w.get_string().value);
            if (level == "majority") {
                wc.acknowledge_level(write_concern::level::k_majority);
            } else if (level == "acknowledged") {
                wc.acknowledge_level(write_concern::level::k_acknowledged);
            } else if (level == "unacknowledged") {
                wc.acknowledge_level(write_concern::level::k_unacknowledged);
            }
        } else if (w.type() == bsoncxx::type::k_int32) {
            wc.nodes(w.get_int32());
        }
        return wc;
    }

    return {};
}

bsoncxx::stdx::optional<read_preference> operations::lookup_read_preference(document::view doc) {
    if (doc["readPreference"] && doc["readPreference"]["mode"]) {
        read_preference rp;
        std::string mode = string::to_string(doc["readPreference"]["mode"].get_string().value);
        if (mode == "Primary") {
            rp.mode(read_preference::read_mode::k_primary);
        } else if (mode == "PrimaryPreferred") {
            rp.mode(read_preference::read_mode::k_primary_preferred);
        } else if (mode == "Secondary") {
            rp.mode(read_preference::read_mode::k_secondary);
        } else if (mode == "SecondaryPreferred") {
            rp.mode(read_preference::read_mode::k_secondary_preferred);
        } else if (mode == "Nearest") {
            rp.mode(read_preference::read_mode::k_nearest);
        }
        return rp;
    }

    return {};
}

options::transaction set_opts(document::view args) {
    options::transaction txn_opts;

    if (auto rc = operations::lookup_read_concern(args)) {
        txn_opts.read_concern(*rc);
    }

    if (auto wc = operations::lookup_write_concern(args)) {
        txn_opts.write_concern(*wc);
    }

    if (auto rp = operations::lookup_read_preference(args)) {
        txn_opts.read_preference(*rp);
    }

    return txn_opts;
}

document::value start_transaction(client_session& session, bsoncxx::document::view operation) {
    if (operation["arguments"]) {
        options::transaction txn_opts = set_opts(operation["arguments"].get_document());
        session.start_transaction(txn_opts);
    } else {
        session.start_transaction();
    }

    return bsoncxx::builder::basic::make_document();
}

document::value assert_session_transaction_state(client_session& session, document::view op) {
    auto state = string::to_string(op["arguments"]["state"].get_string().value);
    switch (session.get_transaction_state()) {
        case client_session::transaction_state::k_transaction_none:
            REQUIRE(state == "none");
            break;
        case client_session::transaction_state::k_transaction_starting:
            REQUIRE(state == "starting");
            break;
        case client_session::transaction_state::k_transaction_in_progress:
            REQUIRE(state == "in_progress");
            break;
        case client_session::transaction_state::k_transaction_committed:
            REQUIRE(state == "committed");
            break;
        case client_session::transaction_state::k_transaction_aborted:
            REQUIRE(state == "aborted");
            break;
    }
    return make_document();
}

bool collection_exists(document::view op) {
    client client{uri{}, test_util::add_test_server_api()};
    auto coll_name = string::to_string(op["arguments"]["collectionName"].get_string().value);
    auto db_name = string::to_string(op["arguments"]["databaseName"].get_string().value);
    return client.database(db_name).has_collection(coll_name);
}

document::value create_index(collection& coll,
                             client_session* session,
                             const document::view& operation) {
    auto arguments = operation["arguments"];

    auto name = arguments["name"].get_string().value;
    auto keys = arguments["keys"].get_document().value;

    bsoncxx::builder::basic::document opts;
    opts.append(bsoncxx::builder::basic::kvp("name", name));

    if (session)
        return make_document(kvp("result", coll.create_index(*session, keys, opts.extract())));
    return make_document(kvp("result", coll.create_index(keys, opts.extract())));
}

bool index_exists(document::view op) {
    client client{uri{}, test_util::add_test_server_api()};
    auto coll_name = string::to_string(op["arguments"]["collectionName"].get_string().value);
    auto db_name = string::to_string(op["arguments"]["databaseName"].get_string().value);
    auto coll = client.database(db_name).collection(coll_name);

    auto indexes = coll.indexes().list();
    auto expected_name = string::to_string(op["arguments"]["indexName"].get_string().value);

    return std::any_of(indexes.begin(), indexes.end(), [&](document::view index) {
        return expected_name == string::to_string(index["name"].get_string().value);
    });
}

document::value download(gridfs::bucket& bucket, document::view op) {
    using bsoncxx::types::bson_value::value;

    auto id = op["arguments"]["id"].get_value();

    std::ostringstream result{};
    bucket.download_to_stream(id, &result);

    auto result_str = result.str();
    std::vector<std::uint8_t> bytes{result_str.begin(), result_str.end()};

    return make_document(kvp("result", value(bytes)));
}

document::value upload(entity::map& map, document::view op) {
    const auto save_result_as_entity = op["saveResultAsEntity"];
    const auto key = save_result_as_entity
                         ? string::to_string(save_result_as_entity.get_string().value)
                         : std::string();
    const auto arguments = op["arguments"].get_document().value;
    const auto object = string::to_string(op["object"].get_string().value);
    auto& bucket = map.get_bucket(object);

    options::gridfs::upload upload_options;

    if (arguments["chunkSizeBytes"]) {
        upload_options.chunk_size_bytes(arguments["chunkSizeBytes"].get_int32().value);
    }

    if (arguments["metadata"]) {
        upload_options.metadata(arguments["metadata"].get_document().value);
    }

    auto filename = arguments["filename"].get_string().value;
    auto uploader = bucket.open_upload_stream(filename, upload_options);
    auto source = arguments["source"].get_document().value;
    auto hex = source["$$hexBytes"].get_string().value;
    auto source_bytes = test_util::convert_hex_string_to_bytes(hex);

    uploader.write(source_bytes.data(), source_bytes.size());
    auto upload_result = uploader.close();
    auto id = upload_result.id();

    if (!key.empty()) {
        map.insert(key, bsoncxx::types::bson_value::value(id));
    }

    return make_document(kvp("result", id));
}

document::value delete_one(collection& coll, client_session* session, document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::view filter = arguments["filter"].get_document().value;
    options::delete_options options{};

    if (const auto collation = arguments["collation"]) {
        options.collation(collation.get_document().value);
    }

    if (const auto hint = arguments["hint"]) {
        if (hint.type() == bsoncxx::type::k_string)
            options.hint(mongocxx::hint(hint.get_string().value));
        else
            options.hint(mongocxx::hint(hint.get_document().value));
    }

    if (const auto let = arguments["let"]) {
        options.let(let.get_document().value);
    }

    if (auto comment = arguments["comment"]) {
        options.comment(comment.get_value());
    }

    auto result = builder::basic::document{};
    std::int32_t deleted_count = 0;

    auto delete_result = [&] {
        if (session)
            return coll.delete_one(*session, filter, options);
        return coll.delete_one(filter, options);
    }();

    if (delete_result) {
        deleted_count = delete_result->deleted_count();
    }

    result.append(
        builder::basic::kvp("result", [deleted_count](builder::basic::sub_document subdoc) {
            subdoc.append(builder::basic::kvp("deletedCount", deleted_count));
        }));

    return result.extract();
}

document::value delete_many(collection& coll, client_session* session, document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::view filter = arguments["filter"].get_document().value;
    options::delete_options options{};

    if (const auto collation = arguments["collation"]) {
        options.collation(collation.get_document().value);
    }

    if (const auto hint = arguments["hint"]) {
        if (hint.type() == bsoncxx::type::k_string) {
            options.hint(mongocxx::hint(hint.get_string().value));
        } else {
            options.hint(mongocxx::hint(hint.get_document().value));
        }
    }

    if (const auto let = arguments["let"]) {
        options.let(let.get_document().value);
    }

    if (const auto comment = arguments["comment"]) {
        options.comment(comment.get_value());
    }

    auto result = builder::basic::document{};
    std::int32_t deleted_count = 0;

    stdx::optional<result::delete_result> delete_result;
    if (session) {
        delete_result = coll.delete_many(*session, filter, options);
    } else {
        delete_result = coll.delete_many(filter, options);
    }

    if (delete_result) {
        deleted_count = delete_result->deleted_count();
    }
    result.append(
        builder::basic::kvp("result", [deleted_count](builder::basic::sub_document subdoc) {
            subdoc.append(builder::basic::kvp("deletedCount", deleted_count));
        }));

    return result.extract();
}

document::value with_transaction(client_session& session,
                                 document::view op,
                                 client_session::with_transaction_cb cb) {
    if (op["arguments"]) {
        auto opts = set_opts(op["arguments"].get_document());
        session.with_transaction(cb, opts);
    } else {
        session.with_transaction(cb);
    }
    return make_document();
}

document::value end_session(entity::map& map, const std::string& name) {
    auto& session = map.get_client_session(name);
    auto id = types::bson_value::value(session.id());

    map.erase(name);
    map.insert(name, bsoncxx::types::bson_value::value(id));
    return make_document();
}

client_session* get_session(document::view op, entity::map& map) {
    if (!op["arguments"]["session"])
        return nullptr;

    auto session_name = string::to_string(op["arguments"]["session"].get_string().value);
    return &map.get_client_session(session_name);
}

document::value run_command(database& db, client_session* session, document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::view command = arguments["command"].get_document().value;

    auto result = builder::basic::document{};
    if (session) {
        result.append(builder::basic::kvp("result", db.run_command(*session, command)));
    } else {
        result.append(builder::basic::kvp("result", db.run_command(command)));
    }
    return result.extract();
}

document::value update_one(collection& coll, client_session* session, document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::view filter = arguments["filter"].get_document().value;
    options::update options{};

    if (const auto collation = arguments["collation"]) {
        options.collation(collation.get_document().value);
    }

    if (const auto hint = arguments["hint"]) {
        if (arguments["hint"].type() == bsoncxx::type::k_string)
            options.hint(mongocxx::hint(hint.get_string().value));
        else
            options.hint(mongocxx::hint(hint.get_document().value));
    }

    if (const auto let = arguments["let"]) {
        options.let(let.get_document().value);
    }

    if (const auto comment = arguments["comment"]) {
        options.comment(comment.get_value());
    }

    if (const auto upsert = arguments["upsert"]) {
        options.upsert(upsert.get_bool().value);
    }

    stdx::optional<result::update> update_one_result;

    {
        const auto update = arguments["update"];
        const auto type = update.type();

        switch (type) {
            case bsoncxx::type::k_document: {
                const auto doc = update.get_document().value;
                update_one_result = session ? coll.update_one(*session, filter, doc, options)
                                            : coll.update_one(filter, doc, options);
                break;
            }
            case bsoncxx::type::k_array: {
                pipeline pipe;
                pipe.append_stages(update.get_array().value);
                update_one_result = session ? coll.update_one(*session, filter, pipe, options)
                                            : coll.update_one(filter, pipe, options);
                break;
            }
            default:
                throw std::logic_error("unexpected type '" + to_string(type) +
                                       "' for field 'update'");
        }
    }

    auto result = builder::basic::document{};
    result.append(builder::basic::kvp("result", [&](builder::basic::sub_document subdoc) {
        if (!update_one_result) {
            return;
        }

        subdoc.append(builder::basic::kvp("matchedCount", update_one_result->matched_count()));
        subdoc.append(builder::basic::kvp("modifiedCount", update_one_result->modified_count()));
        subdoc.append(builder::basic::kvp("upsertedCount", update_one_result->upserted_count()));

        if (auto upserted_element = update_one_result->upserted_id()) {
            subdoc.append(builder::basic::kvp("upsertedId", upserted_element->get_value()));
        }
    }));

    return result.extract();
}

document::value update_many(collection& coll, document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::view filter = arguments["filter"].get_document().value;
    options::update options{};

    if (const auto collation = arguments["collation"]) {
        options.collation(collation.get_document().value);
    }

    if (const auto hint = arguments["hint"]) {
        if (hint.type() == bsoncxx::type::k_string)
            options.hint(mongocxx::hint(hint.get_string().value));
        else
            options.hint(mongocxx::hint(hint.get_document().value));
    }

    if (const auto let = arguments["let"]) {
        options.let(let.get_document().value);
    }

    if (const auto comment = arguments["comment"]) {
        options.comment(comment.get_value());
    }

    if (const auto upsert = arguments["upsert"]) {
        options.upsert(upsert.get_bool().value);
    }

    stdx::optional<result::update> update_many_result;

    {
        const auto update = arguments["update"];
        const auto type = update.type();

        switch (type) {
            case bsoncxx::type::k_document: {
                const auto doc = update.get_document().value;
                update_many_result = coll.update_many(filter, doc, options);
                break;
            }
            case bsoncxx::type::k_array: {
                pipeline pipe;
                pipe.append_stages(update.get_array().value);
                update_many_result = coll.update_many(filter, pipe, options);
                break;
            }
            default:
                throw std::logic_error("unexpected type '" + to_string(type) +
                                       "' for field 'update'");
        }
    }

    auto result = builder::basic::document{};
    result.append(builder::basic::kvp("result", [&](builder::basic::sub_document subdoc) {
        if (!update_many_result) {
            return;
        }

        subdoc.append(builder::basic::kvp("matchedCount", update_many_result->matched_count()));
        subdoc.append(builder::basic::kvp("modifiedCount", update_many_result->modified_count()));
        subdoc.append(builder::basic::kvp("upsertedCount", update_many_result->upserted_count()));

        if (auto upserted_element = update_many_result->upserted_id()) {
            subdoc.append(builder::basic::kvp("upsertedId", upserted_element->get_value()));
        }
    }));

    return result.extract();
}

document::value count_documents(collection& coll,
                                client_session* session,
                                document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::value empty_filter = builder::basic::make_document();
    document::view filter;
    if (arguments["filter"]) {
        filter = arguments["filter"].get_document().value;
    } else {
        filter = empty_filter.view();
    }
    options::count options{};

    if (const auto collation = arguments["collation"]) {
        options.collation(collation.get_document().value);
    }

    if (const auto limit = arguments["limit"]) {
        options.limit(as_int64(limit));
    }

    if (const auto skip = arguments["skip"]) {
        options.skip(as_int64(skip));
    }

    if (const auto hint = arguments["hint"]) {
        if (hint.type() == bsoncxx::type::k_string) {
            options.hint(mongocxx::hint(hint.get_string().value));
        } else {
            options.hint(mongocxx::hint(hint.get_document().value));
        }
    }

    if (const auto comment = arguments["comment"]) {
        options.comment(comment.get_value());
    }

    int64_t count;

    if (session) {
        count = coll.count_documents(*session, filter, options);
    } else {
        count = coll.count_documents(filter, options);
    }

    auto result = builder::basic::document{};
    result.append(builder::basic::kvp("result", count));
    return result.extract();
}

document::value estimated_document_count(collection& coll, document::view operation) {
    options::estimated_document_count options{};

    if (const auto arguments = operation["arguments"]) {
        if (const auto max_time_ms = arguments["maxTimeMS"]) {
            options.max_time(std::chrono::milliseconds(max_time_ms.get_int32()));
        }

        if (const auto comment = arguments["comment"]) {
            options.comment(comment.get_value());
        }
    }

    int64_t edc = coll.estimated_document_count(options);

    auto result = builder::basic::document{};
    result.append(builder::basic::kvp("result", edc));
    return result.extract();
}

document::value distinct(collection& coll, client_session* session, document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    auto field_name = arguments["fieldName"].get_string().value;

    document::value empty_filter = builder::basic::make_document();
    document::view filter;

    if (const auto f = arguments["filter"]) {
        filter = f.get_document().value;
    } else {
        filter = empty_filter.view();
    }

    options::distinct options{};

    if (const auto collation = arguments["collation"]) {
        options.collation(collation.get_document().value);
    }

    if (const auto comment = arguments["comment"]) {
        options.comment(comment.get_value());
    }

    stdx::optional<cursor> result_cursor;
    if (session) {
        result_cursor.emplace(coll.distinct(*session, field_name, filter, options));
    } else {
        result_cursor.emplace(coll.distinct(field_name, filter, options));
    }

    // A cursor returned by collection::distinct returns one document like:
    // { values: [<value1>, <value2>, ... ]}
    auto result = builder::basic::document{};
    result.append(builder::basic::kvp("result", [&result_cursor](builder::basic::sub_array array) {
        for (auto&& result_doc : *result_cursor) {
            for (auto&& value : result_doc["values"].get_array().value) {
                array.append(value.get_value());
            }
        }
    }));

    return result.extract();
}

document::value rewrap_many_datakey(entity::map& map,
                                    const std::string& object,
                                    document::view operation) {
    auto filter = operation["arguments"]["filter"].get_document();

    options::rewrap_many_datakey rewrap_opts;
    std::string provider;
    if (operation["arguments"]["opts"]["provider"]) {
        provider = std::string(operation["arguments"]["opts"]["provider"].get_string().value);
    }
    rewrap_opts.provider(provider);

    if (operation["arguments"]["opts"]["masterKey"]) {
        auto master_key = operation["arguments"]["opts"]["masterKey"].get_document().view();
        rewrap_opts.master_key(master_key);
    }

    client_encryption& client_encryption = map.get_client_encryption(object);
    auto result = client_encryption.rewrap_many_datakey(filter.view(), rewrap_opts);

    auto maybe_bulk_write = result.result();
    if (maybe_bulk_write) {
        auto bulk_write_result = maybe_bulk_write.value();
        auto doc = make_document(
            kvp("result",
                make_document(
                    kvp("bulkWriteResult", bulk_write_result_to_document(bulk_write_result)))));
        return doc;
    } else {
        auto doc =
            make_document(kvp("result", make_document(kvp("bulkWriteResult", types::b_null{}))));
        return doc;
    }
}

document::value delete_key(entity::map& map, const std::string& object, document::view operation) {
    auto arguments = operation["arguments"].get_document().value;
    auto key = arguments["id"].get_value();

    client_encryption& client_encryption = map.get_client_encryption(object);
    auto result = client_encryption.delete_key(key);

    auto doc =
        make_document(kvp("result", make_document(kvp("deletedCount", result.deleted_count()))));
    return doc;
}

document::value get_key(entity::map& map, const std::string& object, document::view operation) {
    auto arguments = operation["arguments"].get_document().value;
    auto key = arguments["id"].get_value();

    client_encryption& client_encryption = map.get_client_encryption(object);
    auto result = client_encryption.get_key(key);

    if (result) {
        auto doc = make_document(kvp("result", *result));
        return doc;
    } else {
        auto doc = make_document(kvp("result", types::b_null{}));
        return doc;
    }
}

document::value get_keys(entity::map& map, const std::string& object, document::view operation) {
    (void)operation;
    client_encryption& client_encryption = map.get_client_encryption(object);
    auto cursor = client_encryption.get_keys();

    return make_document(kvp("result", [&cursor](builder::basic::sub_array array) {
        for (auto&& document : cursor) {
            array.append(document);
        }
    }));
}

document::value create_data_key(entity::map& map,
                                const std::string& object,
                                document::view operation) {
    std::string provider;
    if (operation["arguments"]["kmsProvider"]) {
        provider = std::string(operation["arguments"]["kmsProvider"].get_string().value);
    }

    options::data_key data_key_opts;
    if (operation["arguments"]["opts"]["masterKey"]) {
        auto master_key = operation["arguments"]["opts"]["masterKey"].get_document().value;
        data_key_opts.master_key(master_key);
    }
    if (operation["arguments"]["opts"]["keyAltNames"]) {
        std::vector<std::string> key_alt_names;
        auto arr = operation["arguments"]["opts"]["keyAltNames"].get_array().value;
        for (const auto& it : arr) {
            key_alt_names.push_back(std::string(it.get_string().value));
        }
        data_key_opts.key_alt_names(key_alt_names);
    }
    if (operation["arguments"]["opts"]["keyMaterial"]) {
        auto bin = operation["arguments"]["opts"]["keyMaterial"].get_binary();
        std::vector<uint8_t> bytes(bin.bytes, bin.bytes + bin.size);
        data_key_opts.key_material(bytes);
    }

    client_encryption& client_encryption = map.get_client_encryption(object);
    auto result = client_encryption.create_data_key(provider, data_key_opts);

    return make_document(kvp("result", result));
}

document::value add_key_alt_name(entity::map& map,
                                 const std::string& object,
                                 document::view operation) {
    auto arguments = operation["arguments"].get_document().value;
    auto id = arguments["id"].get_value();
    std::string key_alt_name = std::string(arguments["keyAltName"].get_string().value);

    client_encryption& client_encryption = map.get_client_encryption(object);
    auto result = client_encryption.add_key_alt_name(id, key_alt_name);

    if (result) {
        auto doc = make_document(kvp("result", *result));
        return doc;
    } else {
        auto doc = make_document(kvp("result", types::b_null{}));
        return doc;
    }
}

document::value get_key_by_alt_name(entity::map& map,
                                    const std::string& object,
                                    document::view operation) {
    auto arguments = operation["arguments"].get_document().value;
    std::string key_alt_name = std::string(arguments["keyAltName"].get_string().value);

    client_encryption& client_encryption = map.get_client_encryption(object);
    auto result = client_encryption.get_key_by_alt_name(key_alt_name);

    if (result) {
        auto doc = make_document(kvp("result", *result));
        return doc;
    } else {
        auto doc = make_document(kvp("result", types::b_null{}));
        return doc;
    }
}

document::value remove_key_alt_name(entity::map& map,
                                    const std::string& object,
                                    document::view operation) {
    auto arguments = operation["arguments"].get_document().value;
    auto id = arguments["id"].get_value();
    std::string key_alt_name = std::string(arguments["keyAltName"].get_string().value);

    client_encryption& client_encryption = map.get_client_encryption(object);
    auto result = client_encryption.remove_key_alt_name(id, key_alt_name);

    if (result) {
        auto doc = make_document(kvp("result", *result));
        return doc;
    } else {
        auto doc = make_document(kvp("result", types::b_null{}));
        return doc;
    }
}

document::value create_find_cursor(entity::map& map,
                                   const std::string& object,
                                   client_session* session,
                                   document::view operation) {
    const auto save_result_as_entity = operation["saveResultAsEntity"];
    const auto key = save_result_as_entity
                         ? string::to_string(save_result_as_entity.get_string().value)
                         : std::string();
    const auto arguments = operation["arguments"].get_document().value;
    const auto empty_filter = builder::basic::make_document();
    const auto filter = [&]() -> document::view {
        const auto f = arguments["filter"];
        return f ? f.get_document().value : empty_filter;
    }();
    const auto options = make_find_options(arguments);

    auto coll = map.get_collection(object);
    auto cursor = session ? coll.find(*session, filter, options) : coll.find(filter, options);

    if (!key.empty()) {
        map.insert(key, std::move(cursor));
    }

    return make_document();
}

document::value create_search_index(collection& coll, document::view operation) {
    const auto arguments = operation["arguments"];
    const auto raw_model = arguments["model"];
    const auto name = raw_model["name"];
    const auto definition = raw_model["definition"].get_document().value;

    const auto model = name ? search_index_model(name.get_string().value, definition)
                            : search_index_model(definition);

    return make_document(kvp("result", coll.search_indexes().create_one(model)));
}

document::value create_search_indexes(collection& coll, document::view operation) {
    auto arguments = operation["arguments"];

    auto raw_models = arguments["models"].get_array().value;
    std::vector<search_index_model> models;

    for (auto&& m : raw_models) {
        search_index_model model = m["name"]
                                       ? search_index_model(m["name"].get_string().value,
                                                            m["definition"].get_document().value)
                                       : search_index_model(m["definition"].get_document().value);
        models.push_back(model);
    }

    std::vector<std::string> created_indexes = coll.search_indexes().create_many(models);

    builder::basic::array result;
    for (auto s : created_indexes) {
        result.append(s);
    }

    return make_document(kvp("result", result.view()));
}

document::value drop_search_index(collection& coll, document::view operation) {
    auto arguments = operation["arguments"];

    auto name = arguments["name"].get_string().value;

    coll.search_indexes().drop_one(name);

    // no return value from drop_one
    return make_document();
}

document::value list_search_indexes(collection& coll, document::view operation) {
    auto arguments = operation["arguments"];
    options::aggregate options;
    if (arguments["aggregationOptions"]) {
        const auto aggregation_options = arguments["aggregationOptions"].get_document().view();
        for (auto&& element : aggregation_options) {
            if (element.key() == stdx::string_view("batchSize")) {
                options.batch_size(element.get_int32().value);
            } else {
                throw std::logic_error{"unsupported aggregateOptions field: " +
                                       std::string(element.key())};
            }
        }
    }

    cursor c = arguments["name"]
                   ? coll.search_indexes().list(arguments["name"].get_string().value, options)
                   : coll.search_indexes().list(options);

    // we must loop over the resulting cursor to trigger server events for the spec tests
    auto result = bsoncxx::builder::basic::document{};
    result.append(
        bsoncxx::builder::basic::kvp("result", [&c](bsoncxx::builder::basic::sub_array array) {
            for (auto&& document : c) {
                array.append(document);
            }
        }));

    return result.extract();
}

document::value update_search_index(collection& coll, document::view operation) {
    auto arguments = operation["arguments"];

    auto name = arguments["name"].get_string().value;
    auto definition = arguments["definition"].get_document().value;

    coll.search_indexes().update_one(name, definition);

    // no return value from update_one
    return make_document();
}

document::value operations::run(entity::map& entity_map,
                                std::unordered_map<std::string, spec::apm_checker>& apm_map,
                                const array::element& op,
                                operations::state& state) {
    auto name = string::to_string(op["name"].get_string().value);
    auto object = string::to_string(op["object"].get_string().value);

    auto empty_doc = make_document();
    auto op_view = op.get_document().view();
    CAPTURE(name, object, to_json(op_view));
    if (name == "find")
        return find(entity_map.get_collection(object), get_session(op_view, entity_map), op_view);
    if (name == "bulkWrite")
        return bulk_write(
            entity_map.get_collection(object), get_session(op_view, entity_map), op_view);
    if (name == "insertMany")
        return insert_many(
            entity_map.get_collection(object), get_session(op_view, entity_map), op_view);
    if (name == "replaceOne")
        return replace_one(
            entity_map.get_collection(object), get_session(op_view, entity_map), op_view);
    if (name == "aggregate") {
        const auto& type = entity_map.type(object);
        if (type == typeid(mongocxx::database))
            return aggregate(
                entity_map.get_database(object), get_session(op_view, entity_map), op_view);
        if (type == typeid(mongocxx::collection))
            return aggregate(
                entity_map.get_collection(object), get_session(op_view, entity_map), op_view);

        CAPTURE(object, type.name());
        throw std::logic_error{"unrecognized object"};
    }
    if (name == "createChangeStream")
        return create_change_stream(entity_map, get_session(op_view, entity_map), object, op_view);
    if (name == "insertOne") {
        if (op["arguments"]["session"]) {
            auto session_name = string::to_string(op["arguments"]["session"].get_string().value);
            auto& session = entity_map.get_client_session(session_name);
            return insert_one(entity_map.get_collection(object), &session, op_view);
        }
        return insert_one(entity_map.get_collection(object), nullptr, op_view);
    }
    if (name == "createFindCursor") {
        return create_find_cursor(entity_map, object, get_session(op_view, entity_map), op_view);
    }
    if (name == "iterateUntilDocumentOrError") {
        const auto& type = entity_map.type(object);

        if (type == typeid(mongocxx::change_stream)) {
            return iterate_until_document_or_error(entity_map.get_change_stream(object),
                                                   state.stream_iters);
        }

        if (type == typeid(mongocxx::cursor)) {
            return iterate_until_document_or_error(entity_map.get_cursor(object),
                                                   state.cursor_iters);
        }

        CAPTURE(object, type.name());
        throw std::logic_error("unrecognized object");
    }
    if (name == "close") {
        const auto& type = entity_map.type(object);

        if (type == typeid(mongocxx::change_stream) || type == typeid(mongocxx::cursor)) {
            entity_map.erase(object);
            return empty_doc;
        }

        CAPTURE(object, type.name());
        FAIL("attempted to close an unrecognized object");
    }
    if (name == "failPoint") {
        auto key = string::to_string(op["arguments"]["client"].get_string().value);
        return fail_point(entity_map, apm_map[key], op_view);
    }
    if (name == "targetedFailPoint") {
        REQUIRE(object == "testRunner");

        const auto arguments = op_view["arguments"];

        const auto fail_point = arguments["failPoint"].get_document().value;
        REQUIRE(!fail_point.empty());

        auto* const session_ptr = get_session(op_view, entity_map);
        REQUIRE(session_ptr);
        auto& session = *session_ptr;
        const auto server_id = session.server_id();

        // Test runners MUST error if the session is not pinned to a mongos server at the time
        // this operation is executed.
        if (server_id == 0) {
            FAIL("session object is not pinned to a mongos server");
        }

        auto& client = session.client();

        // If the driver exposes an API to target a specific server for a command, the test runner
        // SHOULD use the client entity associated with the session to execute the
        // configureFailPoint command.
        client["admin"].run_command(fail_point, server_id);

        return make_document(kvp("uri", client.uri().to_string()),
                             kvp("failPoint", fail_point["configureFailPoint"].get_string()),
                             kvp("serverId", static_cast<std::int64_t>(server_id)));
    }
    if (name == "findOneAndDelete")
        return find_one_and_delete(
            entity_map.get_collection(object), get_session(op_view, entity_map), op_view);
    if (name == "findOneAndReplace")
        return find_one_and_replace(
            entity_map.get_collection(object), get_session(op_view, entity_map), op_view);
    if (name == "findOneAndUpdate")
        return find_one_and_update(
            entity_map.get_collection(object), get_session(op_view, entity_map), op_view);
    if (name == "listCollections") {
        return list_collections(entity_map, get_session(op_view, entity_map), object, op_view);
    }
    if (name == "listCollectionNames") {
        return list_collection_names(entity_map, get_session(op_view, entity_map), object, op_view);
    }
    if (name == "listDatabases") {
        return list_databases(entity_map, get_session(op_view, entity_map), object, op_view);
    }
    if (name == "listDatabaseNames") {
        return list_database_names(entity_map, get_session(op_view, entity_map), object, op_view);
    }
    if (name == "listIndexes") {
        return list_indexes(entity_map, get_session(op_view, entity_map), object);
    }
    if (name == "assertSessionNotDirty") {
        REQUIRE(!get_session(op_view, entity_map)->get_dirty());
        return empty_doc;
    }
    if (name == "assertSessionDirty") {
        REQUIRE(get_session(op_view, entity_map)->get_dirty());
        return empty_doc;
    }
    if (name == "endSession") {
        return end_session(entity_map, object);
    }
    if (name == "assertSameLsidOnLastTwoCommands") {
        auto key = string::to_string(op["arguments"]["client"].get_string().value);
        auto& apm = apm_map[key];

        auto cse1 = *(apm.end() - 1);
        auto cse2 = *(apm.end() - 2);

        CAPTURE(to_json(cse1), to_json(cse2));
        REQUIRE(cse1["commandStartedEvent"]["command"]["lsid"].get_value() ==
                cse2["commandStartedEvent"]["command"]["lsid"].get_value());
        return empty_doc;
    }
    if (name == "assertDifferentLsidOnLastTwoCommands") {
        auto key = string::to_string(op["arguments"]["client"].get_string().value);
        auto& apm = apm_map[key];

        auto cse1 = *(apm.end() - 1);
        auto cse2 = *(apm.end() - 2);

        CAPTURE(to_json(cse1), to_json(cse2));
        REQUIRE(cse1["commandStartedEvent"]["command"]["lsid"].get_value() !=
                cse2["commandStartedEvent"]["command"]["lsid"].get_value());
        return empty_doc;
    }
    if (name == "startTransaction") {
        auto& session = entity_map.get_client_session(object);
        return start_transaction(session, op_view);
    }
    if (name == "commitTransaction") {
        auto& session = entity_map.get_client_session(object);
        session.commit_transaction();
        return empty_doc;
    }
    if (name == "abortTransaction") {
        auto& session = entity_map.get_client_session(object);
        session.abort_transaction();
        return empty_doc;
    }
    if (name == "assertSessionTransactionState") {
        auto session_name = string::to_string(op["arguments"]["session"].get_string().value);
        auto& session = entity_map.get_client_session(session_name);
        return assert_session_transaction_state(session, op_view);
    }
    if (name == "assertSessionPinned") {
        auto& session = entity_map.get_client_session(
            string::to_string(op["arguments"]["session"].get_string().value));
        REQUIRE(session.server_id() != 0);
        return make_document();
    }
    if (name == "assertSessionUnpinned") {
        auto& session = entity_map.get_client_session(
            string::to_string(op["arguments"]["session"].get_string().value));
        REQUIRE(session.server_id() == 0);
        return make_document();
    }
    if (name == "dropCollection") {
        auto coll_name = string::to_string(op["arguments"]["collection"].get_string().value);
        auto& db = entity_map.get_database(object);
        auto* session = get_session(op_view, entity_map);
        if (session) {
            db.collection(coll_name).drop(*session);
        } else {
            db.collection(coll_name).drop();
        }
        return empty_doc;
    }
    if (name == "createCollection") {
        const auto coll_name = string::to_string(op["arguments"]["collection"].get_string().value);
        auto& db = entity_map.get_database(object);
        auto opts = builder::basic::document{};
        const auto arguments = op["arguments"];
        if (const auto timeseries = arguments["timeseries"]) {
            opts.append(builder::basic::kvp("timeseries", timeseries.get_document().view()));
        }
        if (const auto eas = arguments["expireAfterSeconds"]) {
            opts.append(builder::basic::kvp("expireAfterSeconds", eas.get_int32().value));
        }
        if (const auto clustered_index = arguments["clusteredIndex"]) {
            opts.append(
                builder::basic::kvp("clusteredIndex", clustered_index.get_document().view()));
        }
        if (const auto pre_and_post = arguments["changeStreamPreAndPostImages"]) {
            opts.append(builder::basic::kvp("changeStreamPreAndPostImages",
                                            pre_and_post.get_document().view()));
        }
        {
            const auto view_on = arguments["viewOn"];
            const auto pipeline = arguments["pipeline"];

            // Both fields must be present as a pair.
            if (view_on && pipeline) {
                opts.append(builder::basic::kvp("viewOn", view_on.get_string().value));
                opts.append(builder::basic::kvp("pipeline", pipeline.get_array().value));
            }
        }

        if (const auto session = get_session(op_view, entity_map)) {
            db.create_collection(*session, coll_name, opts.view());
        } else {
            db.create_collection(coll_name, opts.view());
        }

        return empty_doc;
    }
    if (name == "modifyCollection") {
        const auto arguments = op["arguments"];
        const auto coll_name = string::to_string(arguments["collection"].get_string().value);
        auto& db = entity_map.get_database(object);

        builder::basic::document command;

        command.append(builder::basic::kvp("collMod", coll_name));

        if (const auto pre_and_post = arguments["changeStreamPreAndPostImages"]) {
            command.append(builder::basic::kvp("changeStreamPreAndPostImages",
                                               pre_and_post.get_document().view()));
        }

        builder::basic::document result;
        if (const auto session = get_session(op_view, entity_map)) {
            result.append(builder::basic::kvp("result", db.run_command(*session, command.view())));
        } else {
            result.append(builder::basic::kvp("result", db.run_command(command.view())));
        }
        return result.extract();
    }
    if (name == "assertCollectionNotExists") {
        REQUIRE_FALSE(collection_exists(op_view));
        return empty_doc;
    }
    if (name == "assertCollectionExists") {
        REQUIRE(collection_exists(op_view));
        return empty_doc;
    }
    if (name == "createIndex") {
        auto& coll = entity_map.get_collection(object);
        if (op["arguments"]["session"]) {
            auto session_name = string::to_string(op["arguments"]["session"].get_string().value);
            auto& session = entity_map.get_client_session(session_name);
            return create_index(coll, &session, op_view);
        }
        return create_index(coll, nullptr, op_view);
    }
    if (name == "assertIndexNotExists") {
        REQUIRE_FALSE(index_exists(op_view));
        return empty_doc;
    }
    if (name == "assertIndexExists") {
        REQUIRE(index_exists(op_view));
        return empty_doc;
    }
    if (name == "withTransaction") {
        auto cb = [&](client_session*) {
            for (auto&& o : op["arguments"]["callback"].get_array().value) {
                operations::run(entity_map, apm_map, o, state);
            }
        };
        auto& session = entity_map.get_client_session(object);
        return with_transaction(session, op_view, cb);
    }
    if (name == "rename") {
        const auto& type = entity_map.type(object);
        if (type != typeid(mongocxx::collection)) {
            FAIL("rename operation requested on object with type " << type.name());
        }
        return rename(entity_map.get_collection(object), op_view);
    }
    if (name == "delete") {
        auto& bucket = entity_map.get_bucket(object);
        auto arguments = op["arguments"];
        bucket.delete_file(arguments.get_document().view()["id"].get_value());
        return empty_doc;
    }
    if (name == "download") {
        auto& bucket = entity_map.get_bucket(object);
        return download(bucket, op_view);
    }
    if (name == "upload") {
        return upload(entity_map, op_view);
    }
    if (name == "deleteOne") {
        auto& coll = entity_map.get_collection(object);
        return delete_one(coll, get_session(op_view, entity_map), op_view);
    }
    if (name == "deleteMany") {
        auto& coll = entity_map.get_collection(object);
        return delete_many(coll, get_session(op_view, entity_map), op_view);
    }
    if (name == "runCommand") {
        auto& db = entity_map.get_database(object);
        return run_command(db, get_session(op_view, entity_map), op_view);
    }
    if (name == "updateOne") {
        auto* session = get_session(op_view, entity_map);
        return update_one(entity_map.get_collection(object), session, op_view);
    }
    if (name == "updateMany") {
        return update_many(entity_map.get_collection(object), op_view);
    }
    if (name == "countDocuments") {
        // TODO: all operations which accept a session should check and apply a session.
        return count_documents(
            entity_map.get_collection(object), get_session(op_view, entity_map), op_view);
    }
    if (name == "estimatedDocumentCount") {
        return estimated_document_count(entity_map.get_collection(object), op_view);
    }
    if (name == "distinct") {
        auto& coll = entity_map.get_collection(object);
        return distinct(coll, get_session(op_view, entity_map), op_view);
    }
    if (name == "rewrapManyDataKey") {
        return rewrap_many_datakey(entity_map, object, op_view);
    }
    if (name == "deleteKey") {
        return delete_key(entity_map, object, op_view);
    }
    if (name == "getKey") {
        return get_key(entity_map, object, op_view);
    }
    if (name == "getKeys") {
        return get_keys(entity_map, object, op_view);
    }
    if (name == "createDataKey") {
        return create_data_key(entity_map, object, op_view);
    }
    if (name == "addKeyAltName") {
        return add_key_alt_name(entity_map, object, op_view);
    }
    if (name == "getKeyByAltName") {
        return get_key_by_alt_name(entity_map, object, op_view);
    }
    if (name == "removeKeyAltName") {
        return remove_key_alt_name(entity_map, object, op_view);
    }
    if (name == "createSearchIndex") {
        auto& coll = entity_map.get_collection(object);

        return create_search_index(coll, op_view);
    }
    if (name == "createSearchIndexes") {
        auto& coll = entity_map.get_collection(object);

        return create_search_indexes(coll, op_view);
    }
    if (name == "dropSearchIndex") {
        auto& coll = entity_map.get_collection(object);

        return drop_search_index(coll, op_view);
    }
    if (name == "listSearchIndexes") {
        auto& coll = entity_map.get_collection(object);

        return list_search_indexes(coll, op_view);
    }
    if (name == "updateSearchIndex") {
        auto& coll = entity_map.get_collection(object);

        return update_search_index(coll, op_view);
    }

    throw std::logic_error{"unsupported operation: " + name};
}

}  // namespace mongocxx
