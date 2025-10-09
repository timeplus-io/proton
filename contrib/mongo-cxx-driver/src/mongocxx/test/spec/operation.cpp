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

#include <functional>
#include <vector>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <bsoncxx/test/catch.hh>
#include <bsoncxx/types.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/cursor.hpp>
#include <mongocxx/database.hpp>
#include <mongocxx/options/aggregate.hpp>
#include <mongocxx/options/count.hpp>
#include <mongocxx/options/delete.hpp>
#include <mongocxx/options/distinct.hpp>
#include <mongocxx/options/find.hpp>
#include <mongocxx/options/find_one_and_delete.hpp>
#include <mongocxx/options/find_one_and_replace.hpp>
#include <mongocxx/options/find_one_and_update.hpp>
#include <mongocxx/options/find_one_common_options.hpp>
#include <mongocxx/options/replace.hpp>
#include <mongocxx/options/update.hpp>
#include <mongocxx/result/delete.hpp>
#include <mongocxx/result/insert_many.hpp>
#include <mongocxx/result/insert_one.hpp>
#include <mongocxx/result/replace_one.hpp>
#include <mongocxx/result/update.hpp>
#include <mongocxx/test/spec/operation.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace spec {

using namespace mongocxx;
using namespace bsoncxx;

int64_t as_int64(const document::element& el) {
    if (el.type() == type::k_int32) {
        return static_cast<std::int64_t>(el.get_int32().value);
    } else if (el.type() == type::k_int64) {
        return el.get_int64().value;
    }
    REQUIRE(false);
    return 0;
}

pipeline build_pipeline(array::view pipeline_docs) {
    pipeline pipeline{};

    pipeline.append_stages(std::move(pipeline_docs));

    return pipeline;
}

bsoncxx::stdx::optional<read_concern> lookup_read_concern(document::view doc) {
    if (doc["readConcern"] && doc["readConcern"]["level"]) {
        read_concern rc;
        rc.acknowledge_string(string::to_string(doc["readConcern"]["level"].get_string().value));
        return rc;
    }

    return {};
}

bsoncxx::stdx::optional<write_concern> lookup_write_concern(document::view doc) {
    if (doc["writeConcern"] && doc["writeConcern"]["w"]) {
        write_concern wc;
        document::element w = doc["writeConcern"]["w"];
        if (w.type() == bsoncxx::type::k_string) {
            std::string level = string::to_string(w.get_string().value);
            if (level.compare("majority") == 0) {
                wc.acknowledge_level(write_concern::level::k_majority);
            } else if (level.compare("acknowledged") == 0) {
                wc.acknowledge_level(write_concern::level::k_acknowledged);
            } else if (level.compare("unacknowledged") == 0) {
                wc.acknowledge_level(write_concern::level::k_unacknowledged);
            }
        } else if (w.type() == bsoncxx::type::k_int32) {
            wc.nodes(w.get_int32());
        }
        return wc;
    }

    return {};
}

bsoncxx::stdx::optional<read_preference> lookup_read_preference(document::view doc) {
    if (doc["readPreference"] && doc["readPreference"]["mode"]) {
        read_preference rp;
        std::string mode = string::to_string(doc["readPreference"]["mode"].get_string().value);
        if (mode.compare("Primary") == 0) {
            rp.mode(read_preference::read_mode::k_primary);
        } else if (mode.compare("PrimaryPreferred") == 0) {
            rp.mode(read_preference::read_mode::k_primary_preferred);
        } else if (mode.compare("Secondary") == 0) {
            rp.mode(read_preference::read_mode::k_secondary);
        } else if (mode.compare("SecondaryPreferred") == 0) {
            rp.mode(read_preference::read_mode::k_secondary_preferred);
        } else if (mode.compare("Nearest") == 0) {
            rp.mode(read_preference::read_mode::k_nearest);
        }
        return rp;
    }

    return {};
}

client_session* operation_runner::_lookup_session(stdx::string_view key) {
    if (key.compare("session0") == 0) {
        return _session0;
    }

    if (key.compare("session1") == 0) {
        return _session1;
    }

    return nullptr;
}

client_session* operation_runner::_lookup_session(document::view doc) {
    if (doc["session"]) {
        return _lookup_session(doc["session"].get_string().value);
    }
    return nullptr;
}

document::value operation_runner::_run_aggregate(document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    pipeline pipeline = build_pipeline(arguments["pipeline"].get_array().value);
    options::aggregate options{};

    if (arguments["batchSize"]) {
        options.batch_size(arguments["batchSize"].get_int32().value);
    }

    if (arguments["collation"]) {
        options.collation(arguments["collation"].get_document().value);
    }

    _set_collection_options(operation);

    stdx::optional<cursor> result_cursor;
    client_session* session = _lookup_session(operation["arguments"].get_document().value);

    if (operation["object"] &&
        operation["object"].get_string().value == stdx::string_view{"database"}) {
        REQUIRE(_db);

        // Run on the database
        if (session) {
            result_cursor.emplace(_db->aggregate(*session, pipeline, options));
        } else {
            result_cursor.emplace(_db->aggregate(pipeline, options));
        }
    } else {
        // Run on the collection
        if (session) {
            result_cursor.emplace(_coll->aggregate(*session, pipeline, options));
        } else {
            result_cursor.emplace(_coll->aggregate(pipeline, options));
        }
    }

    auto result = builder::basic::document{};
    result.append(builder::basic::kvp("result", [&result_cursor](builder::basic::sub_array array) {
        for (auto&& document : *result_cursor) {
            array.append(document);
        }
    }));

    return result.extract();
}

document::value operation_runner::_run_distinct(document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::view filter{};

    if (arguments["filter"]) {
        filter = arguments["filter"].get_document().value;
    }

    bsoncxx::stdx::string_view field_name = arguments["fieldName"].get_string().value;

    options::distinct options{};

    if (arguments["collation"]) {
        options.collation(arguments["collation"].get_document().value);
    }

    stdx::optional<cursor> result_cursor;
    if (client_session* session = _lookup_session(operation["arguments"].get_document().value)) {
        result_cursor.emplace(_coll->distinct(*session, field_name, filter, options));
    } else {
        result_cursor.emplace(_coll->distinct(field_name, filter, options));
    }

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

document::value operation_runner::_run_find(document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::value empty_filter = builder::basic::make_document();
    document::view filter;
    if (arguments["filter"]) {
        filter = arguments["filter"].get_document().value;
    } else {
        filter = empty_filter.view();
    }
    options::find options{};

    if (arguments["batchSize"]) {
        options.batch_size(static_cast<std::int32_t>(as_int64(arguments["batchSize"])));
    }

    if (arguments["collation"]) {
        options.collation(arguments["collation"].get_document().value);
    }

    if (arguments["limit"]) {
        options.limit(as_int64(arguments["limit"]));
    }

    if (arguments["skip"]) {
        options.skip(as_int64(arguments["skip"]));
    }

    if (arguments["sort"]) {
        options.sort(arguments["sort"].get_document().value);
    }

    if (arguments["allowDiskUse"]) {
        options.allow_disk_use(arguments["allowDiskUse"].get_bool().value);
    }

    if (arguments["modifiers"]) {
        document::view modifiers = arguments["modifiers"].get_document().value;
        if (modifiers["$comment"]) {
            options.comment(modifiers["$comment"].get_string().value);
        }

        if (modifiers["$hint"]) {
            hint my_hint(modifiers["$hint"].get_document().value);
            options.hint(my_hint);
        }

        if (modifiers["$max"]) {
            options.max(modifiers["$max"].get_document().value);
        }

        if (modifiers["$maxTimeMS"]) {
            std::chrono::milliseconds my_millis(modifiers["$maxTimeMS"].get_int32().value);
            options.max_time(my_millis);
        }

        if (modifiers["$min"]) {
            options.min(modifiers["$min"].get_document().value);
        }

        if (modifiers["$returnKey"]) {
            options.return_key(modifiers["$returnKey"].get_bool().value);
        }

        if (modifiers["$showDiskLoc"]) {
            options.show_record_id(modifiers["$showDiskLoc"].get_bool().value);
        }
    }

    stdx::optional<cursor> result_cursor;
    if (client_session* session = _lookup_session(operation["arguments"].get_document().value)) {
        result_cursor.emplace(_coll->find(*session, filter, options));
    } else {
        result_cursor.emplace(_coll->find(filter, options));
    }

    auto result = builder::basic::document{};
    result.append(builder::basic::kvp("result", [&result_cursor](builder::basic::sub_array array) {
        for (auto&& document : *result_cursor) {
            array.append(document);
        }
    }));

    return result.extract();
}

document::value operation_runner::_run_delete_many(document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::view filter = arguments["filter"].get_document().value;
    options::delete_options options{};

    if (arguments["collation"]) {
        options.collation(arguments["collation"].get_document().value);
    }

    if (arguments["hint"]) {
        if (arguments["hint"].type() == bsoncxx::type::k_string)
            options.hint(hint{arguments["hint"].get_string().value});
        else
            options.hint(hint{arguments["hint"].get_document().value});
    }

    auto result = builder::basic::document{};
    std::int32_t deleted_count = 0;

    if (client_session* session = _lookup_session(operation["arguments"].get_document().value)) {
        if (auto delete_result = _coll->delete_many(*session, filter, options)) {
            deleted_count = delete_result->deleted_count();
        }
    } else {
        if (auto delete_result = _coll->delete_many(filter, options)) {
            deleted_count = delete_result->deleted_count();
        }
    }

    result.append(
        builder::basic::kvp("result", [deleted_count](builder::basic::sub_document subdoc) {
            subdoc.append(builder::basic::kvp("deletedCount", deleted_count));
        }));

    return result.extract();
}

document::value operation_runner::_run_delete_one(document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::view filter = arguments["filter"].get_document().value;
    options::delete_options options{};

    if (arguments["collation"]) {
        options.collation(arguments["collation"].get_document().value);
    }

    if (arguments["hint"]) {
        if (arguments["hint"].type() == bsoncxx::type::k_string)
            options.hint(hint{arguments["hint"].get_string().value});
        else
            options.hint(hint{arguments["hint"].get_document().value});
    }

    auto result = builder::basic::document{};
    std::int32_t deleted_count = 0;

    if (client_session* session = _lookup_session(operation["arguments"].get_document().value)) {
        if (auto delete_result = _coll->delete_one(*session, filter, options)) {
            deleted_count = delete_result->deleted_count();
        }
    } else {
        if (auto delete_result = _coll->delete_one(filter, options)) {
            deleted_count = delete_result->deleted_count();
        }
    }

    result.append(
        builder::basic::kvp("result", [deleted_count](builder::basic::sub_document subdoc) {
            subdoc.append(builder::basic::kvp("deletedCount", deleted_count));
        }));

    return result.extract();
}

document::value operation_runner::_run_find_one_and_delete(document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::view filter = arguments["filter"].get_document().value;
    options::find_one_and_delete options{};

    if (arguments["collation"]) {
        options.collation(arguments["collation"].get_document().value);
    }

    if (arguments["hint"]) {
        if (arguments["hint"].type() == bsoncxx::type::k_string)
            options.hint(hint{arguments["hint"].get_string().value});
        else
            options.hint(hint{arguments["hint"].get_document().value});
    }

    if (arguments["hint"]) {
        if (arguments["hint"].type() == bsoncxx::type::k_string)
            options.hint(hint{arguments["hint"].get_string().value});
        else
            options.hint(hint{arguments["hint"].get_document().value});
    }

    if (arguments["projection"]) {
        options.projection(arguments["projection"].get_document().value);
    }

    if (arguments["sort"]) {
        options.sort(arguments["sort"].get_document().value);
    }

    auto result = builder::basic::document{};
    stdx::optional<document::value> document;
    if (client_session* session = _lookup_session(operation["arguments"].get_document().value)) {
        document = _coll->find_one_and_delete(*session, filter, options);
    } else {
        document = _coll->find_one_and_delete(filter, options);
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

document::value operation_runner::_run_find_one(document::view operation) {
    auto arguments = operation["arguments"].get_document().value;
    auto filter = arguments["filter"].get_document().value;

    builder::basic::document result{};
    auto document = _coll->find_one(filter);

    // Server versions below 3.0 sometimes return an empty document rather than null when no
    // documents match.
    if (document && !(document->view().empty())) {
        result.append(builder::basic::kvp("result", *document));
    } else {
        result.append(builder::basic::kvp("result", types::b_null{}));
    }

    return result.extract();
}

document::value operation_runner::_run_find_one_and_replace(document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::view filter = arguments["filter"].get_document().value;
    document::view replacement = arguments["replacement"].get_document().value;
    options::find_one_and_replace options{};

    if (arguments["collation"]) {
        options.collation(arguments["collation"].get_document().value);
    }

    if (arguments["hint"]) {
        if (arguments["hint"].type() == bsoncxx::type::k_string)
            options.hint(hint{arguments["hint"].get_string().value});
        else
            options.hint(hint{arguments["hint"].get_document().value});
    }

    if (arguments["projection"]) {
        options.projection(arguments["projection"].get_document().value);
    }

    if (arguments["returnDocument"]) {
        std::string return_document =
            bsoncxx::string::to_string(arguments["returnDocument"].get_string().value);

        if (return_document == "After") {
            options.return_document(options::return_document::k_after);
        }

        if (return_document == "Before") {
            options.return_document(options::return_document::k_before);
        }
    }

    if (arguments["sort"]) {
        options.sort(arguments["sort"].get_document().value);
    }

    if (arguments["upsert"]) {
        options.upsert(arguments["upsert"].get_bool().value);
    }

    auto result = builder::basic::document{};
    stdx::optional<document::value> document;
    if (client_session* session = _lookup_session(operation["arguments"].get_document().value)) {
        document = _coll->find_one_and_replace(*session, filter, replacement, options);
    } else {
        document = _coll->find_one_and_replace(filter, replacement, options);
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

document::value operation_runner::_run_find_one_and_update(document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::view filter = arguments["filter"].get_document().value;
    options::find_one_and_update options{};

    if (arguments["collation"]) {
        options.collation(arguments["collation"].get_document().value);
    }

    if (arguments["hint"]) {
        if (arguments["hint"].type() == bsoncxx::type::k_string)
            options.hint(hint{arguments["hint"].get_string().value});
        else
            options.hint(hint{arguments["hint"].get_document().value});
    }

    if (arguments["projection"]) {
        options.projection(arguments["projection"].get_document().value);
    }

    if (arguments["arrayFilters"]) {
        options.array_filters(arguments["arrayFilters"].get_array().value);
    }

    if (arguments["returnDocument"]) {
        std::string return_document =
            bsoncxx::string::to_string(arguments["returnDocument"].get_string().value);

        if (return_document == "After") {
            options.return_document(options::return_document::k_after);
        }

        if (return_document == "Before") {
            options.return_document(options::return_document::k_before);
        }
    }

    if (arguments["sort"]) {
        options.sort(arguments["sort"].get_document().value);
    }

    if (arguments["upsert"]) {
        options.upsert(arguments["upsert"].get_bool().value);
    }

    auto result = builder::basic::document{};
    stdx::optional<document::value> document;
    client_session* session = _lookup_session(operation["arguments"].get_document().value);

    switch (arguments["update"].type()) {
        case bsoncxx::type::k_document: {
            document::view update = arguments["update"].get_document().value;
            if (session) {
                document = _coll->find_one_and_update(*session, filter, update, options);
            } else {
                document = _coll->find_one_and_update(filter, update, options);
            }
            break;
        }
        case bsoncxx::type::k_array: {
            pipeline update = build_pipeline(arguments["update"].get_array().value);
            if (session) {
                document = _coll->find_one_and_update(*session, filter, update, options);
            } else {
                document = _coll->find_one_and_update(filter, update, options);
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

document::value operation_runner::_run_insert_many(document::view operation) {
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;
    document::view arguments = operation["arguments"].get_document().value;
    array::view documents = arguments["documents"].get_array().value;
    std::vector<document::view> documents_to_insert{};
    options::insert insert_options;

    if (arguments["options"]) {
        document::view options = arguments["options"].get_document().value;
        if (options["ordered"]) {
            insert_options.ordered(options["ordered"].get_bool().value);
        }
    }

    for (auto&& element : documents) {
        documents_to_insert.push_back(element.get_document().value);
    }

    stdx::optional<result::insert_many> insert_many_result;
    if (client_session* session = _lookup_session(operation["arguments"].get_document().value)) {
        insert_many_result = _coll->insert_many(*session, documents_to_insert, insert_options);
    } else {
        insert_many_result = _coll->insert_many(documents_to_insert, insert_options);
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

    result.append(kvp("result", make_document(kvp("insertedIds", inserted_ids_builder.extract()))));
    return result.extract();
}

document::value operation_runner::_run_insert_one(document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::view document = arguments["document"].get_document().value;

    options::insert opts{};
    if (arguments["bypassDocumentValidation"]) {
        opts.bypass_document_validation(true);
    }

    stdx::optional<result::insert_one> insert_one_result;
    if (client_session* session = _lookup_session(arguments)) {
        insert_one_result = _coll->insert_one(*session, document, opts);
    } else {
        insert_one_result = _coll->insert_one(document, opts);
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

document::value operation_runner::_run_replace_one(document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::view filter = arguments["filter"].get_document().value;
    document::view replacement = arguments["replacement"].get_document().value;
    options::replace options{};

    if (arguments["collation"]) {
        options.collation(arguments["collation"].get_document().value);
    }

    if (arguments["hint"]) {
        if (arguments["hint"].type() == bsoncxx::type::k_string)
            options.hint(hint{arguments["hint"].get_string().value});
        else
            options.hint(hint{arguments["hint"].get_document().value});
    }

    if (arguments["upsert"]) {
        options.upsert(arguments["upsert"].get_bool().value);
    }

    std::int32_t matched_count = 0;
    bsoncxx::stdx::optional<std::int32_t> modified_count;
    std::int32_t upserted_count = 0;
    stdx::optional<result::replace_one> replace_result;
    if (client_session* session = _lookup_session(operation["arguments"].get_document().value)) {
        replace_result = _coll->replace_one(*session, filter, replacement, options);
    } else {
        replace_result = _coll->replace_one(filter, replacement, options);
    }
    bsoncxx::stdx::optional<types::bson_value::view> upserted_id{};

    if (replace_result) {
        matched_count = replace_result->matched_count();

        // Server versions below 2.4 do not return an `nModified` count.
        try {
            modified_count = replace_result->modified_count();
        } catch (const std::exception& e) {
        }

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

document::value operation_runner::_run_update_many(document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::view filter = arguments["filter"].get_document().value;
    options::update options{};

    if (arguments["collation"]) {
        options.collation(arguments["collation"].get_document().value);
    }

    if (arguments["hint"]) {
        if (arguments["hint"].type() == bsoncxx::type::k_string)
            options.hint(hint{arguments["hint"].get_string().value});
        else
            options.hint(hint{arguments["hint"].get_document().value});
    }

    if (arguments["upsert"]) {
        options.upsert(arguments["upsert"].get_bool().value);
    }

    if (arguments["arrayFilters"]) {
        options.array_filters(arguments["arrayFilters"].get_array().value);
    }

    std::int32_t matched_count = 0;
    bsoncxx::stdx::optional<std::int32_t> modified_count;
    std::int32_t upserted_count = 0;
    stdx::optional<result::update> update_result;
    client_session* session = _lookup_session(operation["arguments"].get_document().value);

    switch (arguments["update"].type()) {
        case bsoncxx::type::k_document: {
            document::view update = arguments["update"].get_document().value;
            if (session) {
                update_result = _coll->update_many(*session, filter, update, options);
            } else {
                update_result = _coll->update_many(filter, update, options);
            }
            break;
        }
        case bsoncxx::type::k_array: {
            pipeline update = build_pipeline(arguments["update"].get_array().value);
            if (session) {
                update_result = _coll->update_many(*session, filter, update, options);
            } else {
                update_result = _coll->update_many(filter, update, options);
            }
            break;
        }
        default:
            throw std::logic_error{"update must be a document or an array"};
    }

    bsoncxx::stdx::optional<types::bson_value::view> upserted_id{};

    if (update_result) {
        matched_count = update_result->matched_count();

        // Server versions below 2.4 do not return an `nModified` count.
        try {
            modified_count = update_result->modified_count();
        } catch (const std::exception& e) {
        }

        upserted_count = update_result->result().upserted_count();

        if (auto upserted_element = update_result->upserted_id()) {
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

document::value operation_runner::_run_update_one(document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::view filter = arguments["filter"].get_document().value;
    options::update options{};

    if (arguments["collation"]) {
        options.collation(arguments["collation"].get_document().value);
    }

    if (arguments["hint"]) {
        if (arguments["hint"].type() == bsoncxx::type::k_string)
            options.hint(hint{arguments["hint"].get_string().value});
        else
            options.hint(hint{arguments["hint"].get_document().value});
    }
    if (arguments["upsert"]) {
        options.upsert(arguments["upsert"].get_bool().value);
    }

    if (arguments["arrayFilters"]) {
        options.array_filters(arguments["arrayFilters"].get_array().value);
    }

    std::int32_t matched_count = 0;
    bsoncxx::stdx::optional<std::int32_t> modified_count;
    std::int32_t upserted_count = 0;
    stdx::optional<result::update> update_result;
    client_session* session = _lookup_session(operation["arguments"].get_document().value);

    switch (arguments["update"].type()) {
        case bsoncxx::type::k_document: {
            document::view update = arguments["update"].get_document().value;
            if (session) {
                update_result = _coll->update_one(*session, filter, update, options);
            } else {
                update_result = _coll->update_one(filter, update, options);
            }
            break;
        }
        case bsoncxx::type::k_array: {
            pipeline update = build_pipeline(arguments["update"].get_array().value);
            if (session) {
                update_result = _coll->update_one(*session, filter, update, options);
            } else {
                update_result = _coll->update_one(filter, update, options);
            }
            break;
        }
        default:
            throw std::logic_error{"update must be a document or an array"};
    }

    bsoncxx::stdx::optional<types::bson_value::view> upserted_id{};

    if (update_result) {
        matched_count = update_result->matched_count();

        // Server versions below 2.4 do not return an `nModified` count.
        try {
            modified_count = update_result->modified_count();
        } catch (const std::exception& e) {
        }

        upserted_count = update_result->result().upserted_count();

        if (auto upserted_element = update_result->upserted_id()) {
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

void operation_runner::_set_collection_options(document::view operation) {
    if (!operation["collectionOptions"]) {
        return;
    }

    document::view options = operation["collectionOptions"].get_document().value;

    if (options["writeConcern"]) {
        write_concern w;
        document::view write_concern_options = options["writeConcern"].get_document().value;

        // Empty writeConcern document means use the default.
        if (!write_concern_options.empty()) {
            REQUIRE(write_concern_options["w"]);

            int32_t w_option = write_concern_options["w"].get_int32().value;
            w.nodes(w_option);
        }
        _coll->write_concern(w);
    }

    if (options["readConcern"]) {
        document::view read_concern_options = options["readConcern"].get_document().value;
        read_concern rc;

        // Empty readConcern document means use the default.
        if (!read_concern_options.empty()) {
            REQUIRE(read_concern_options["level"]);

            rc.acknowledge_string(read_concern_options["level"].get_string().value);
        }
        _coll->read_concern(rc);
    }
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

document::value operation_runner::_run_bulk_write(document::view operation) {
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;

    options::bulk_write options;
    std::vector<model::write> writes;

    _set_collection_options(operation);

    auto arguments = operation["arguments"].get_document().value;
    if (arguments["options"]) {
        document::view options_doc = arguments["options"].get_document().value;
        if (options_doc["ordered"]) {
            options.ordered(options_doc["ordered"].get_bool().value);
        }
    }
    auto requests = arguments["requests"].get_array().value;
    for (auto&& request_element : requests) {
        auto request = request_element.get_document().value;
        auto request_arguments = request["arguments"].get_document().value;
        auto operation_name = request["name"].get_string().value;

        if (operation_name.compare("updateOne") == 0) {
            auto update_one = _build_update_model<model::update_one>(request_arguments);

            if (request_arguments["hint"]) {
                if (request_arguments["hint"].type() == bsoncxx::type::k_string)
                    update_one.hint(hint{request_arguments["hint"].get_string().value});
                else
                    update_one.hint(hint{request_arguments["hint"].get_document().value});
            }

            if (request_arguments["collation"]) {
                update_one.collation(request_arguments["collation"].get_document().value);
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

            if (request_arguments["hint"]) {
                if (request_arguments["hint"].type() == bsoncxx::type::k_string)
                    update_many.hint(hint{request_arguments["hint"].get_string().value});
                else
                    update_many.hint(hint{request_arguments["hint"].get_document().value});
            }

            if (request_arguments["collation"]) {
                update_many.collation(request_arguments["collation"].get_document().value);
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
            if (request_arguments["hint"]) {
                if (request_arguments["hint"].type() == bsoncxx::type::k_string)
                    replace_one.hint(hint{request_arguments["hint"].get_string().value});
                else
                    replace_one.hint(hint{request_arguments["hint"].get_document().value});
            }

            if (request_arguments["collation"]) {
                replace_one.collation(request_arguments["collation"].get_document().value);
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
            if (request_arguments["hint"]) {
                if (request_arguments["hint"].type() == bsoncxx::type::k_string)
                    delete_one.hint(hint{request_arguments["hint"].get_string().value});
                else
                    delete_one.hint(hint{request_arguments["hint"].get_document().value});
            }

            if (request_arguments["collation"]) {
                delete_one.collation(request_arguments["collation"].get_document().value);
            }

            writes.emplace_back(delete_one);
        } else if (operation_name.compare("deleteMany") == 0) {
            document::view filter = request_arguments["filter"].get_document().value;
            model::delete_many delete_many(filter);
            if (request_arguments["hint"]) {
                if (request_arguments["hint"].type() == bsoncxx::type::k_string)
                    delete_many.hint(hint{request_arguments["hint"].get_string().value});
                else
                    delete_many.hint(hint{request_arguments["hint"].get_document().value});
            }

            if (request_arguments["collation"]) {
                delete_many.collation(request_arguments["collation"].get_document().value);
            }

            writes.emplace_back(delete_many);
        } else {
            /* should not happen. */
            REQUIRE(false);
        }
    }

    std::int32_t deleted_count = 0;
    std::int32_t matched_count = 0;
    std::int32_t modified_count = 0;
    std::int32_t upserted_count = 0;
    result::bulk_write::id_map upserted_ids;
    std::int32_t inserted_count = 0;
    stdx::optional<result::bulk_write> bulk_write_result;
    if (client_session* session = _lookup_session(operation["arguments"].get_document().value)) {
        bulk_write_result = _coll->bulk_write(*session, writes, options);
    } else {
        bulk_write_result = _coll->bulk_write(writes, options);
    }
    if (bulk_write_result) {
        matched_count = bulk_write_result->matched_count();
        modified_count = bulk_write_result->modified_count();
        upserted_count = bulk_write_result->upserted_count();
        upserted_ids = bulk_write_result->upserted_ids();
        inserted_count = bulk_write_result->inserted_count();
        deleted_count = bulk_write_result->deleted_count();
    }
    builder::basic::document upserted_ids_builder;
    for (auto&& index_and_id : upserted_ids) {
        upserted_ids_builder.append(
            kvp(std::to_string(index_and_id.first), index_and_id.second.get_int32().value));
    }
    auto upserted_ids_doc = upserted_ids_builder.extract();
    auto result = bsoncxx::builder::basic::document{};
    // Construct the result document.
    // Note: insertedIds is currently hard coded as an empty document, because result::bulk_write
    // provides no way to see inserted ids. This is compliant with the CRUD spec, as insertedIds
    // are: "NOT REQUIRED: Drivers may choose to not provide this property." So just add an empty
    // document for insertedIds. There are no current bulk write tests testing insert operations.
    // The insertedIds field in current bulk write spec tests is always an empty document.
    result.append(kvp("result",
                      make_document(kvp("matchedCount", matched_count),
                                    kvp("modifiedCount", modified_count),
                                    kvp("upsertedCount", upserted_count),
                                    kvp("deletedCount", deleted_count),
                                    kvp("insertedCount", inserted_count),
                                    kvp("insertedIds", make_document()),
                                    kvp("upsertedIds", upserted_ids_doc))));
    return result.extract();
}

document::value operation_runner::_run_count_documents(document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::view filter = arguments["filter"].get_document().value;
    options::count options{};

    if (arguments["collation"]) {
        options.collation(arguments["collation"].get_document().value);
    }

    if (arguments["limit"]) {
        options.limit(arguments["limit"].get_int32().value);
    }

    if (arguments["skip"]) {
        options.skip(arguments["skip"].get_int32().value);
    }

    int64_t count;
    if (client_session* session = _lookup_session(operation["arguments"].get_document().value)) {
        count = _coll->count_documents(*session, filter, options);
    } else {
        count = _coll->count_documents(filter, options);
    }

    auto result = builder::basic::document{};

    // The JSON parser reads the counts as int32, so the document will not be equal to the expected
    // result without casting.
    result.append(builder::basic::kvp("result", static_cast<std::int32_t>(count)));

    return result.extract();
}

document::value operation_runner::_run_estimated_document_count(document::view) {
    int64_t count = _coll->estimated_document_count();

    auto result = builder::basic::document{};

    // The JSON parser reads the counts as int32, so the document will not be equal to the expected
    // result without casting.
    result.append(builder::basic::kvp("result", static_cast<std::int32_t>(count)));

    return result.extract();
}

document::value operation_runner::_run_start_transaction(bsoncxx::document::view operation) {
    options::transaction txn_opts;

    if (operation["arguments"] && operation["arguments"]["options"]) {
        document::view opts = operation["arguments"]["options"].get_document();

        if (auto rc = lookup_read_concern(opts)) {
            txn_opts.read_concern(*rc);
        }

        if (auto wc = lookup_write_concern(opts)) {
            txn_opts.write_concern(*wc);
        }

        if (auto rp = lookup_read_preference(opts)) {
            txn_opts.read_preference(*rp);
        }
    }

    auto session = _lookup_session(operation["object"].get_string().value);
    REQUIRE(session);
    session->start_transaction(txn_opts);

    return bsoncxx::builder::basic::make_document();
}

document::value operation_runner::_run_commit_transaction(bsoncxx::document::view operation) {
    auto session = _lookup_session(operation["object"].get_string().value);
    REQUIRE(session);
    session->commit_transaction();
    return bsoncxx::builder::basic::make_document();
}

document::value operation_runner::_run_abort_transaction(bsoncxx::document::view operation) {
    auto session = _lookup_session(operation["object"].get_string().value);
    REQUIRE(session);
    session->abort_transaction();
    return bsoncxx::builder::basic::make_document();
}

document::value operation_runner::_run_run_command(bsoncxx::document::view operation) {
    document::view arguments = operation["arguments"].get_document().value;
    document::view command = arguments["command"].get_document().value;

    stdx::optional<document::value> reply;
    if (client_session* session = _lookup_session(operation["arguments"].get_document().value)) {
        reply = _db->run_command(*session, command);
    } else {
        reply = _db->run_command(command);
    }

    auto result = builder::basic::document{};
    result.append(builder::basic::kvp("result", *reply));

    return result.extract();
}

document::value operation_runner::_create_index(const document::view& operation) {
    auto arguments = operation["arguments"];
    auto session = _lookup_session(arguments.get_document().value);
    REQUIRE(session);

    auto name = arguments["name"].get_string().value;
    auto keys = arguments["keys"].get_document().value;

    bsoncxx::builder::basic::document opts;
    opts.append(bsoncxx::builder::basic::kvp("name", name));

    return _coll->create_index(*session, keys, opts.extract());
}

document::value operation_runner::_run_create_collection(document::view operation) {
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;

    bsoncxx::document::value empty_document({});

    auto arguments = operation["arguments"].get_document().value;
    auto collection_name = arguments["collection"].get_string().value;
    auto session = _lookup_session(arguments);
    bsoncxx::builder::basic::document opts;

    if (arguments["encryptedFields"]) {
        opts.append(kvp("encryptedFields", arguments["encryptedFields"].get_document().value));
    }

    if (session) {
        _db->create_collection(*session, collection_name, opts.extract());
    } else {
        _db->create_collection(collection_name, opts.extract());
    }
    return empty_document;
}

document::value operation_runner::_run_drop_collection(document::view operation) {
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;

    bsoncxx::document::value empty_document({});

    auto arguments = operation["arguments"].get_document().value;

    auto collection_name = operation["arguments"]["collection"].get_string().value;

    if (arguments.find("encryptedFields") != arguments.end()) {
        auto encrypted_fields = arguments["encryptedFields"].get_document().value;
        auto encrypted_fields_map = make_document(kvp("encryptedFields", encrypted_fields));
        _db->collection(collection_name).drop(stdx::nullopt, std::move(encrypted_fields_map));
    } else {
        _db->collection(collection_name).drop();
    }
    return empty_document;
}

operation_runner::operation_runner(collection* coll) : operation_runner(nullptr, coll) {}
operation_runner::operation_runner(database* db,
                                   collection* coll,
                                   client_session* session0,
                                   client_session* session1,
                                   client* client)
    : _coll(coll), _db(db), _session0(session0), _session1(session1), _client(client) {}

document::value operation_runner::run(document::view operation) {
    using namespace bsoncxx::builder::basic;
    bsoncxx::document::value empty_document({});

    stdx::string_view key = operation["name"].get_string().value;
    stdx::string_view object;
    if (operation["object"]) {
        object = operation["object"].get_string().value;
    }

    if (key.compare("aggregate") == 0) {
        return _run_aggregate(operation);
    } else if (key.compare("count") == 0) {
        throw std::logic_error{"count command not supported"};
    } else if (key.compare("countDocuments") == 0) {
        return _run_count_documents(operation);
    } else if (key.compare("estimatedDocumentCount") == 0) {
        return _run_estimated_document_count(operation);
    } else if (key.compare("distinct") == 0) {
        return _run_distinct(operation);
    } else if (key.compare("find") == 0) {
        return _run_find(operation);
    } else if (key.compare("deleteMany") == 0) {
        return _run_delete_many(operation);
    } else if (key.compare("deleteOne") == 0) {
        return _run_delete_one(operation);
    } else if (key.compare("findOne") == 0) {
        return _run_find_one(operation);
    } else if (key.compare("findOneAndDelete") == 0) {
        return _run_find_one_and_delete(operation);
    } else if (key.compare("findOneAndReplace") == 0) {
        return _run_find_one_and_replace(operation);
    } else if (key.compare("findOneAndUpdate") == 0) {
        return _run_find_one_and_update(operation);
    } else if (key.compare("insertMany") == 0) {
        return _run_insert_many(operation);
    } else if (key.compare("insertOne") == 0) {
        return _run_insert_one(operation);
    } else if (key.compare("replaceOne") == 0) {
        return _run_replace_one(operation);
    } else if (key.compare("updateMany") == 0) {
        return _run_update_many(operation);
    } else if (key.compare("updateOne") == 0) {
        return _run_update_one(operation);
    } else if (key.compare("bulkWrite") == 0) {
        return _run_bulk_write(operation);
    } else if (key.compare("startTransaction") == 0) {
        return _run_start_transaction(operation);
    } else if (key.compare("commitTransaction") == 0) {
        return _run_commit_transaction(operation);
    } else if (key.compare("abortTransaction") == 0) {
        return _run_abort_transaction(operation);
    } else if (key.compare("runCommand") == 0) {
        return _run_run_command(operation);
    } else if (key.compare("assertSessionPinned") == 0) {
        const client_session* session =
            _lookup_session(operation["arguments"].get_document().value);
        REQUIRE(session);
        REQUIRE(session->server_id() != 0);
        return empty_document;
    } else if (key.compare("assertSessionUnpinned") == 0) {
        const client_session* session =
            _lookup_session(operation["arguments"].get_document().value);
        REQUIRE(session);
        REQUIRE(session->server_id() == 0);
        return empty_document;
    } else if (key.compare("assertSessionTransactionState") == 0) {
        const auto arguments = operation["arguments"].get_document().value;
        const client_session* session = _lookup_session(arguments);
        REQUIRE(session);
        const auto state = arguments["state"].get_string().value;
        switch (session->get_transaction_state()) {
            case client_session::transaction_state::k_transaction_none:
                REQUIRE(state == stdx::string_view("none"));
                break;
            case client_session::transaction_state::k_transaction_starting:
                REQUIRE(state == stdx::string_view("starting"));
                break;
            case client_session::transaction_state::k_transaction_in_progress:
                REQUIRE(state == stdx::string_view("in_progress"));
                break;
            case client_session::transaction_state::k_transaction_committed:
                REQUIRE(state == stdx::string_view("committed"));
                break;
            case client_session::transaction_state::k_transaction_aborted:
                REQUIRE(state == stdx::string_view("aborted"));
                break;
        }
        return empty_document;
    } else if (key.compare("watch") == 0) {
        if (object.compare("collection") == 0) {
            _coll->watch();
        } else if (object.compare("database") == 0) {
            _db->watch();
        } else if (object.compare("client") == 0) {
            _client->watch();
        } else {
            throw std::logic_error{"unsupported operation object: " + string::to_string(object)};
        }
        return empty_document;
    } else if (key.compare("rename") == 0) {
        _coll->rename(operation["arguments"]["to"].get_string().value);
        return empty_document;
    } else if (key.compare("drop") == 0) {
        _coll->drop();
        return empty_document;
    } else if (key.compare("dropCollection") == 0) {
        return _run_drop_collection(operation);
    } else if (key.compare("listCollectionNames") == 0) {
        _db->list_collection_names();
        return empty_document;
    } else if (key.compare("listCollectionObjects") == 0) {
        throw std::logic_error("listCollectionObjects is not implemented in mongocxx");
    } else if (key.compare("listCollections") == 0) {
        _db->list_collections();
        return empty_document;
    } else if (key.compare("listDatabases") == 0) {
        _client->list_databases().begin(); /* calling begin() iterates the cursor */
        return empty_document;
    } else if (key.compare("listDatabaseNames") == 0) {
        _client->list_database_names();
        return empty_document;
    } else if (key.compare("listIndexes") == 0) {
        _coll->list_indexes().begin(); /* calling begin() iterates the cursor */
        return empty_document;
    } else if (key.compare("download") == 0) {
        std::ostream null_stream(nullptr); /* no need to store output */
        auto bucket = _db->gridfs_bucket();
        bucket.download_to_stream(operation["arguments"]["id"].get_value(), &null_stream);

        return empty_document;
    } else if (key.compare("createCollection") == 0) {
        return _run_create_collection(operation);
    } else if (key.compare("assertCollectionNotExists") == 0) {
        auto collection_name = operation["arguments"]["collection"].get_string().value;
        client client{uri{}};
        REQUIRE_FALSE(client[_db->name()].has_collection(collection_name));
        return empty_document;
    } else if (key.compare("assertCollectionExists") == 0) {
        auto collection_name = operation["arguments"]["collection"].get_string().value;
        client client{uri{}};
        REQUIRE(client[_db->name()].has_collection(collection_name));
        return empty_document;
    } else if (key.compare("createIndex") == 0) {
        return _create_index(operation);
    } else if (key.compare("assertIndexNotExists") == 0) {
        client client{uri{}};
        auto cursor = client[_db->name()][_coll->name()].list_indexes();

        REQUIRE(
            cursor.end() ==
            std::find_if(cursor.begin(), cursor.end(), [operation](bsoncxx::document::view doc) {
                return (doc["name"].get_string() == operation["arguments"]["index"].get_string());
            }));

        return empty_document;
    } else if (key.compare("assertIndexExists") == 0) {
        client client{uri{}};
        auto db = operation["arguments"]["database"].get_string().value;
        auto collection = operation["arguments"]["collection"].get_string().value;
        auto cursor = client[db][collection].list_indexes();

        REQUIRE(
            cursor.end() !=
            std::find_if(cursor.begin(), cursor.end(), [operation](bsoncxx::document::view doc) {
                return (doc["name"].get_string() == operation["arguments"]["index"].get_string());
            }));

        return empty_document;
    } else if (key.compare("targetedFailPoint") == 0) {
        REQUIRE(object == stdx::string_view("testRunner"));

        const auto arguments = operation["arguments"].get_document().value;

        const auto session_ptr = _lookup_session(arguments);
        REQUIRE(session_ptr);
        auto& session = *session_ptr;
        const auto server_id = session.server_id();

        if (server_id == 0) {
            FAIL("session object is not pinned to a mongos server");
        }

        const auto command = arguments["failPoint"].get_document().value;
        REQUIRE(!command.empty());

        session.client()["admin"].run_command(command, server_id);

        return empty_document;
    } else {
        throw std::logic_error{"unsupported operation: " + string::to_string(key)};
    }
}

}  // namespace spec
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
