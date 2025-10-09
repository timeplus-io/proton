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

#include <cstdlib>
#include <iostream>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

int main(int, char**) {
    // The mongocxx::instance constructor and destructor initialize and shut down the driver,
    // respectively. Therefore, a mongocxx::instance must be created before using the driver and
    // must remain alive for as long as the driver is in use.
    mongocxx::instance inst{};
    mongocxx::client conn{mongocxx::uri{}};
    auto coll = conn["test"]["coll"];
    coll.drop();

    auto doc1 = make_document(kvp("a", 1));
    auto doc2 = make_document(kvp("$set", make_document(kvp("a", 2))));

    // Create a model for an insert_one operation.
    mongocxx::model::insert_one insert_op{doc1.view()};

    // Create a model for a delete_one operation.
    mongocxx::model::delete_one delete_op{doc1.view()};

    // Create a model for an update_one operation.
    mongocxx::model::update_one upsert_op{doc1.view(), doc2.view()};

    // Set upsert to true: if no document matches {"a": 1}, insert {"a": 2}.
    upsert_op.upsert(true);

    // Create a bulk_write operation. By default, its operations are executed in order.
    auto bulk = coll.create_bulk_write();

    // Insert {"a": 1}.
    bulk.append(insert_op);

    // Delete {"a": 1}.
    bulk.append(delete_op);

    // Since no document matches {"a": 1}, insert {"a": 2}.
    bulk.append(upsert_op);

    // Insert {"a": 1}. Note the same model may be appended multiple times.
    bulk.append(insert_op);

    // Update {"a": 1} to {"a": 2}.
    bulk.append(upsert_op);

    auto result = bulk.execute();

    if (!result) {
        return EXIT_FAILURE;
    }

    if (result->inserted_count() != 2) {
        return EXIT_FAILURE;
    }

    if (result->matched_count() != 1) {
        return EXIT_FAILURE;
    }

    if (result->modified_count() != 1) {
        return EXIT_FAILURE;
    }

    if (result->deleted_count() != 1) {
        return EXIT_FAILURE;
    }

    if (result->upserted_count() != 1) {
        return EXIT_FAILURE;
    }

    std::cout << "Upserted IDs" << std::endl;
    for (const auto& id : result->upserted_ids()) {
        std::cout << "Bulk write index: " << id.first << std::endl
                  << (id.second.get_oid().value.to_string()) << std::endl;
    }

    // The collection should contain two copies of {"a": 2}.
    auto cursor = coll.find({});
    int i = 0;
    for (auto&& doc : cursor) {
        i++;
        if (doc["a"].get_int32() != 2) {
            return EXIT_FAILURE;
        }
    }

    return (i == 2) ? EXIT_SUCCESS : EXIT_FAILURE;
}
