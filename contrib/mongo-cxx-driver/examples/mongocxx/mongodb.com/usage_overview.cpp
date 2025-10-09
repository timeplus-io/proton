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

#include <iostream>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/builder/basic/sub_array.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/database.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>

// NOTE: Any time this file is modified, a WEBSITE ticket should be opened to sync the changes with
// the "What is MongoDB" webpage, which the example was originally added to as part of WEBSITE-5148.

int main() {
    // 1. Connect to MongoDB instance running on localhost
    mongocxx::instance instance{};
    mongocxx::client client{mongocxx::uri{}};

    mongocxx::database db = client["test"];
    mongocxx::collection coll = db["restaurants"];

    // 2. Insert
    using bsoncxx::builder::basic::kvp;
    bsoncxx::builder::basic::document doc1;
    doc1.append(kvp("name", "Sun Bakery Trattoria"),
                kvp("stars", 4),
                kvp("categories", [](bsoncxx::builder::basic::sub_array arr) {
                    arr.append("Pizza", "Pasta", "Italian", "Coffee", "Sandwiches");
                }));

    bsoncxx::builder::basic::document doc2;
    doc2.append(kvp("name", "Blue Bagels Grill"),
                kvp("stars", 3),
                kvp("categories", [](bsoncxx::builder::basic::sub_array arr) {
                    arr.append("Bagels", "Cookies", "Sandwiches");
                }));

    bsoncxx::builder::basic::document doc3;
    doc3.append(kvp("name", "Hot Bakery Cafe"),
                kvp("stars", 4),
                kvp("categories", [](bsoncxx::builder::basic::sub_array arr) {
                    arr.append("Bakery", "Cafe", "Coffee", "Dessert");
                }));

    bsoncxx::builder::basic::document doc4;
    doc4.append(kvp("name", "XYZ Coffee Bar"),
                kvp("stars", 5),
                kvp("categories", [](bsoncxx::builder::basic::sub_array arr) {
                    arr.append("Bakery", "Coffee", "Cafe", "Bakery", "Chocolates");
                }));

    bsoncxx::builder::basic::document doc5;
    doc5.append(kvp("name", "456 Cookies Shop"),
                kvp("stars", 4),
                kvp("categories", [](bsoncxx::builder::basic::sub_array arr) {
                    arr.append("Bakery", "Cookies", "Cake", "Coffee");
                }));

    std::vector<bsoncxx::document::value> documents = {
        doc1.extract(), doc2.extract(), doc3.extract(), doc4.extract(), doc5.extract()};

    auto result = coll.insert_many(documents);

    // 3. Query
    for (auto&& doc : coll.find({})) {
        std::cout << bsoncxx::to_json(doc) << std::endl;
    }

    // 4. Create Index
    bsoncxx::builder::basic::document index_specification;
    index_specification.append(kvp("name", 1));

    coll.create_index(index_specification.extract());

    // 5. Perform aggregation
    mongocxx::pipeline stages;

    bsoncxx::builder::basic::document match_stage;
    bsoncxx::builder::basic::document group_stage;

    using bsoncxx::builder::basic::sub_document;

    match_stage.append(kvp("categories", "Bakery"));
    group_stage.append(kvp("_id", "$stars"),
                       kvp("count", [](sub_document sub) { sub.append(kvp("$sum", 1)); }));

    stages.match(match_stage.view()).group(group_stage.view());

    for (auto&& doc : coll.aggregate(stages)) {
        std::cout << bsoncxx::to_json(doc) << std::endl;
    }
}
