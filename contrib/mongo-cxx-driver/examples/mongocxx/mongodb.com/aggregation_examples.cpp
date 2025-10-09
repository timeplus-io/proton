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

#include <iostream>
#include <list>
#include <numeric>
#include <sstream>
#include <vector>

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/instance.hpp>

// NOTE: Any time this file is modified, a DOCS ticket should be opened to sync the changes with the
// corresponding page on mongodb.com/docs. See CXX-1514, CXX-1249, and DRIVERS-356 for more info.

using namespace mongocxx;

void aggregation_examples(const mongocxx::database& db) {
    {
        // Start Aggregation Example 1
        using namespace bsoncxx::builder::basic;

        mongocxx::pipeline p{};
        p.match(make_document(kvp("items.fruit", "banana")));
        p.sort(make_document(kvp("date", 1)));

        auto cursor = db["sales"].aggregate(p, mongocxx::options::aggregate{});
        // End Aggregation Example 1

        auto count = std::distance(cursor.begin(), cursor.end());
        if (count != 0L) {
            throw std::logic_error("wrong count in example 1");
        }
    }

    {
        // Start Aggregation Example 2
        using namespace bsoncxx::builder::basic;

        mongocxx::pipeline p{};
        p.unwind("$items");
        p.match(make_document(kvp("items.fruit", "banana")));
        p.group(make_document(
            kvp("_id", make_document(kvp("day", make_document(kvp("$dayOfWeek", "$date"))))),
            kvp("count", make_document(kvp("$sum", "$items.quantity")))));
        p.project(make_document(
            kvp("dayOfWeek", "$_id.day"), kvp("numberSold", "$count"), kvp("_id", 0)));
        p.sort(make_document(kvp("numberSold", 1)));

        auto cursor = db["sales"].aggregate(p, mongocxx::options::aggregate{});
        // End Aggregation Example 2

        auto count = std::distance(cursor.begin(), cursor.end());
        if (count != 0L) {
            throw std::logic_error("wrong count in example 2");
        }
    }

    {
        // Start Aggregation Example 3
        using namespace bsoncxx::builder::basic;

        mongocxx::pipeline p{};
        p.unwind("$items");
        p.group(make_document(
            kvp("_id", make_document(kvp("day", make_document(kvp("$dayOfWeek", "$date"))))),
            kvp("items_sold", make_document(kvp("$sum", "$items.quantity"))),
            kvp("revenue",
                make_document(
                    kvp("$sum",
                        make_document(
                            kvp("$multiply", make_array("$items.quantity", "$items.price"))))))));
        p.project(make_document(
            kvp("day", "$_id.day"),
            kvp("revenue", 1),
            kvp("items_sold", 1),
            kvp("discount",
                make_document(
                    kvp("$cond",
                        make_document(
                            kvp("if", make_document(kvp("$lte", make_array("$revenue", 250)))),
                            kvp("then", 25),
                            kvp("else", 0)))))));
        auto cursor = db["sales"].aggregate(p, mongocxx::options::aggregate{});
        // End Aggregation Example 3

        auto count = std::distance(cursor.begin(), cursor.end());
        if (count != 0L) {
            throw std::logic_error("wrong count in example 3");
        }
    }

    {
        // Start Aggregation Example 4
        using namespace bsoncxx::builder::basic;

        mongocxx::pipeline p{};
        p.lookup(make_document(
            kvp("from", "air_airlines"),
            kvp("let", make_document(kvp("constituents", "$airlines"))),
            kvp("pipeline",
                make_array(make_document(
                    kvp("$match",
                        make_document(kvp(
                            "$expr",
                            make_document(kvp("$in", make_array("$name", "$$constituents"))))))))),
            kvp("as", "airlines")));
        p.project(make_document(
            kvp("_id", 0),
            kvp("name", 1),
            kvp("airlines",
                make_document(kvp(
                    "$filter",
                    make_document(kvp("input", "$airlines"),
                                  kvp("as", "airline"),
                                  kvp("cond",
                                      make_document(kvp(
                                          "$eq", make_array("$$airline.country", "Canada"))))))))));

        auto cursor = db["air_alliances"].aggregate(p, mongocxx::options::aggregate{});
        // End Aggregation Example 4

        auto count = std::distance(cursor.begin(), cursor.end());
        if (count != 0L) {
            throw std::logic_error("wrong count in example 4");
        }
    }
}

int main() {
    // The mongocxx::instance constructor and destructor initialize and shut down the driver,
    // respectively. Therefore, a mongocxx::instance must be created before using the driver and
    // must remain alive for as long as the driver is in use.
    const mongocxx::instance inst{};

    const mongocxx::client conn{mongocxx::uri{}};
    auto const db = conn["documentation_examples"];

    // SERVER-79306: Ensure the database exists for consistent behavior with sharded clusters.
    conn["documentation_examples"].create_collection("dummy");

    try {
        aggregation_examples(db);
    } catch (const std::logic_error& e) {
        std::cerr << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
