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

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/types.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_array;
using bsoncxx::builder::basic::make_document;

int main(int, char**) {
    // The mongocxx::instance constructor and destructor initialize and shut down the driver,
    // respectively. Therefore, a mongocxx::instance must be created before using the driver and
    // must remain alive for as long as the driver is in use.
    mongocxx::instance inst{};
    mongocxx::client conn{mongocxx::uri{}};

    auto db = conn["test"];

    // TODO: fix dates

    bsoncxx::document::value restaurant_doc = make_document(
        kvp("address",
            make_document(kvp("street", "2 Avenue"),
                          kvp("zipcode", 10075),
                          kvp("building", "1480"),
                          kvp("coord", make_array(-73.9557413, 40.7720266)))),
        kvp("borough", "Manhattan"),
        kvp("cuisine", "Italian"),
        kvp("grades",
            make_array(
                make_document(kvp("date", bsoncxx::types::b_date{std::chrono::milliseconds{12323}}),
                              kvp("grade", "A"),
                              kvp("score", 11)),
                make_document(
                    kvp("date", bsoncxx::types::b_date{std::chrono::milliseconds{121212}}),
                    kvp("grade", "B"),
                    kvp("score", 17)))),
        kvp("name", "Vella"),
        kvp("restaurant_id", "41704620"));

    // We choose to move in our document here, which transfers ownership to insert_one()
    auto res = db["restaurants"].insert_one(std::move(restaurant_doc));
}
