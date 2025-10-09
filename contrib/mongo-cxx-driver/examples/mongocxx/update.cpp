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

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
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

    // Update top-level fields in a single document.
    {
        db["restaurants"].update_one(
            make_document(kvp("name", "Juni")),
            make_document(kvp("$set", make_document(kvp("cuisine", "American (New)"))),
                          kvp("$currentDate", make_document(kvp("lastModified", true)))));
    }

    // Update an embedded document in a single document.
    {
        db["restaurants"].update_one(
            make_document(kvp("restaurant_id", "41156888")),
            make_document(kvp("$set", make_document(kvp("address.street", "East 31st Street")))));
    }

    // Update multiple documents.
    {
        db["restaurants"].update_many(
            make_document(kvp("address.zipcode", "10016"), kvp("cuisine", "Other")),
            make_document(kvp("$set", make_document(kvp("cuisine", "Category to be determined"))),
                          kvp("$currentDate", make_document(kvp("lastModified", true)))));
    }

    // Replace the contents of a single document.
    {
        db["restaurants"].replace_one(
            make_document(kvp("restaurant_id", "41704620")),
            make_document(kvp("name", "Vella 2"),
                          kvp("address",
                              make_document(kvp("coord", make_array(-73.9557413, 40.7720266)),
                                            kvp("building", "1480"),
                                            kvp("street", "2 Avenue"),
                                            kvp("zipcode", "10075")))));
    }
}
