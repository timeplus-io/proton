// Copyright 2016 MongoDB Inc.
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
#include <bsoncxx/stdx/string_view.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;
using bsoncxx::stdx::string_view;
using mongocxx::collection;
using mongocxx::validation_criteria;

int main(int, char**) {
    // The mongocxx::instance constructor and destructor initialize and shut down the driver,
    // respectively. Therefore, a mongocxx::instance must be created before using the driver and
    // must remain alive for as long as the driver is in use.
    mongocxx::instance inst{};
    mongocxx::client conn{mongocxx::uri{}};

    auto db = conn["test"];

    // Create a collection with document validation enabled.
    {
        // Clean up any existing "zombies" collection.
        if (db.has_collection("zombies")) {
            db["zombies"].drop();
        }

        // Create a validation rule: all zombies need to eat some brains.
        collection zombies = db.create_collection(
            "zombies",
            make_document(
                kvp("validator", make_document(kvp("brains", make_document(kvp("$gt", 0)))))));

        try {
            // Insert a document passing validation
            auto res =
                zombies.insert_one(make_document(kvp("name", "Bloody Betty"), kvp("brains", 3)));

            std::cout << "Bloody Betty passed document validation!" << std::endl;

            // Insert a document failing validation, which should throw.
            auto res2 =
                zombies.insert_one(make_document(kvp("name", "Undead Fred"), kvp("brains", 0)));

            std::cout << "ERROR: server does not support document validation." << std::endl;

        } catch (const mongocxx::exception& e) {
            std::cout << "Some zombie needs to eat more brains:" << std::endl;
            std::cout << e.what() << std::endl;
        }
    }
}
