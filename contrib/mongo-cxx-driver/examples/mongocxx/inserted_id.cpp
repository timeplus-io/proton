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
#include <bsoncxx/types.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/instance.hpp>

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

int main(int, char**) {
    // The mongocxx::instance constructor and destructor initialize and shut down the driver,
    // respectively. Therefore, a mongocxx::instance must be created before using the driver and
    // must remain alive for as long as the driver is in use.
    mongocxx::instance inst{};
    mongocxx::client conn{mongocxx::uri{}};

    auto collection = conn["test"]["col"];

    try {
        auto result = collection.insert_one(make_document(kvp("test", 1)));

        if (!result) {
            std::cout << "Unacknowledged write. No id available." << std::endl;
            return EXIT_SUCCESS;
        }

        if (result->inserted_id().type() == bsoncxx::type::k_oid) {
            bsoncxx::oid id = result->inserted_id().get_oid().value;
            std::string id_str = id.to_string();
            std::cout << "Inserted id: " << id_str << std::endl;
        } else {
            std::cout << "Inserted id was not an OID type" << std::endl;
        }
    } catch (const mongocxx::exception& e) {
        std::cout << "An exception occurred: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
