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

#include <chrono>
#include <iostream>
#include <thread>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/database.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/find.hpp>
#include <mongocxx/uri.hpp>

using bsoncxx::builder::basic::document;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

//
// Document number counter for sample inserted documents.  This just
// makes the tailed document more obviously in sequence.
//

static std::int32_t counter = 0;

//
// Drop and recreate capped collection. Inserts a doc as tailing an empty
// collection returns a closed cursor.
//
// TODO CDRIVER-2093: After CDRIVER-2093 is fixed, we can provide better
// diagnostics as to whether the cursor is alive or closed and won't need
// to prime the collection with a document.
//
void init_capped_collection(mongocxx::client* conn, std::string name) {
    auto db = (*conn)["test"];
    auto coll = db[name];

    coll.drop();
    db.create_collection(name, make_document(kvp("capped", true), kvp("size", 1024 * 1024)));

    document builder{};
    builder.append(kvp("n", counter++));
    db[name].insert_one(builder.extract());
}

//
// Insert 5 documents so there are more documents to tail.
//
void insert_docs(mongocxx::collection* coll) {
    std::cout << "Inserting batch... " << std::endl;
    for (int j = 0; j < 5; j++) {
        document builder{};
        builder.append(kvp("n", counter++));
        coll->insert_one(builder.extract());
    }
}

int main(int, char**) {
    // The mongocxx::instance constructor and destructor initialize and shut down the driver,
    // respectively. Therefore, a mongocxx::instance must be created before using the driver and
    // must remain alive for as long as the driver is in use.
    mongocxx::instance inst{};
    mongocxx::client conn{mongocxx::uri{}};
    std::string name{"capped_coll"};

    // Create the capped collection.
    init_capped_collection(&conn, name);

    // Construct a tailable cursor.
    auto coll = conn["test"][name];
    mongocxx::options::find opts{};
    opts.cursor_type(mongocxx::cursor::type::k_tailable);
    auto cursor = coll.find({}, opts);

    // Loop "forever", or in this case, until we find >= 25 documents.
    std::cout << "Tailing the collection..." << std::endl;
    int docs_found = 0;
    for (;;) {
        // Loop over the cursor until no more documents are available.
        // On the next iteration of the outer loop, if more documents
        // are available, the tailable cursor will return them.
        for (auto&& doc : cursor) {
            std::cout << bsoncxx::to_json(doc) << std::endl;
            docs_found++;
        }

        if (docs_found >= 25) {
            break;
        }

        // No documents are available, so sleep a bit before trying again.
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // For the sake of this example, add more documents before the next loop.
        insert_docs(&coll);
    }

    return EXIT_SUCCESS;
}
