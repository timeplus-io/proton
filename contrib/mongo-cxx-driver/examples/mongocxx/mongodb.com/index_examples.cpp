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

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>

// NOTE: Any time this file is modified, a DOCS ticket should be opened to sync the changes with the
// corresponding page on mongodb.com/docs. See CXX-1514, CXX-1249, and DRIVERS-356 for more info.

void index_examples(const mongocxx::database& db) {
    {
        // Start Index Example 1
        using namespace bsoncxx::builder::basic;
        auto result = db["records"].create_index(make_document(kvp("score", 1)));
        // End Index Example 1
    }

    {
        // Start Index Example 2
        using namespace bsoncxx::builder::basic;
        auto result = db["restaurants"].create_index(
            make_document(kvp("cuisine", 1), kvp("name", 1)),
            make_document(kvp("partialFilterExpression",
                              make_document(kvp("rating", make_document(kvp("$gt", 5)))))));
        // End Index Example 2
    }
}

int main() {
    // The mongocxx::instance constructor and destructor initialize and shut down the driver,
    // respectively. Therefore, a mongocxx::instance must be created before using the driver and
    // must remain alive for as long as the driver is in use.
    const mongocxx::instance inst{};

    const mongocxx::client conn{mongocxx::uri{}};
    const auto db = conn["documentation_examples"];
    index_examples(db);

    return EXIT_SUCCESS;
}
