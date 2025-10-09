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

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>

// NOTE: Any time this file is modified, a DOCS ticket should be opened to sync the changes with the
// corresponding page on mongodb.com/docs. See CXX-1514, CXX-1249, and DRIVERS-356 for more info.

void runcommand_examples(mongocxx::database& db) {
    {
        // Start runCommand Example 1
        using namespace bsoncxx::builder::basic;
        auto buildInfo = db.run_command(make_document(kvp("buildInfo", 1)));
        // End runCommand Example 1

        if (buildInfo.view()["ok"].get_double() != double{1}) {
            throw std::logic_error("buildInfo command failed in runCommand example 1");
        }
    }
}

int main() {
    // The mongocxx::instance constructor and destructor initialize and shut down the driver,
    // respectively. Therefore, a mongocxx::instance must be created before using the driver and
    // must remain alive for as long as the driver is in use.
    const mongocxx::instance inst{};

    const mongocxx::client conn{mongocxx::uri{}};
    auto db = conn["documentation_examples"];

    try {
        runcommand_examples(db);
    } catch (const std::logic_error& e) {
        std::cerr << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
