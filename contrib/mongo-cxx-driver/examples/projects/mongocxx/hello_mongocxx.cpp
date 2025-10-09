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

#include <cstdlib>
#include <iostream>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>

int main(int argc, char* argv[]) {
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;

    // The mongocxx::instance constructor and destructor initialize and shut down the driver,
    // respectively. Therefore, a mongocxx::instance must be created before using the driver and
    // must remain alive for as long as the driver is in use.
    mongocxx::instance inst;

    try {
        const auto uri = mongocxx::uri{(argc >= 2) ? argv[1] : mongocxx::uri::k_default_uri};

        auto client = mongocxx::client{uri};
        auto admin = client["admin"];
        auto result = admin.run_command(make_document(kvp("ping", 1)));
        std::cout << bsoncxx::to_json(result) << std::endl;

        return EXIT_SUCCESS;
    } catch (const std::exception& xcp) {
        std::cout << "connection failed: " << xcp.what() << std::endl;
        return EXIT_FAILURE;
    }
}
