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

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <mongocxx/change_stream.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>
#include <mongocxx/uri.hpp>

namespace {

// watch_forever iterates the change stream until an error occurs.
void watch_forever(mongocxx::collection& collection) {
    mongocxx::options::change_stream options;
    // Wait up to 1 second before polling again.
    const std::chrono::milliseconds await_time{1000};
    options.max_await_time(await_time);

    mongocxx::change_stream stream = collection.watch(options);

    while (true) {
        for (const auto& event : stream) {
            std::cout << bsoncxx::to_json(event) << std::endl;
        }
        std::cout << "No new notifications. Trying again..." << std::endl;
    }
}

}  // namespace

int main(int argc, char* argv[]) {
    mongocxx::instance inst{};
    auto uri_str = mongocxx::uri::k_default_uri;
    std::string db = "db";
    std::string coll = "coll";

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        bsoncxx::stdx::optional<std::string> nextarg;
        if (i < argc - 1) {
            nextarg = std::string(argv[i + 1]);
        }

        if (arg == "--uri") {
            if (!nextarg) {
                std::cerr << "Expected value for '" << arg << "' option" << std::endl;
                return EXIT_FAILURE;
            }
            uri_str = *nextarg;
            i++;
            continue;
        }
        if (arg == "--db") {
            if (!nextarg) {
                std::cerr << "Expected value for '" << arg << "' option" << std::endl;
                return EXIT_FAILURE;
            }
            db = *nextarg;
            i++;
            continue;
        }
        if (arg == "--coll") {
            if (!nextarg) {
                std::cerr << "Expected value for '" << arg << "'option" << std::endl;
                return EXIT_FAILURE;
            }
            coll = *nextarg;
            i++;
            continue;
        }

        std::cerr << "Unexpected argument: '" << arg << "'" << std::endl;
        std::cerr << "Usage: " << argv[0] << " [--uri <uri>] [--db <db_name>] [--coll <coll_name>]"
                  << std::endl;
        return EXIT_FAILURE;
    }

    mongocxx::pool pool{mongocxx::uri(uri_str)};

    try {
        auto entry = pool.acquire();
        auto collection = (*entry)[db][coll];

        std::cout << "Watching for notifications on the collection " << db << "." << coll
                  << std::endl;
        std::cout << "To observe a notification, try inserting a document." << std::endl;
        watch_forever(collection);

        return EXIT_SUCCESS;
    } catch (const std::exception& exception) {
        std::cerr << "Caught exception \"" << exception.what() << "\"" << std::endl;
    } catch (...) {
        std::cerr << "Caught unknown exception type" << std::endl;
    }

    return EXIT_FAILURE;
}
