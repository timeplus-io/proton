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
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>

using bsoncxx::to_json;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;
using namespace mongocxx;

int main() {
    // The mongocxx::instance constructor and destructor initialize and shut down the driver,
    // respectively. Therefore, a mongocxx::instance must be created before using the driver and
    // must remain alive for as long as the driver is in use.
    mongocxx::instance inst{};

    mongocxx::client conn{mongocxx::uri{"mongodb://localhost/?replicaSet=replset"}};

    // By default, a session is causally consistent. Pass options::client_session to override
    // causal consistency.
    auto session = conn.start_session();
    auto coll = conn["db"]["collection"];
    auto result = coll.update_one(session,
                                  make_document(kvp("_id", 1)),
                                  make_document(kvp("$inc", make_document(kvp("x", 1)))));

    std::cout << "Updated " << result->modified_count() << " documents" << std::endl;

    // Read from secondary. In a causally consistent session the data is guaranteed to reflect the
    // update we did on the primary. The query may block waiting for the secondary to catch up,
    // or time out and fail after 2 seconds.
    options::find opts;
    read_preference secondary;
    secondary.mode(read_preference::read_mode::k_secondary);
    opts.read_preference(secondary).max_time(std::chrono::milliseconds(2000));
    auto cursor = coll.find(session, make_document(kvp("_id", 1)), opts);
    for (auto&& doc : cursor) {
        std::cout << bsoncxx::to_json(doc) << std::endl;
    }
}
