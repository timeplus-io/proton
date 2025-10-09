// Copyright 2020-present MongoDB Inc.
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
#include <ctime>
#include <iostream>

#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/client_session.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/client_session.hpp>
#include <mongocxx/options/insert.hpp>
#include <mongocxx/read_concern.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/write_concern.hpp>

using bsoncxx::builder::stream::close_document;
using bsoncxx::builder::stream::document;
using bsoncxx::builder::stream::finalize;
using bsoncxx::builder::stream::open_document;

int main() {
    using namespace mongocxx;

    instance inst{};
    client client{mongocxx::uri{"mongodb://localhost/?replicaSet=replset"}};

    // Start Causal Consistency Example 1

    write_concern wc_majority{};
    wc_majority.majority(std::chrono::milliseconds(1000));

    read_concern rc_majority{};
    rc_majority.acknowledge_level(read_concern::level::k_majority);

    // Use a causally-consistent session to run some operations.
    options::client_session session_opts;
    session_opts.causal_consistency(true);

    auto session_1 = client.start_session(std::move(session_opts));

    auto time_point = std::chrono::system_clock::now();
    bsoncxx::types::b_date current_date(time_point);

    auto items = client["test"]["items"];
    items.write_concern(wc_majority);
    items.read_concern(rc_majority);

    // Run an update_one with our causally-consistent session.
    auto none = bsoncxx::types::b_null{};
    auto update_filter = document{} << "sku"
                                    << "111"
                                    << "end" << none << finalize;
    auto update_op = document{} << "$set" << open_document << "end" << current_date
                                << close_document << finalize;
    items.update_one(session_1, std::move(update_filter), std::move(update_op), {});

    // Run an insert with our causally-consistent session.
    auto insert_doc = document{} << "sku"
                                 << "nuts-111"
                                 << "name"
                                 << "Pecans"
                                 << "start" << current_date << finalize;
    items.insert_one(session_1, std::move(insert_doc));

    // End Causal Consistency Example 1

    // Start Causal Consistency Example 2

    // Make a new session, session_2, and make it causally-consistent
    // with session_1, so that session_2 will read session_1's writes.
    options::client_session session_opts_2;
    session_opts_2.causal_consistency(true);

    auto session_2 = client.start_session(std::move(session_opts_2));

    // Set the cluster time for session_2 to session_1's cluster time,
    // and set the operation time for session2 to session2's operation time.
    auto cluster_time_1 = session_1.cluster_time();
    auto operation_time_1 = session_1.operation_time();
    session_2.advance_cluster_time(std::move(cluster_time_1));
    session_2.advance_operation_time(std::move(operation_time_1));

    read_preference rp_secondary;
    rp_secondary.mode(read_preference::read_mode::k_secondary);

    items = client["test"]["items"];
    items.read_preference(rp_secondary);
    items.write_concern(wc_majority);
    items.read_concern(rc_majority);

    // Run a find on session_2, which should now find all writes done
    // inside of session_1.
    auto find_query = document{} << "end" << none << finalize;

    auto cursor = items.find(session_2, std::move(find_query));
    for (auto&& doc : cursor) {
        std::cout << bsoncxx::to_json(doc) << std::endl;
    }

    // End Causal Consistency Example 2
}
