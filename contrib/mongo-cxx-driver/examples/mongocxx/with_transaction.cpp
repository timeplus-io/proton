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

#include <iostream>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/client_session.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/insert.hpp>
#include <mongocxx/options/transaction.hpp>
#include <mongocxx/read_concern.hpp>
#include <mongocxx/read_preference.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/write_concern.hpp>

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;
using namespace mongocxx;

//
// This example shows how to use the client_session::with_transaction helper to
// conveniently run a custom callback inside of a transaction.
//
int main() {
    // Start Transactions withTxn API Example 1

    // The mongocxx::instance constructor and destructor initialize and shut down the driver,
    // respectively. Therefore, a mongocxx::instance must be created before using the driver and
    // must remain alive for as long as the driver is in use.
    mongocxx::instance inst{};

    // For a replica set, include the replica set name and a seedlist of the members in the URI
    // string; e.g.
    // uriString =
    // 'mongodb://mongodb0.example.com:27017,mongodb1.example.com:27017/?replicaSet=myRepl'
    // For a sharded cluster, connect to the mongos instances; e.g.
    // uriString = 'mongodb://mongos0.example.com:27017,mongos1.example.com:27017/'
    mongocxx::client client{mongocxx::uri{"mongodb://localhost/?replicaSet=replset"}};

    write_concern wc_majority{};
    wc_majority.acknowledge_level(write_concern::level::k_majority);

    read_concern rc_local{};
    rc_local.acknowledge_level(read_concern::level::k_local);

    read_preference rp_primary{};
    rp_primary.mode(read_preference::read_mode::k_primary);

    // Prereq: Create collections.

    auto foo = client["mydb1"]["foo"];
    auto bar = client["mydb2"]["bar"];

    try {
        options::insert opts;
        opts.write_concern(wc_majority);

        foo.insert_one(make_document(kvp("abc", 0)), opts);
        bar.insert_one(make_document(kvp("xyz", 0)), opts);
    } catch (const mongocxx::exception& e) {
        std::cout << "An exception occurred while inserting: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    // Step 1: Define the callback that specifies the sequence of operations to perform inside the
    // transactions.
    client_session::with_transaction_cb callback = [&](client_session* session) {
        // Important::  You must pass the session to the operations.
        foo.insert_one(*session, make_document(kvp("abc", 1)));
        bar.insert_one(*session, make_document(kvp("xyz", 999)));
    };

    // Step 2: Start a client session
    auto session = client.start_session();

    // Step 3: Use with_transaction to start a transaction, execute the callback,
    // and commit (or abort on error).
    try {
        options::transaction opts;
        opts.write_concern(wc_majority);
        opts.read_concern(rc_local);
        opts.read_preference(rp_primary);

        session.with_transaction(callback, opts);
    } catch (const mongocxx::exception& e) {
        std::cout << "An exception occurred: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;

    // End Transactions withTxn API Example 1
}
