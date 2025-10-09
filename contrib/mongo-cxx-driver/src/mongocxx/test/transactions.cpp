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
#include <unordered_set>

#include <bsoncxx/test/catch.hh>
#include <mongocxx/client.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/test/client_helpers.hh>

namespace {
using namespace mongocxx;

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_array;
using bsoncxx::builder::basic::make_document;

// Run on replica set with 1 node
TEST_CASE("Transaction tests", "[transactions]") {
    instance::current();
    client mongodb_client{uri{}, test_util::add_test_server_api()};

    if (!test_util::is_replica_set(mongodb_client)) {
        WARN("Skipping: transactions tests require replica set");
        return;
    } else if (test_util::get_max_wire_version(mongodb_client) < 7) {
        WARN("Skipping - transactions tests require max wire version is >= 7");
        return;
    }

    // The test run in first 3 SECTIONs below
    auto successful_insert_test = [&mongodb_client](
                                      client_session session,
                                      stdx::optional<options::transaction> transaction_opts) {
        auto db = mongodb_client["test"];
        auto coll = db["txn_test"];

        // Insert a document that should change in this test
        coll.insert_one(make_document(kvp("should_be_two", 1)));

        if (transaction_opts) {
            session.start_transaction(transaction_opts);
        } else {
            session.start_transaction();
        }

        try {
            coll.update_one(session,
                            make_document(kvp("should_be_two", 1)),
                            make_document(kvp("$set", make_document(kvp("should_be_two", 2)))));
            coll.insert_one(session, make_document(kvp("x", 1)));
        } catch (mongocxx::exception& e) {
            INFO("Collection transaction exception: " << e.what() << "\n");
            session.abort_transaction();
            REQUIRE(false);
        }

        try {
            session.commit_transaction();
        } catch (const operation_exception& e) {
            INFO("Exception raw:     " << bsoncxx::to_json(*(e.raw_server_error())) << "\n");
            INFO("Exception message: " << e.what() << "\n");

            if (e.has_error_label("TransientTransactionError")) {
                INFO("Transient error in transaction.\n");
                REQUIRE(false);
            } else {
                INFO("Non-transient error in transaction.\n");
                REQUIRE(false);
            }
        }

        // Document with key should_be_two should have a value of 2
        REQUIRE(coll.count_documents(make_document(kvp("should_be_two", 2))) == 1);
        // Document {x:1} should have also been inserted
        REQUIRE(coll.count_documents(make_document(kvp("x", 1))) == 1);

        // Cleanup
        coll.find_one_and_delete(make_document(kvp("should_be_two", 2)));
        coll.find_one_and_delete(make_document(kvp("x", 1)));
    };

    SECTION(
        "A move-constructed options::transactions object transfers properties and invalidates the "
        "original") {
        read_concern rc;
        rc.acknowledge_level(read_concern::level::k_majority);

        options::transaction src_opts;
        src_opts.read_concern(rc);

        options::transaction dst_opts(std::move(src_opts));
        REQUIRE(dst_opts.read_concern()->acknowledge_level() == read_concern::level::k_majority);

        auto session = mongodb_client.start_session();
        REQUIRE_THROWS_AS(session.start_transaction(src_opts), logic_error);
    }

    SECTION("Transaction options have no default values") {
        options::transaction transaction_opts;
        auto rc = transaction_opts.read_concern();
        auto wc = transaction_opts.write_concern();
        auto rp = transaction_opts.read_preference();

        REQUIRE(!rc);
        REQUIRE(!wc);
        REQUIRE(!rp);
    }

    SECTION("A transaction succeeds with default options") {
        successful_insert_test(mongodb_client.start_session(), {});
    }

    SECTION("Run transaction with session's default transaction options") {
        options::client_session client_opts;
        options::transaction transaction_opts;
        transaction_opts.read_preference(read_preference());
        client_opts.default_transaction_opts(transaction_opts);

        successful_insert_test(mongodb_client.start_session(client_opts), {});
    }

    SECTION("A transaction succeeds with explicit options") {
        options::transaction transaction_opts;
        transaction_opts.read_preference(read_preference());

        successful_insert_test(mongodb_client.start_session(), transaction_opts);
    }

    SECTION("A transaction can have maxCommitTimeMS") {
        // We simply pass this option through to libmongoc, so check that
        // it gets successfully set and returned.
        options::transaction opts;

        REQUIRE(!opts.max_commit_time_ms());

        opts.max_commit_time_ms(std::chrono::milliseconds{100});

        auto returned_opts = opts.max_commit_time_ms();
        REQUIRE(returned_opts);
        REQUIRE(returned_opts == std::chrono::milliseconds{100});
    }

    SECTION("Transaction commit can have a TransientTransactionError label") {
        auto db = mongodb_client["test"];
        auto coll = db["txn_test"];
        bool has_transient_error_null_str = false;
        bool has_transient_error_no_null_str = false;

        auto session = mongodb_client.start_session();

        // Insert a document that should NOT change in this test
        coll.insert_one(make_document(kvp("should_be_one", 1)));

        mongodb_client["admin"].run_command(
            make_document(kvp("configureFailPoint", "failCommand"),
                          kvp("mode", make_document(kvp("times", 1))),
                          kvp("data",
                              make_document(kvp("failCommands", make_array("update")),
                                            kvp("errorCode", 10107)))));
        session.start_transaction();

        try {
            coll.update_one(session,
                            make_document(kvp("should_be_one", 1)),
                            make_document(kvp("$set", make_document(kvp("should_be_one", 2)))));
        } catch (const operation_exception& e) {
            // Intentionally do NOT abort to force TransientTransactionError from server on commit.
            auto label = "TransientTransactionError";
            has_transient_error_null_str = e.has_error_label(label);
            has_transient_error_no_null_str = e.has_error_label(stdx::string_view(label, 25));
        }

        REQUIRE(has_transient_error_null_str);
        REQUIRE(has_transient_error_no_null_str);
        has_transient_error_null_str = false;
        has_transient_error_no_null_str = false;

        try {
            session.commit_transaction();
        } catch (const operation_exception& e) {
            auto label = "TransientTransactionError";
            has_transient_error_null_str = e.has_error_label(label);
            has_transient_error_no_null_str = e.has_error_label(stdx::string_view(label, 25));
        }

        mongodb_client["admin"].run_command(
            make_document(kvp("configureFailPoint", "failCommand"),
                          kvp("mode", "off"),
                          kvp("data",
                              make_document(kvp("failCommands", make_array("update")),
                                            kvp("errorCode", 10107)))));

        REQUIRE(has_transient_error_null_str);
        REQUIRE(has_transient_error_no_null_str);

        // Document {should_be_one:1} should not have been updated.
        REQUIRE(coll.count_documents(make_document(kvp("should_be_one", 2))) == 0);
        REQUIRE(coll.count_documents(make_document(kvp("should_be_one", 1))) == 1);

        // Cleanup
        coll.find_one_and_delete(make_document(kvp("should_be_one", 1)));
    }
}

TEST_CASE("Transactions Documentation Examples", "[transactions]") {
    instance::current();
    client client{uri{}, test_util::add_test_server_api()};

    if (!test_util::is_replica_set(client)) {
        WARN("Skipping: transactions tests require replica set");
        return;
    } else if (test_util::get_max_wire_version(client) < 7) {
        WARN("Skipping - transactions tests require max wire version is >= 7");
        return;
    }

    /* Create necessary collections. */
    client["hr"]["employees"].drop();
    client["hr"]["employees"].insert_one(
        make_document(kvp("employee", 3), kvp("status", "Active")));
    client["reporting"]["events"].drop();
    client["reporting"]["events"].insert_one(
        make_document(kvp("employee", 3), kvp("status", "Active")));

    SECTION("Intro Example 1") {
        // Start Transactions Intro Example 1
        auto update_employee_info = [](client_session& session) {
            auto& client = session.client();
            auto employees = client["hr"]["employees"];
            auto events = client["reporting"]["events"];

            options::transaction txn_opts;
            read_concern rc;
            rc.acknowledge_level(read_concern::level::k_snapshot);
            txn_opts.read_concern(rc);
            write_concern wc;
            wc.acknowledge_level(write_concern::level::k_majority);
            txn_opts.write_concern(wc);
            session.start_transaction(txn_opts);

            try {
                employees.update_one(
                    make_document(kvp("employee", 3)),
                    make_document(kvp("$set", make_document(kvp("status", "Inactive")))));
                events.insert_one(make_document(
                    kvp("employee", 3),
                    kvp("status", make_document(kvp("new", "Inactive"), kvp("old", "Active")))));
            } catch (const operation_exception& oe) {
                std::cout << "Caught exception during transaction, aborting." << std::endl;
                session.abort_transaction();
                throw oe;
            }

            while (true) {
                try {
                    session.commit_transaction();  // Uses write concern set at transaction start.
                    std::cout << "Transaction committed." << std::endl;
                    break;
                } catch (const operation_exception& oe) {
                    // Can retry commit.
                    if (oe.has_error_label("UnknownTransactionCommitResult")) {
                        std::cout << "UnknownTransactionCommitResult, retrying commit operation ..."
                                  << std::endl;
                        continue;
                    } else {
                        std::cout << "Error during commit ..." << std::endl;
                        throw oe;
                    }
                }
            }
        };
        // End Transactions Intro Example 1
        auto session = client.start_session();
        update_employee_info(session);
    }

    SECTION("Retry Example 1") {
        // Start Transactions Retry Example 1
        using transaction_func = std::function<void(client_session & session)>;
        auto run_transaction_with_retry = [](transaction_func txn_func, client_session& session) {
            while (true) {
                try {
                    txn_func(session);  // performs transaction.
                    break;
                } catch (const operation_exception& oe) {
                    std::cout << "Transaction aborted. Caught exception during transaction."
                              << std::endl;
                    // If transient error, retry the whole transaction.
                    if (oe.has_error_label("TransientTransactionError")) {
                        std::cout << "TransientTransactionError, retrying transaction ..."
                                  << std::endl;
                        continue;
                    } else {
                        throw oe;
                    }
                }
            }
        };
        // End Transactions Retry Example 1
        auto session = client.start_session();
        run_transaction_with_retry(
            [&client](client_session& session) {
                session.start_transaction();
                auto coll = client["test"]["coll"];
                coll.insert_one(make_document(kvp("x", 1)));
                session.commit_transaction();
            },
            session);
    }

    SECTION("Retry Example 2") {
        // Start Transactions Retry Example 2
        auto commit_with_retry = [](client_session& session) {
            while (true) {
                try {
                    session.commit_transaction();  // Uses write concern set at transaction start.
                    std::cout << "Transaction committed." << std::endl;
                    break;
                } catch (const operation_exception& oe) {
                    // Can retry commit
                    if (oe.has_error_label("UnknownTransactionCommitResult")) {
                        std::cout << "UnknownTransactionCommitResult, retrying commit operation ..."
                                  << std::endl;
                        continue;
                    } else {
                        std::cout << "Error during commit ..." << std::endl;
                        throw oe;
                    }
                }
            }
        };
        // End Transactions Retry Example 2
        auto session = client.start_session();
        session.start_transaction();
        auto coll = client["test"]["coll"];
        coll.insert_one(make_document(kvp("x", 1)));
        commit_with_retry(session);
    }

    SECTION("Retry Example 3") {
        // Start Transactions Retry Example 3
        using transaction_func = std::function<void(client_session & session)>;
        auto run_transaction_with_retry = [](transaction_func txn_func, client_session& session) {
            while (true) {
                try {
                    txn_func(session);  // performs transaction.
                    break;
                } catch (const operation_exception& oe) {
                    std::cout << "Transaction aborted. Caught exception during transaction."
                              << std::endl;
                    // If transient error, retry the whole transaction.
                    if (oe.has_error_label("TransientTransactionError")) {
                        std::cout << "TransientTransactionError, retrying transaction ..."
                                  << std::endl;
                        continue;
                    } else {
                        throw oe;
                    }
                }
            }
        };

        auto commit_with_retry = [](client_session& session) {
            while (true) {
                try {
                    session.commit_transaction();  // Uses write concern set at transaction start.
                    std::cout << "Transaction committed." << std::endl;
                    break;
                } catch (const operation_exception& oe) {
                    // Can retry commit
                    if (oe.has_error_label("UnknownTransactionCommitResult")) {
                        std::cout << "UnknownTransactionCommitResult, retrying commit operation ..."
                                  << std::endl;
                        continue;
                    } else {
                        std::cout << "Error during commit ..." << std::endl;
                        throw oe;
                    }
                }
            }
        };

        // Updates two collections in a transaction
        auto update_employee_info = [&](client_session& session) {
            auto& client = session.client();
            auto employees = client["hr"]["employees"];
            auto events = client["reporting"]["events"];

            options::transaction txn_opts;
            read_concern rc;
            rc.acknowledge_level(read_concern::level::k_snapshot);
            txn_opts.read_concern(rc);
            write_concern wc;
            wc.acknowledge_level(write_concern::level::k_majority);
            txn_opts.write_concern(wc);

            session.start_transaction(txn_opts);

            try {
                employees.update_one(
                    make_document(kvp("employee", 3)),
                    make_document(kvp("$set", make_document(kvp("status", "Inactive")))));
                events.insert_one(make_document(
                    kvp("employee", 3),
                    kvp("status", make_document(kvp("new", "Inactive"), kvp("old", "Active")))));
            } catch (const operation_exception& oe) {
                std::cout << "Caught exception during transaction, aborting." << std::endl;
                session.abort_transaction();
                throw oe;
            }

            commit_with_retry(session);
        };

        auto session = client.start_session();
        try {
            run_transaction_with_retry(update_employee_info, session);
        } catch (const operation_exception& oe) {
            // Do something with error.
            throw oe;
        }
        // End Transactions Retry Example 3
    }
}

TEST_CASE("Transactions Mongos Pinning Prose Tests", "[transactions]") {
    instance::current();

    if (test_util::compare_versions(test_util::get_server_version(), "4.1.6") < 0) {
        WARN("Skipping - requires server 4.1.6+");
        return;
    }

    if (test_util::get_topology() != "sharded") {
        WARN("Skipping - requires sharded cluster topology");
        return;
    }

    // @require_mongos_count_at_least(2)
    {
        mongocxx::client client{mongocxx::uri{"mongodb://localhost:27017"}};
        REQUIRE(client["config"].has_collection("shards"));
    };
    {
        mongocxx::client client{mongocxx::uri{"mongodb://localhost:27018"}};
        REQUIRE(client["config"].has_collection("shards"));
    };

    const auto uri =
        mongocxx::uri("mongodb://localhost:27017,localhost:27018/?localThresholdMS=1000");

    std::unordered_set<std::uint16_t> ports;

    options::apm apm_opts;
    apm_opts.on_command_started(
        [&](const events::command_started_event& event) { ports.insert(event.port()); });
    options::client client_opts;
    client_opts.apm_opts(apm_opts);
    mongocxx::client client{uri, client_opts};

    auto test = client["test"]["test"];
    test.insert_one(make_document());

    auto s = client.start_session();
    s.start_transaction();
    test.insert_one(s, make_document());
    s.commit_transaction();

    // Prose Test 1
    SECTION("Unpin for next transaction") {
        for (int i = 0; i < 50; ++i) {
            s.start_transaction();
            auto cursor = test.find(s, {});
            REQUIRE(cursor.begin() != cursor.end());
            s.commit_transaction();
        }

        REQUIRE(ports.size() > 1u);
    }

    // Prose Test 2
    SECTION("Unpin for non-transaction operation") {
        for (int i = 0; i < 50; ++i) {
            auto cursor = test.find(s, {});
            REQUIRE(cursor.begin() != cursor.end());
        }

        REQUIRE(ports.size() > 1u);
    }
}

}  // namespace
