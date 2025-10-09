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

#include <fstream>
#include <set>
#include <string>
#include <vector>

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/exception/exception.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <bsoncxx/test/catch.hh>
#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>
#include <mongocxx/database.hpp>
#include <mongocxx/exception/error_code.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/exception/server_error_code.hpp>
#include <mongocxx/options/distinct.hpp>
#include <mongocxx/options/find_one_and_delete.hpp>
#include <mongocxx/pipeline.hpp>
#include <mongocxx/result/delete.hpp>
#include <mongocxx/result/insert_one.hpp>
#include <mongocxx/test/spec/monitoring.hh>
#include <mongocxx/test/spec/operation.hh>
#include <mongocxx/test/spec/util.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace spec {

using namespace bsoncxx;
using namespace mongocxx;
using namespace spec;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_array;
using bsoncxx::builder::basic::make_document;
using bsoncxx::stdx::optional;
using bsoncxx::stdx::string_view;

static const int kMaxHelloFailCommands = 7;

uint32_t error_code_from_name(string_view name) {
    if (name.compare("CannotSatisfyWriteConcern") == 0) {
        return 100;
    } else if (name.compare("DuplicateKey") == 0) {
        return 11000;
    } else if (name.compare("NoSuchTransaction") == 0) {
        return 251;
    } else if (name.compare("WriteConflict") == 0) {
        return 112;
    } else if (name.compare("Interrupted") == 0) {
        return 11601;
    } else if (name.compare("MaxTimeMSExpired") == 0) {
        return 50;
    } else if (name.compare("UnknownReplWriteConcern") == 0) {
        return 79;
    } else if (name.compare("UnsatisfiableWriteConcern") == 0) {
        return 100;
    } else if (name.compare("OperationNotSupportedInTransaction") == 0) {
        return 263;
    } else if (name.compare("APIStrictError") == 0) {
        return 323;
    } else if (name.compare("ChangeStreamFatalError") == 0) {
        return 280;
    }

    return 0;
}

/* Called with the entire test file and individual tests. */
bool should_skip_spec_test(const client& client, document::view test) {
    std::set<std::string> unsupported_tests = {
        "CreateIndex and dropIndex omits default write concern",
        "MapReduce omits default write concern",
        "Deprecated count with empty collection",
        "Deprecated count with collation",
        "Deprecated count without a filter",
        "Deprecated count with a filter",
        "Deprecated count with skip and limit",

        // CXX-2678: missing required runCommand interface to set readPreference.
        "run command fails with explicit secondary read preference",
    };

    if (test["description"]) {
        std::string description = std::string(test["description"].get_string().value);
        if (unsupported_tests.find(description) != unsupported_tests.end()) {
            UNSCOPED_INFO("Test skipped - " << description << "\n"
                                            << "reason: unsupported in C++ driver");
            return true;
        }
    }

    if (test["skipReason"]) {
        UNSCOPED_INFO("Test skipped - " << test["description"].get_string().value << "\n"
                                        << "reason: " << test["skipReason"].get_string().value);
        return true;
    }

    auto run_mongohouse_tests = std::getenv("RUN_MONGOHOUSE_TESTS");
    if (run_mongohouse_tests && std::string(run_mongohouse_tests) == "ON") {
        // mongohoused does not return `version` field in response to serverStatus.
        // Exit early to run the test.
        return false;
    }

    std::string server_version = test_util::get_server_version(client);

    std::string topology = test_util::get_topology(client);

    if (test["ignore_if_server_version_greater_than"]) {
        std::string max_server_version = bsoncxx::string::to_string(
            test["ignore_if_server_version_greater_than"].get_string().value);
        if (test_util::compare_versions(server_version, max_server_version) > 0) {
            return true;
        }
    }

    auto should_skip = [&](document::view requirements) {
        if (requirements["topology"]) {
            auto topologies = requirements["topology"].get_array().value;
            bool found = false;
            for (auto&& el : topologies) {
                if (std::string(el.get_string().value) == topology) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return true;
            }
        }

        if (requirements["minServerVersion"]) {
            auto min_server_version =
                string::to_string(requirements["minServerVersion"].get_string().value);
            if (test_util::compare_versions(server_version, min_server_version) < 0) {
                return true;
            }
        }

        if (requirements["maxServerVersion"]) {
            auto max_server_version =
                string::to_string(requirements["maxServerVersion"].get_string().value);
            if (test_util::compare_versions(server_version, max_server_version) > 0) {
                return true;
            }
        }

        return false;
    };

    if (test["runOn"]) {
        for (auto&& el : test["runOn"].get_array().value) {
            if (!should_skip(el.get_document())) {
                return false;
            }
        }
    } else if (!should_skip(test)) {
        return false;
    }

    UNSCOPED_INFO("Skipping - unsupported server version '" + server_version + "' with topology '" +
                  topology + "'");
    return true;
}

void configure_fail_point(const client& client, document::view test) {
    if (test["failPoint"]) {
        client["admin"].run_command(test["failPoint"].get_document().value);
    }
}

void disable_fail_point(const client& client, stdx::string_view fail_point) {
    /* Some transactions tests have a failCommand for "hello" repeat seven times. */
    for (int i = 0; i < kMaxHelloFailCommands; i++) {
        try {
            client["admin"].run_command(
                make_document(kvp("configureFailPoint", fail_point), kvp("mode", "off")));
            break;
        } catch (const std::exception&) {
            /* Tests that fail with hello also fail to disable the failpoint
             * (since we run hello when opening the connection). Ignore those
             * errors. */
            continue;
        }
    }
}

struct fail_point_guard_type {
    mongocxx::client& client;
    std::string name;

    fail_point_guard_type(mongocxx::client& client, std::string name)
        : client(client), name(std::move(name)) {}

    ~fail_point_guard_type() {
        disable_fail_point(client, name);
    }

    fail_point_guard_type(const fail_point_guard_type&) = delete;
    fail_point_guard_type& operator=(const fail_point_guard_type&) = delete;
    fail_point_guard_type(fail_point_guard_type&&) = delete;
    fail_point_guard_type& operator=(fail_point_guard_type&&) = delete;
};

void disable_fail_point(std::string uri_string,
                        options::client client_opts,
                        stdx::string_view fail_point) {
    mongocxx::client client = {uri{uri_string}, client_opts};
    disable_fail_point(client, fail_point);
}

void disable_targeted_fail_point(std::uint32_t server_id) {
    const auto command_owner =
        make_document(kvp("configureFailPoint", "failCommand"), kvp("mode", "off"));
    const auto command = command_owner.view();

    // Some transactions tests have a failCommand for "hello" repeat seven times.
    for (int i = 0; i < kMaxHelloFailCommands; i++) {
        try {
            // Create a new client for every attempt to force server discovery from scratch to
            // guarantee the hello or isMaster fail points are actually triggered on the required
            // mongos.
            mongocxx::client client = {uri{"mongodb://localhost:27017,localhost:27018"},
                                       test_util::add_test_server_api()};
            client["admin"].run_command(command, server_id);
            break;
        } catch (...) {
            continue;
        }
    }
}

struct targeted_fail_point_guard_type {
    std::uint32_t server_id;

    targeted_fail_point_guard_type(std::uint32_t server_id) : server_id(server_id) {
        REQUIRE(server_id != 0);
    }

    ~targeted_fail_point_guard_type() {
        try {
            disable_targeted_fail_point(server_id);
        } catch (...) {
        }
    }

    targeted_fail_point_guard_type(const targeted_fail_point_guard_type&) = delete;
    targeted_fail_point_guard_type& operator=(const targeted_fail_point_guard_type&) = delete;
    targeted_fail_point_guard_type(targeted_fail_point_guard_type&&) = delete;
    targeted_fail_point_guard_type& operator=(targeted_fail_point_guard_type&&) = delete;
};

void set_up_collection(const client& client,
                       document::view test,
                       string_view database_name,
                       string_view collection_name) {
    write_concern wc_majority;
    wc_majority.acknowledge_level(write_concern::level::k_majority);

    const auto db_name = test[database_name].get_string().value;
    const auto coll_name = test[collection_name].get_string().value;

    // Create a collection object from the MongoClient, using the `database_name` and
    // `collection_name` fields of the YAML file.
    auto db = client[db_name];
    auto coll = db[coll_name];

    // For compatibility with Client Side Encryption tests.
    bsoncxx::builder::basic::document opts;
    if (const auto ef = test["encrypted_fields"]) {
        opts.append(kvp("encryptedFields", ef.get_document().value));
    }

    // Drop the test collection, using writeConcern "majority".
    coll.drop(wc_majority, opts.view());

    // For compatibility with Client Side Encryption tests.
    if (test["json_schema"]) {
        opts.append(
            kvp("validator",
                make_document(kvp("$jsonSchema", test["json_schema"].get_document().value))));
    }

    // Execute the "create" command to recreate the collection, using writeConcern "majority".
    coll = db.create_collection(coll_name, opts.view(), wc_majority);

    // Seed collection with data, if we have it
    if (const auto data = test["data"]) {
        options::insert insert_opts;
        insert_opts.write_concern(wc_majority);

        for (auto&& doc : data.get_array().value) {
            coll.insert_one(doc.get_document().value, insert_opts);
        }
    }

    // When testing against a sharded cluster run a `distinct` command on the newly created
    // collection on all mongoses.
    if (test_util::is_sharded_cluster(client)) {
        auto s0 = mongocxx::client(uri("mongodb://localhost:27017"));
        auto s1 = mongocxx::client(uri("mongodb://localhost:27018"));

        s0[db_name][coll_name].distinct("x", {});
        s1[db_name][coll_name].distinct("x", {});
    }
}

void initialize_collection(collection* coll, array::view initial_data) {
    // Deleting all documents from the collection has much better performance than dropping the
    // collection
    coll->delete_many({});

    std::vector<document::view> documents_to_insert;
    for (auto&& document : initial_data) {
        documents_to_insert.push_back(document.get_document().value);
    }

    if (documents_to_insert.size() > 0) {
        write_concern wc_majority;
        wc_majority.acknowledge_level(write_concern::level::k_majority);

        options::insert insert_opts;
        insert_opts.write_concern(wc_majority);

        coll->insert_many(documents_to_insert, insert_opts);
    }
}

void parse_database_options(document::view op, database* out) {
    if (op["databaseOptions"]) {
        auto rc = lookup_read_concern(op["databaseOptions"].get_document());
        if (rc) {
            out->read_concern(*rc);
        }

        auto wc = lookup_write_concern(op["databaseOptions"].get_document());
        if (wc) {
            out->write_concern(*wc);
        }

        auto rp = lookup_read_preference(op["databaseOptions"].get_document());
        if (rp) {
            out->read_preference(*rp);
        }
    }
}

void parse_collection_options(document::view op, collection* out) {
    if (op["collectionOptions"]) {
        auto rc = lookup_read_concern(op["collectionOptions"].get_document());
        if (rc) {
            out->read_concern(*rc);
        }

        auto wc = lookup_write_concern(op["collectionOptions"].get_document());
        if (wc) {
            out->write_concern(*wc);
        }

        auto rp = lookup_read_preference(op["collectionOptions"].get_document());
        if (rp) {
            out->read_preference(*rp);
        }
    }
}

void run_operation_check_result(document::view op, make_op_runner_fn make_op_runner) {
    std::string error_msg;
    optional<document::value> server_error;
    optional<operation_exception> op_exception;
    optional<std::exception> exception;
    optional<document::value> actual_result;

    INFO("Operation: " << bsoncxx::to_json(op));
    try {
        auto op_runner = make_op_runner();
        actual_result = op_runner.run(op);
    } catch (const operation_exception& e) {
        error_msg = e.what();
        server_error = e.raw_server_error();
        op_exception = e;
        exception = e;
    } catch (std::exception& e) {
        error_msg = e.what();
        exception = e;
    }

    // "If the result document has an 'errorContains' field, verify that the method threw an
    // exception or returned an error, and that the value of the 'errorContains' field
    // matches the error string."
    if (op["result"]["errorContains"]) {
        if (!exception) {
            REQUIRE(actual_result);
            FAIL("expected an error, got: " << bsoncxx::to_json(*actual_result));
        }
        INFO("expected error message " << op["result"]["errorContains"].get_string().value);
        INFO("got error message" << error_msg);
        // Do a case insensitive check.
        auto error_contains =
            test_util::tolowercase(op["result"]["errorContains"].get_string().value);
        REQUIRE(test_util::tolowercase(error_msg).find(error_contains) < error_msg.length());
    } else if (exception) {
        CAPTURE(server_error);
        FAIL("unexpected exception: " << error_msg);
    }

    // "If the result document has an 'errorCodeName' field, verify that the method threw a
    // command failed exception or returned an error, and that the value of the
    // 'errorCodeName' field matches the 'codeName' in the server error response."
    if (op["result"]["errorCodeName"]) {
        if (!op_exception) {
            REQUIRE(actual_result);
            FAIL("expected an error, got: " << bsoncxx::to_json(*actual_result));
        }
        REQUIRE(op_exception);
        uint32_t expected = error_code_from_name(op["result"]["errorCodeName"].get_string().value);
        REQUIRE(op_exception->code().value() == static_cast<int>(expected));
    }

    // "If the result document has an 'errorLabelsContain' field, [...] Verify that all of
    // the error labels in 'errorLabelsContain' are present"
    if (op["result"]["errorLabelsContain"]) {
        if (!op_exception) {
            REQUIRE(actual_result);
            FAIL("expected an error, got: " << bsoncxx::to_json(*actual_result));
        }
        for (auto&& label_el : op["result"]["errorLabelsContain"].get_array().value) {
            auto label = label_el.get_string().value;
            REQUIRE(op_exception->has_error_label(label));
        }
    }

    // "If the result document has an 'errorLabelsOmit' field, [...] Verify that none of the
    // error labels in 'errorLabelsOmit' are present."
    if (op["result"]["errorLabelsOmit"]) {
        if (!op_exception) {
            REQUIRE(actual_result);
            FAIL("expected an error, got: " << bsoncxx::to_json(*actual_result));
        }
        for (auto&& label_el : op["result"]["errorLabelsOmit"].get_array().value) {
            auto label = label_el.get_string().value;
            REQUIRE(!op_exception->has_error_label(label));
        }
    }

    // "If the operation returns a raw command response, eg from runCommand, then compare
    // only the fields present in the expected result document. Otherwise, compare the
    // method's return value to result using the same logic as the CRUD Spec Tests runner."
    if (!exception && op["result"]) {
        REQUIRE(actual_result);
        REQUIRE(actual_result->view()["result"]);
        INFO("actual result: " << bsoncxx::to_json(actual_result->view()));
        INFO("expected result: " << bsoncxx::to_json(op));

        REQUIRE(test_util::matches(actual_result->view()["result"].get_value(),
                                   op["result"].get_value()));
    }
}

uri get_uri(document::view test) {
    std::string uri_string = "mongodb://localhost:27017/?";

    if (test_util::is_sharded_cluster()) {
        const auto use_multiple_mongoses = test["useMultipleMongoses"];
        if (use_multiple_mongoses && use_multiple_mongoses.get_bool().value) {
            // If true, and the topology type is Sharded, the MongoClient for this test should be
            // initialized with multiple mongos seed addresses. If false or omitted, only a single
            // mongos address should be specified.
            uri_string = "mongodb://localhost:27017,localhost:27018/?";

            // Verify that both mongos are actually present.
            const mongocxx::client client0 = {uri{"mongodb://localhost:27017"},
                                              test_util::add_test_server_api()};
            const mongocxx::client client1 = {uri{"mongodb://localhost:27018"},
                                              test_util::add_test_server_api()};

            if (!client0["config"].has_collection("shards")) {
                FAIL("missing required mongos on port 27017 with useMultipleMongoses=true");
            }

            if (!client1["config"].has_collection("shards")) {
                FAIL("missing required mongos on port 27018 with useMultipleMongoses=true");
            }
        }
    }

    auto add_opt = [&uri_string](std::string opt) {
        if (uri_string.back() != '?') {
            uri_string += '&';
        }
        uri_string += opt;
    };

    if (test["clientOptions"]) {
        if (test["clientOptions"]["retryWrites"]) {
            add_opt(std::string("retryWrites=") +
                    (test["clientOptions"]["retryWrites"].get_bool().value ? "true" : "false"));
        }
        if (test["clientOptions"]["retryReads"]) {
            add_opt(std::string("retryReads=") +
                    (test["clientOptions"]["retryReads"].get_bool().value ? "true" : "false"));
        }
        if (test["clientOptions"]["readConcernLevel"]) {
            add_opt("readConcernLevel=" +
                    std::string(test["clientOptions"]["readConcernLevel"].get_string().value));
        }
        if (test["clientOptions"]["w"]) {
            if (test["clientOptions"]["w"].type() == type::k_int32) {
                add_opt("w=" + std::to_string(test["clientOptions"]["w"].get_int32().value));
            } else {
                add_opt("w=" + string::to_string(test["clientOptions"]["w"].get_string().value));
            }
        }
        if (test["clientOptions"]["heartbeatFrequencyMS"]) {
            add_opt(
                "heartbeatFrequencyMS=" +
                std::to_string(test["clientOptions"]["heartbeatFrequencyMS"].get_int32().value));
        }
        if (test["clientOptions"]["readPreference"]) {
            add_opt("readPreference=" +
                    string::to_string(test["clientOptions"]["readPreference"].get_string().value));
        }
    }
    return uri{uri_string};
}

void run_tests_in_suite(std::string ev, test_runner cb, std::set<std::string> unsupported_tests) {
    char* tests_path = std::getenv(ev.c_str());
    INFO("checking for path from environment variable: " << ev);
    INFO("test path is: " << tests_path);
    REQUIRE(tests_path);

    std::string path{tests_path};
    if (path.back() == '/') {
        path.pop_back();
    }

    std::ifstream test_files{path + "/test_files.txt"};
    REQUIRE(test_files.good());

    std::string test_file;
    while (std::getline(test_files, test_file)) {
        SECTION(test_file) {
            if (unsupported_tests.find(test_file) != unsupported_tests.end()) {
                WARN("Skipping unsupported test file: " << test_file);
            } else {
                cb(path + "/" + test_file);
            }
        }
    }
}

void run_tests_in_suite(std::string ev, test_runner cb) {
    std::set<std::string> empty;
    run_tests_in_suite(ev, cb, empty);
}

static void test_setup(document::view test, document::view test_spec) {
    // Step 2: Create a MongoClient and call `client.admin.runCommand({killAllSessions: []})` to
    // clean up any open transactions from previous test failures.
    client client{get_uri(test), test_util::add_test_server_api()};
    try {
        client["admin"].run_command(make_document(kvp("killAllSessions", make_array())));
    } catch (const mongocxx::exception& e) {
        // Ignore a command failure with error code 11601 ("Interrupted") to work around
        // SERVER-38335.
        if (e.code() != server_error_code(11601)) {
            FAIL("unexpected exception during killAllSessions: " << e.what());
        }
    }

    // Steps 2-7.
    set_up_collection(client, test_spec);

    // Step 8: If failPoint is specified, its value is a configureFailPoint command.
    configure_fail_point(client, test);
}

void parse_session_opts(document::view session_opts, options::client_session* out) {
    options::transaction txn_opts;
    if (session_opts["defaultTransactionOptions"]) {
        if (auto rc =
                lookup_read_concern(session_opts["defaultTransactionOptions"].get_document())) {
            txn_opts.read_concern(*rc);
        }

        if (auto wc =
                lookup_write_concern(session_opts["defaultTransactionOptions"].get_document())) {
            txn_opts.write_concern(*wc);
        }

        if (auto rp =
                lookup_read_preference(session_opts["defaultTransactionOptions"].get_document())) {
            txn_opts.read_preference(*rp);
        }

        if (auto cc = session_opts["causalConsistency"]) {
            out->causal_consistency(cc.get_bool());
        }

        if (auto mct = session_opts["maxCommitTimeMS"]) {
            txn_opts.max_commit_time_ms(std::chrono::milliseconds(mct.get_int64()));
        }
    }

    out->default_transaction_opts(txn_opts);
}

using bsoncxx::stdx::string_view;
void run_transaction_operations(
    document::view test,
    client* client,
    string_view db_name,
    string_view coll_name,
    client_session* session0,
    client_session* session1,
    stdx::optional<targeted_fail_point_guard_type>* targeted_fail_point_guard,
    const apm_checker& apm_checker,
    bool throw_on_error = false) {
    auto operations = test["operations"].get_array().value;

    REQUIRE(session0);
    REQUIRE(session1);
    REQUIRE(targeted_fail_point_guard);

    for (auto&& op : operations) {
        std::string error_msg;
        optional<document::value> server_error;
        optional<operation_exception> exception;
        optional<document::value> actual_result;
        std::error_code ec;
        INFO("Operation: " << bsoncxx::to_json(op.get_document().value));

        const auto operation = op.get_document().value;

        // Handle with_transaction separately.
        if (operation["name"].get_string().value.compare("withTransaction") == 0) {
            const auto session = [&]() -> mongocxx::client_session* {
                const auto object = operation["object"].get_string().value;
                if (object.compare("session0") == 0) {
                    return session0;
                }

                if (object.compare("session1") == 0) {
                    return session1;
                }

                FAIL("unexpected session object: " << object);
                return nullptr;  // -Wreturn-type
            }();

            auto with_txn_test_cb = [&](client_session*) {
                // This will get called from client_session::with_transaction.
                // Run the nested operation(s) inside here.
                REQUIRE(operation["arguments"]);
                REQUIRE(operation["arguments"]["callback"]);
                auto callback = operation["arguments"]["callback"].get_document().value;
                run_transaction_operations(callback,
                                           client,
                                           db_name,
                                           coll_name,
                                           session0,
                                           session1,
                                           targeted_fail_point_guard,
                                           apm_checker,
                                           true);
            };

            try {
                session->with_transaction(with_txn_test_cb);
            } catch (const operation_exception& e) {
                error_msg = e.what();
                server_error = e.raw_server_error();
                exception = e;
                ec = e.code();
            } catch (const mongocxx::logic_error& e) {
                // CXX-1679: some tests trigger client errors that are thrown as logic_error rather
                // than operation_exception (i.e. update without $ operator).
                error_msg = e.what();
                exception.emplace(make_error_code(mongocxx::error_code(0)));
                ec = e.code();
            }
        } else {
            try {
                // Create a Database object from the MongoClient, using the `database_name` field at
                // the top level of the test file.
                auto db = client->database(db_name);
                parse_database_options(operation, &db);

                // Create a Collection object from the Database, using the `collection_name` field
                // at the top level of the test file. If collectionOptions or databaseOptions is
                // present, create the Collection or Database object with the provided options,
                // respectively. Otherwise create the object with the default options.
                auto coll = db[coll_name];
                parse_collection_options(operation, &coll);

                // Set the targetedFailPoint guard early to account for the possibility of an
                // exception being thrown after the targetedFailPoint operation but before
                // activating the disable guard. There is no harm attempting to disable a fail point
                // that hasn't been set.
                if (operation["name"] && operation["name"].get_string().value ==
                                             stdx::string_view("targetedFailPoint")) {
                    const auto arguments = operation["arguments"];

                    const auto session = [&]() -> mongocxx::client_session* {
                        const auto value = arguments["session"].get_string().value;
                        if (value == stdx::string_view("session0")) {
                            return session0;
                        }
                        if (value == stdx::string_view("session1")) {
                            return session1;
                        }
                        FAIL("unexpected session name: " << value);
                        return nullptr;  // -Wreturn-type
                    }();

                    // We expect and assume the name of the fail point is always "failCommand". To
                    // date, *all* legacy spec tests use "failCommand" as the fail point name.
                    REQUIRE(arguments["failPoint"]["configureFailPoint"].get_string().value ==
                            stdx::string_view("failCommand"));

                    // We expect at most one targetedFailPoint operation per test case.
                    REQUIRE(!(*targeted_fail_point_guard));

                    // When executing this operation, the test runner MUST keep a record of both the
                    // fail point and pinned mongos server so that the fail point can be disabled on
                    // the same mongos server after the test.
                    targeted_fail_point_guard->emplace(session->server_id());
                }

                actual_result =
                    operation_runner{&db, &coll, session0, session1, client}.run(operation);
            } catch (const operation_exception& e) {
                error_msg = e.what();
                server_error = e.raw_server_error();
                exception = e;
                ec = e.code();
            } catch (const mongocxx::logic_error& e) {
                // CXX-1679: some tests trigger client errors that are thrown as logic_error rather
                // than operation_exception (i.e. update without $ operator).
                error_msg = e.what();
                exception.emplace(make_error_code(mongocxx::error_code(0)));
                ec = e.code();
            }
        }

        CAPTURE(apm_checker.print_all());

        // "If the result document has an 'errorContains' field, verify that the method threw an
        // exception or returned an error, and that the value of the 'errorContains' field
        // matches the error string."
        if (op["result"]["errorContains"]) {
            REQUIRE(exception);
            // Do a case insensitive check.
            auto error_contains =
                test_util::tolowercase(op["result"]["errorContains"].get_string().value);

            REQUIRE_THAT(error_msg, Catch::Contains(error_contains, Catch::CaseSensitive::No));
        }

        // "If the result document has an 'errorCodeName' field, verify that the method threw a
        // command failed exception or returned an error, and that the value of the
        // 'errorCodeName' field matches the 'codeName' in the server error response."
        if (op["result"]["errorCodeName"]) {
            REQUIRE(exception);
            REQUIRE(server_error);
            uint32_t expected =
                error_code_from_name(op["result"]["errorCodeName"].get_string().value);
            REQUIRE(exception->code().value() == static_cast<int>(expected));
        }

        // "If the result document has an 'errorLabelsContain' field, [...] Verify that all of
        // the error labels in 'errorLabelsContain' are present"
        if (op["result"]["errorLabelsContain"]) {
            REQUIRE(exception);
            for (auto&& label_el : op["result"]["errorLabelsContain"].get_array().value) {
                auto label = label_el.get_string().value;
                if (!exception->has_error_label(label)) {
                    FAIL("Expected exception to contain the label '" << label
                                                                     << "' but it did not.\n");
                }
            }
        }

        // "If the result document has an 'errorLabelsOmit' field, [...] Verify that none of the
        // error labels in 'errorLabelsOmit' are present."
        if (op["result"]["errorLabelsOmit"]) {
            REQUIRE(exception);
            for (auto&& label_el : op["result"]["errorLabelsOmit"].get_array().value) {
                auto label = label_el.get_string().value;
                if (exception->has_error_label(label)) {
                    FAIL("Expected exception to NOT contain the label '" << label
                                                                         << "' but it did.\n");
                }
            }
        }

        // "If the operation returns a raw command response, eg from runCommand, then compare
        // only the fields present in the expected result document. Otherwise, compare the
        // method's return value to result using the same logic as the CRUD Spec Tests runner."
        if (!exception && op["result"]) {
            REQUIRE(actual_result);
            REQUIRE(actual_result->view()["result"]);
            INFO("actual result" << bsoncxx::to_json(actual_result->view()));
            INFO("expected result" << bsoncxx::to_json(op.get_document().value));
            REQUIRE(test_util::matches(actual_result->view()["result"].get_value(),
                                       op["result"].get_value()));
        }

        // If we are running inside with_transaction, we need to rethrow what we caught.
        if (throw_on_error && exception) {
            throw operation_exception(ec, std::move(*server_error), error_msg);
        }
    }
}

void run_transactions_tests_in_file(const std::string& test_path) {
    INFO("Test path: " << test_path);

    const auto test_spec = test_util::parse_test_file(test_path);
    REQUIRE(test_spec);

    const auto test_spec_view = test_spec->view();
    const auto db_name = test_spec_view["database_name"].get_string().value;
    const auto coll_name = test_spec_view["collection_name"].get_string().value;
    const auto tests = test_spec_view["tests"].get_array().value;

    /* we may not have a supported topology */
    if (should_skip_spec_test({uri{}, test_util::add_test_server_api()}, test_spec_view)) {
        WARN("File skipped - " + test_path);
        return;
    }

    for (auto&& test : tests) {
        const auto description = string::to_string(test["description"].get_string().value);

        SECTION(description) {
            client setup_client{get_uri(test.get_document().value),
                                test_util::add_test_server_api()};

            // Step 1: If the `skipReason` field is present, skip this test completely.
            if (should_skip_spec_test(setup_client, test.get_document().value)) {
                continue;
            }

            // Steps 2-8.
            test_setup(test.get_document().value, test_spec_view);

            {
                stdx::optional<fail_point_guard_type> fail_point_guard;
                if (test["failPoint"]) {
                    const auto fail_point_name = string::to_string(
                        test["failPoint"]["configureFailPoint"].get_string().value);
                    fail_point_guard.emplace(setup_client, fail_point_name);
                }

                // Step 9: Create a new MongoClient `client`, with Command Monitoring listeners
                // enabled. (Using a new MongoClient for each test ensures a fresh session pool that
                // hasn't executed any transactions previously, so the tests can assert actual
                // txnNumbers, starting from 1.) Pass this test's `clientOptions` if present
                // (handled by `get_uri()`).
                options::client client_opts;
                apm_checker apm_checker;
                client_opts.apm_opts(
                    apm_checker.get_apm_opts(true /* command_started_events_only */));
                client_opts = test_util::add_test_server_api(client_opts);
                client client = {get_uri(test.get_document().value), client_opts};

                options::client_session session0_opts;
                options::client_session session1_opts;

                if (const auto session_opts = test["sessionOptions"]) {
                    if (session_opts["session0"]) {
                        parse_session_opts(test["sessionOptions"]["session0"].get_document().value,
                                           &session0_opts);
                    }
                    if (session_opts["session1"]) {
                        parse_session_opts(test["sessionOptions"]["session1"].get_document().value,
                                           &session1_opts);
                    }
                }

                document::value session_lsid0{{}};
                document::value session_lsid1{{}};

                {
                    // Step 10: Call client.startSession twice to create ClientSession objects
                    // `session0` and `session1`, using the test's "sessionOptions" if they are
                    // present.
                    client_session session0 = client.start_session(session0_opts);
                    client_session session1 = client.start_session(session1_opts);

                    // Save their lsids so they are available after calling `endSession`.
                    session_lsid0.reset(session0.id());
                    session_lsid1.reset(session1.id());

                    // The test runner MUST also ensure that the configureFailPoint command is
                    // excluded from the list of observed command monitoring events for this client
                    // (if applicable).
                    apm_checker.clear();
                    apm_checker.set_ignore_command_monitoring_event("configureFailPoint");

                    // If a test uses `targetedFailPoint`, disable the fail point after running all
                    // `operations` to avoid spurious failures in subsequent tests.
                    stdx::optional<targeted_fail_point_guard_type> targeted_fail_point_guard;

                    // Step 11. Perform the operations.
                    run_transaction_operations(test.get_document().value,
                                               &client,
                                               db_name,
                                               coll_name,
                                               &session0,
                                               &session1,
                                               &targeted_fail_point_guard,
                                               apm_checker);

                    // Step 12: Call session0.endSession() and session1.endSession (via
                    // client_session dtors).
                }

                // Step 13. If the test includes a list of command-started events in expectations,
                // compare them to the actual command-started events using the same logic as the
                // legacy Command Monitoring Spec Tests runner, plus the rules in the
                // Command-Started Events instructions.
                test_util::match_visitor visitor =
                    [&](bsoncxx::stdx::string_view key,
                        bsoncxx::stdx::optional<bsoncxx::types::bson_value::view> main,
                        bsoncxx::types::bson_value::view pattern) {
                        if (key.compare("lsid") == 0) {
                            REQUIRE(pattern.type() == type::k_string);
                            REQUIRE(main);
                            REQUIRE(main->type() == type::k_document);
                            auto session_name = pattern.get_string().value;
                            if (session_name.compare("session0") == 0) {
                                REQUIRE(
                                    test_util::matches(session_lsid0, main->get_document().value));
                            } else {
                                REQUIRE(
                                    test_util::matches(session_lsid1, main->get_document().value));
                            }
                            return test_util::match_action::k_skip;
                        } else if (pattern.type() == type::k_null) {
                            if (main) {
                                return test_util::match_action::k_not_equal;
                            }
                            return test_util::match_action::k_skip;
                        } else if (key.compare("upsert") == 0 || key.compare("multi") == 0) {
                            // libmongoc includes `multi: false` and `upsert: false`.
                            // Some tests do not include `multi: false` and `upsert: false`
                            // in expectations. See DRIVERS-2271 and DRIVERS-976.
                            return test_util::match_action::k_skip;
                        }
                        return test_util::match_action::k_skip;
                    };

                if (test["expectations"]) {
                    apm_checker.compare(test["expectations"].get_array().value, false, visitor);
                }

                // Step 14: If failPoint is specified, disable the fail point to avoid spurious
                // failures in subsequent tests (fail_point_guard dtor).
            }

            // Step 15: For each element in outcome: ...
            if (test["outcome"] && test["outcome"]["collection"]) {
                auto outcome_coll_name = coll_name;
                if (test["outcome"]["collection"]["name"]) {
                    outcome_coll_name = test["outcome"]["collection"]["name"].get_string().value;
                }
                client client{get_uri(test.get_document().value), test_util::add_test_server_api()};
                auto coll = client[db_name][outcome_coll_name];
                test_util::check_outcome_collection(
                    &coll, test["outcome"]["collection"].get_document().value);
            }
        }
    }
}

void run_crud_tests_in_file(const std::string& test_path, uri test_uri) {
    using bsoncxx::builder::basic::kvp;
    using bsoncxx::builder::basic::make_document;

    using bsoncxx::string::to_string;

    INFO("Test path: " << test_path);
    optional<document::value> test_spec = test_util::parse_test_file(test_path);
    REQUIRE(test_spec);

    options::client client_opts;
    apm_checker apm_checker;
    client_opts.apm_opts(apm_checker.get_apm_opts(true /* command_started_events_only */));
    client_opts = test_util::add_test_server_api(client_opts);
    client client{std::move(test_uri), client_opts};

    document::view test_spec_view = test_spec->view();
    if (should_skip_spec_test(client, test_spec_view)) {
        return;
    }

    for (auto&& test : test_spec_view["tests"].get_array().value) {
        auto description = test["description"].get_string().value;
        SECTION(to_string(description)) {
            if (should_skip_spec_test(client, test.get_document())) {
                continue;
            }

            auto get_value_or_default = [&](std::string key, std::string default_str) {
                if (test_spec_view[key]) {
                    return to_string(test_spec_view[key].get_string().value);
                }
                return default_str;
            };

            auto database_name = get_value_or_default("database_name", "crud_test");
            auto collection_name = get_value_or_default("collection_name", "test");

            auto database = client[database_name];
            auto collection = database[collection_name];

            if (test_spec_view["data"]) {
                initialize_collection(&collection, test_spec_view["data"].get_array().value);
            }

            operation_runner op_runner{&database, &collection, nullptr, nullptr, &client};

            std::string outcome_collection_name = collection_name;
            if (test["outcome"] && test["outcome"]["collection"]["name"]) {
                outcome_collection_name =
                    to_string(test["outcome"]["collection"]["name"].get_string().value);
                auto outcome_collection = database[outcome_collection_name];
                initialize_collection(&outcome_collection, array::view{});
            }

            configure_fail_point(client, test.get_document().value);

            stdx::optional<fail_point_guard_type> fail_point_guard;
            if (test["failPoint"]) {
                const auto fail_point_name =
                    string::to_string(test["failPoint"]["configureFailPoint"].get_string().value);
                fail_point_guard.emplace(client, fail_point_name);
            }

            apm_checker.clear();
            auto perform_op =
                [&database, &op_runner, &test, &outcome_collection_name](document::view operation) {
                    optional<document::value> actual_outcome_value;
                    INFO("Operation: " << bsoncxx::to_json(operation));
                    try {
                        actual_outcome_value = op_runner.run(operation);
                    } catch (const mongocxx::operation_exception& e) {
                        REQUIRE([&operation, &test, &e]() {
                            if (operation["error"]) { /* v2 tests expect tests[i].operation.error */
                                return operation["error"].get_bool().value;
                            } else if (test["outcome"] && test["outcome"]["error"]) {
                                /* v1 tests expect tests[i].outcome.error (but some tests may
                                 have "outcome" without a nested "error") */
                                return test["outcome"]["error"].get_bool().value;
                            } else {
                                WARN("Caught operation exception: " << e.what());
                                return false;
                            }
                        }());
                        return; /* do not check results if error is expected */
                    } catch (const std::exception& e) {
                        WARN("Caught exception: " << e.what());
                    } catch (...) {
                        WARN("Caught unknown exception");
                    }

                    if (test["outcome"]) {
                        if (test["outcome"]["collection"]) {
                            auto outcome_collection = database[outcome_collection_name];
                            test_util::check_outcome_collection(
                                &outcome_collection,
                                test["outcome"]["collection"].get_document().value);
                        }

                        if (test["outcome"]["result"]) {
                            // wrap the result, since it might not be a document.
                            bsoncxx::document::view actual_outcome = actual_outcome_value->view();
                            auto actual_result_wrapped =
                                make_document(kvp("result", actual_outcome["result"].get_value()));
                            auto expected_result_wrapped =
                                make_document(kvp("result", test["outcome"]["result"].get_value()));
                            REQUIRE_BSON_MATCHES(actual_result_wrapped, expected_result_wrapped);
                        }
                    }
                };

            if (test["operations"]) {
                /* v2 tests expect a tests[i].operations array */
                for (auto&& operation : test["operations"].get_array().value) {
                    perform_op(operation.get_document().value);
                }
            } else if (test["operation"]) {
                /* v1 tests expect a single document, tests[i].operation */
                perform_op(test["operation"].get_document().value);
            }

            if (test["expectations"]) {
                /* some tests use empty documents, instead of arrays */
                if (test["expectations"].type() == type::k_array) {
                    apm_checker.compare(test["expectations"].get_array().value, true);
                } else {
                    REQUIRE(test["expectations"].get_document().view().empty());
                }
            }
        }
    }
}

}  // namespace spec
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
