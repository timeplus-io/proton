// Copyright 2020 MongoDB Inc.
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

#include <bsoncxx/json.hpp>
#include <bsoncxx/stdx/string_view.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <bsoncxx/test/catch.hh>
#include <bsoncxx/types/bson_value/value.hpp>
#include <bsoncxx/types/bson_value/view.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/test/spec/monitoring.hh>
#include <mongocxx/test/spec/operation.hh>
#include <mongocxx/test/spec/util.hh>

namespace {

using namespace bsoncxx::stdx;
using namespace bsoncxx::string;
using namespace mongocxx::spec;

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

// Set serverSelectionTryOnce to false; ADL starts up in the background, and may not be
// ready to receive connections yet when the test begins running.
const std::string kMongohouseURI =
    "mongodb://mhuser:pencil@localhost/?serverSelectionTryOnce=false";

void run_mongohouse_tests_in_file(std::string test_path) {
    return run_crud_tests_in_file(test_path, uri{kMongohouseURI});
}

// Test that the driver properly constructs and issues a killCursors command to Atlas Data Lake.
void test_kill_cursors() {
    instance::current();

    options::client client_opts;
    apm_checker apm_checker;
    client_opts.apm_opts(apm_checker.get_apm_opts(false));
    client client{uri{kMongohouseURI}, client_opts};

    // Issue a query that will leave a cursor open on the server.
    options::find find_opts;
    find_opts.batch_size(2);
    find_opts.limit(3);

    bsoncxx::types::bson_value::value cursor_id_val{bsoncxx::types::bson_value::view{}};
    std::string cursor_ns;
    bool find_command_found = false;

    {
        auto cursor = client["test"]["driverdata"].find({}, find_opts);

        // Call begin() to run the find on the server.
        cursor.begin();

        // Observe the CommandSucceededEvent event for the find command.
        for (auto&& doc : apm_checker) {
            auto event = doc.view();
            if (event.find("command_succeeded_event") == event.end()) {
                continue;
            }

            if (event["command_succeeded_event"]["command_name"].get_string().value !=
                bsoncxx::stdx::string_view{"find"}) {
                continue;
            }

            // Grab cursor information.
            auto reply = event["command_succeeded_event"]["reply"].get_document();
            auto cursor_doc = reply.view()["cursor"].get_document();
            cursor_id_val = cursor_doc.view()["id"].get_owning_value();
            cursor_ns = std::string(cursor_doc.view()["ns"].get_string().value);

            find_command_found = true;

            break;
        }

        REQUIRE(find_command_found);

        apm_checker.clear();

        // Destroy the cursor with scope end.
    }

    bool cmd_started_validated = false;
    bool cmd_succeeded_validated = false;

    // Observe events for the killCursors command.
    for (auto&& doc : apm_checker) {
        auto event = doc.view();

        // Use the command started event for killCursors to validate cursor info.
        if (event.find("command_started_event") != event.end()) {
            if (event["command_started_event"]["command_name"].get_string().value !=
                bsoncxx::stdx::string_view{"killCursors"}) {
                continue;
            }

            // Validate namespace
            auto db = event["command_started_event"]["database_name"].get_string().value;
            auto coll = event["command_started_event"]["command"]["killCursors"].get_string().value;
            std::string cmd_ns(db);
            cmd_ns += ".";
            cmd_ns += std::string(coll);

            if (cmd_ns.compare(cursor_ns) != 0) {
                continue;
            }

            auto cursors_killed =
                event["command_started_event"]["command"]["cursors"].get_array().value;

            if (std::find(cursors_killed.cbegin(), cursors_killed.cend(), cursor_id_val.view()) !=
                cursors_killed.cend()) {
                cmd_started_validated = true;
            }
        }

        // Use the command succeeded event to confirm that the cursor was killed.
        if (event.find("command_succeeded_event") != event.end()) {
            if (event["command_succeeded_event"]["command_name"].get_string().value !=
                bsoncxx::stdx::string_view{"killCursors"}) {
                continue;
            }

            auto cursors_killed_elem = event["command_succeeded_event"]["reply"]["cursorsKilled"];
            auto cursors_killed_val = cursors_killed_elem.get_value();
            auto cursors_killed_arr = cursors_killed_elem.get_array();
            auto cursors_killed = cursors_killed_arr.value;

            if (std::find(cursors_killed.cbegin(), cursors_killed.cend(), cursor_id_val.view()) !=
                cursors_killed.cend()) {
                cmd_succeeded_validated = true;
            }
        }
    }

    REQUIRE(cmd_started_validated);
    REQUIRE(cmd_succeeded_validated);
}

// Test that the driver can establish a connection with Atlas Data Lake without authentication.
// For these tests, create a MongoClient using a valid connection string without auth
// credentials and execute a ping command.
void test_connection_without_auth() {
    instance::current();

    client client{uri{}, test_util::add_test_server_api()};
    client["admin"].run_command(make_document(kvp("ping", 1)));
}

// Test that the driver can establish a connection with Atlas Data Lake with authentication.
// For these tests, create a MongoClient using a valid connection string with SCRAM-SHA-1
// and credentials from the drivers-evergreen-tools ADL configuration and execute a ping
// command. Repeat this test using SCRAM-SHA-256.
void test_auth_with_scram_sha() {
    instance::current();

    client client1{uri{"mongodb://mhuser:pencil@localhost/?authMechanism=SCRAM-SHA-1"}};
    client1["admin"].run_command(make_document(kvp("ping", 1)));

    client client256{uri{"mongodb://mhuser:pencil@localhost/?authMechanism=SCRAM-SHA-256"}};
    client256["admin"].run_command(make_document(kvp("ping", 1)));
}

TEST_CASE("Test mongohouse", "[mongohouse]") {
    instance::current();

    run_tests_in_suite("MONGOHOUSE_TESTS_PATH", &run_mongohouse_tests_in_file);

    // Run prose tests
    test_kill_cursors();
    test_connection_without_auth();
    test_auth_with_scram_sha();
}

}  // namespace
