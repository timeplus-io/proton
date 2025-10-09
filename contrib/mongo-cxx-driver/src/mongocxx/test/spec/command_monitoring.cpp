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

#include <bsoncxx/document/view.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <bsoncxx/test/catch.hh>
#include <mongocxx/client.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/test/client_helpers.hh>
#include <mongocxx/test/spec/operation.hh>
#include <mongocxx/test/spec/util.hh>

namespace {
using namespace bsoncxx;
using namespace mongocxx;

void initialize_collection(document::view test_spec_view) {
    std::string db_name = string::to_string(test_spec_view["database_name"].get_string().value);
    std::string col_name = string::to_string(test_spec_view["collection_name"].get_string().value);

    client temp_client{uri{}, test_util::add_test_server_api()};
    collection temp_col = temp_client[db_name][col_name];

    temp_col.drop();

    if (test_spec_view["data"]) {
        // insert this data
        array::view arr = test_spec_view["data"].get_array().value;

        for (array::element el : arr) {
            temp_col.insert_one(el.get_document().value);
        }
    }
}

void run_command_monitoring_tests_in_file(std::string test_path) {
    INFO("Test path: " << test_path);
    bsoncxx::stdx::optional<document::value> test_spec = test_util::parse_test_file(test_path);

    REQUIRE(test_spec);

    document::view test_spec_view = test_spec->view();

    std::string db_name = string::to_string(test_spec_view["database_name"].get_string().value);
    std::string col_name = string::to_string(test_spec_view["collection_name"].get_string().value);

    array::view tests = test_spec_view["tests"].get_array().value;

    for (auto&& test : tests) {
        initialize_collection(test_spec_view);
        std::string description = string::to_string(test["description"].get_string().value);
        INFO("Test description: " << description);
        array::view expectations = test["expectations"].get_array().value;

        // Use a separate client to check version info, so as not to interfere with APM
        client temp_client{uri{}, test_util::add_test_server_api()};
        if (spec::should_skip_spec_test(temp_client, test.get_document().value)) {
            return;
        }

        // Used by the listeners
        auto events = expectations.begin();

        options::client client_opts;
        options::apm apm_opts;

        ///////////////////////////////////////////////////////////////////////
        // Begin command listener lambdas
        ///////////////////////////////////////////////////////////////////////

        // COMMAND STARTED
        apm_opts.on_command_started([&](const events::command_started_event& event) {
            if (event.command_name().compare("endSessions") == 0) {
                return;
            }

            auto expected = (*events).get_document().value;
            events++;

            for (auto ele : expected["command_started_event"].get_document().value) {
                bsoncxx::stdx::string_view field{ele.key()};
                types::bson_value::view value{ele.get_value()};

                if (field.compare("command_name") == 0) {
                    REQUIRE(event.command_name() == value.get_string().value);
                } else if (field.compare("command") == 0) {
                    document::view expected_command = value.get_document().value;
                    document::view command = event.command();
                    REQUIRE_BSON_MATCHES(command, expected_command);
                } else if (field.compare("database_name") == 0) {
                    REQUIRE(event.database_name() == value.get_string().value);
                } else {
                    // Should not happen.
                    REQUIRE(false);
                }
            }
        });

        // COMMAND FAILED
        apm_opts.on_command_failed([&](const events::command_failed_event& event) {
            auto expected = (*events).get_document().value;
            events++;

            for (auto ele : expected["command_failed_event"].get_document().value) {
                bsoncxx::stdx::string_view field{ele.key()};
                types::bson_value::view value{ele.get_value()};

                if (field.compare("command_name") == 0) {
                    REQUIRE(event.command_name() == value.get_string().value);
                } else {
                    // Should not happen.
                    REQUIRE(false);
                }
            }
        });

        // COMMAND SUCCESS
        apm_opts.on_command_succeeded([&](const events::command_succeeded_event& event) {
            if (event.command_name().compare("endSessions") == 0) {
                return;
            }

            auto expected = (*events).get_document().value;
            events++;

            for (auto ele : expected["command_succeeded_event"].get_document().value) {
                bsoncxx::stdx::string_view field{ele.key()};
                types::bson_value::view value{ele.get_value()};

                if (field.compare("command_name") == 0) {
                    REQUIRE(event.command_name() == value.get_string().value);
                } else if (field.compare("reply") == 0) {
                    document::view expected_reply = value.get_document().value;
                    document::view reply = event.reply();
                    REQUIRE_BSON_MATCHES(reply, expected_reply);
                } else {
                    // Should not happen.
                    REQUIRE(false);
                }
            }
        });

        ///////////////////////////////////////////////////////////////////////
        // End command listener lambdas
        ///////////////////////////////////////////////////////////////////////

        // Apply listeners, and run operations.
        client_opts.apm_opts(apm_opts);
        client client{uri{}, test_util::add_test_server_api(client_opts)};

        collection coll = client[db_name][col_name];

        document::view operation = test["operation"].get_document().value;
        std::string operation_name = string::to_string(operation["name"].get_string().value);
        spec::operation_runner op_runner{&coll};

        try {
            op_runner.run(operation);
        } catch (mongocxx::exception& e) {
            // do nothing.
        }

        REQUIRE(events == expectations.end());
    }
}

TEST_CASE("Command Monitoring Spec Tests", "[command_monitoring_spec]") {
    instance::current();
    char* command_monitoring_tests_path = std::getenv("COMMAND_MONITORING_TESTS_PATH");
    REQUIRE(command_monitoring_tests_path);

    std::string path{command_monitoring_tests_path};

    if (path.back() == '/') {
        path.pop_back();
    }

    std::ifstream test_files{path + "/test_files.txt"};

    REQUIRE(test_files.good());

    std::string test_file;

    while (std::getline(test_files, test_file)) {
        run_command_monitoring_tests_in_file(path + "/" + test_file);
    }
}
}  // namespace
