// Copyright 2016 MongoDB Inc.
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

#include <bsoncxx/string/to_string.hpp>
#include <bsoncxx/test/catch.hh>
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

void run_retryable_reads_tests_in_file(std::string test_path) {
    INFO("Test path: " << test_path);
    optional<document::value> test_spec = test_util::parse_test_file(test_path);
    REQUIRE(test_spec);

    options::client client_opts;
    apm_checker apm_checker;
    apm_checker.set_ignore_command_monitoring_event("killCursors");
    client_opts.apm_opts(apm_checker.get_apm_opts(true /* command_started_events_only */));
    client_opts = test_util::add_test_server_api(client_opts);

    document::view test_spec_view = test_spec->view();
    for (auto&& test : test_spec_view["tests"].get_array().value) {
        client client{get_uri(test.get_document().value), client_opts};
        if (should_skip_spec_test(client, test_spec_view)) {
            return;
        }

        INFO("Test description: " << test["description"].get_string().value);
        if (should_skip_spec_test(client, test.get_document())) {
            continue;
        }

        auto get_value_or_default = [&](std::string key, std::string default_str) {
            if (test_spec_view[key]) {
                return to_string(test_spec_view[key].get_string().value);
            }
            return default_str;
        };

        auto database_name = get_value_or_default("database_name", "retryable_reads_test");
        auto collection_name = get_value_or_default("collection_name", "test");

        auto database = client[database_name];
        auto collection = database[collection_name];

        /* SPEC: GridFS tests are denoted by when the YAML file contains 'bucket_name'. */
        if (test_spec_view["bucket_name"]) {
            auto bucket_name = to_string(test_spec_view["bucket_name"].get_string().value);
            auto data = test_spec_view["data"].get_document().value;

            auto files_collection_name = bucket_name + ".files";
            auto chunks_collection_name = bucket_name + ".chunks";

            auto files_collection = database[files_collection_name];
            auto chunks_collection = database[chunks_collection_name];

            initialize_collection(&files_collection, data[files_collection_name].get_array().value);
            initialize_collection(&chunks_collection,
                                  data[chunks_collection_name].get_array().value);
        } else {
            initialize_collection(&collection, test_spec_view["data"].get_array().value);
        }

        operation_runner op_runner{
            &database, &collection, nullptr /* session0 */, nullptr /* session1 */, &client};

        configure_fail_point(client, test.get_document().value);

        apm_checker.clear();
        for (auto&& element : test["operations"].get_array().value) {
            auto operation = element.get_document().value;

            INFO("Operation: " << bsoncxx::to_json(operation));
            optional<document::value> actual_outcome_value;
            try {
                actual_outcome_value = op_runner.run(operation);
            } catch (const operation_exception& e) {
                REQUIRE(operation["error"].get_bool().value);
            }

            if (operation["result"]) {
                // wrap the result, since it might not be a document.
                bsoncxx::document::view actual_outcome = actual_outcome_value->view();
                auto actual_result_wrapped =
                    make_document(kvp("result", actual_outcome["result"].get_value()));
                auto expected_result_wrapped =
                    make_document(kvp("result", operation["result"].get_value()));
                REQUIRE_BSON_MATCHES(actual_result_wrapped, expected_result_wrapped);
            }
        }

        if (test["failPoint"]) {
            disable_fail_point(client, test["failPoint"]["configureFailPoint"].get_string().value);
        }
    }
}

TEST_CASE("retryable reads spec tests", "[retryable_reads_spec]") {
    instance::current();

    std::set<std::string> unsupported_tests{"gridfs-downloadByName.json",
                                            "gridfs-downloadByName-serverErrors.json",
                                            "listCollectionObjects.json",
                                            "listCollectionObjects-serverErrors.json",
                                            "listDatabaseObjects.json",
                                            "listDatabaseObjects-serverErrors.json",
                                            "listIndexNames.json",
                                            "listIndexNames-serverErrors.json",
                                            "mapReduce.json",
                                            "count.json",
                                            "count-serverErrors.json"};

    run_tests_in_suite(
        "RETRYABLE_READS_LEGACY_TESTS_PATH", run_retryable_reads_tests_in_file, unsupported_tests);
}
}  // namespace
