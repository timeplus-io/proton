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

#include <cctype>
#include <fstream>

#include <bsoncxx/test/catch.hh>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>

namespace {

struct URIOptionsTest {
    bsoncxx::stdx::string_view description;
    bsoncxx::stdx::string_view uri;
    bool valid;
    bool warning;
    bsoncxx::document::view options;

    static URIOptionsTest parse(bsoncxx::document::view test_doc) {
        URIOptionsTest test;

        for (auto el : test_doc) {
            if (0 == el.key().compare("description")) {
                test.description = el.get_string().value;
            } else if (0 == el.key().compare("uri")) {
                test.uri = el.get_string().value;
            } else if (0 == el.key().compare("valid")) {
                test.valid = el.get_bool().value;
            } else if (0 == el.key().compare("warning")) {
                test.warning = el.get_bool().value;
            } else if (0 == el.key().compare("options")) {
                test.options = el.get_document().value;
            } else if ((0 == el.key().compare("hosts")) || ((0 == el.key().compare("auth")))) {
                // hosts and auth will always be null are to be skipped as per the spec test
                // description
            } else {
                FAIL("URIOptionsTest does not understand the field: '" << el.key()
                                                                       << "'. Please add support.");
            }
        }
        return test;
    }
};

static bsoncxx::document::value _doc_from_file(mongocxx::stdx::string_view sub_path) {
    const char* test_path = std::getenv("URI_OPTIONS_TESTS_PATH");
    REQUIRE(test_path);

    std::string path = std::string(test_path) + sub_path.data();
    CAPTURE(path);

    std::ifstream file{path};
    REQUIRE(file);

    std::string file_contents((std::istreambuf_iterator<char>(file)),
                              std::istreambuf_iterator<char>());

    return bsoncxx::from_json(file_contents);
}

static void assert_elements_equal(bsoncxx::document::element expected_option,
                                  bsoncxx::document::element my_option) {
    REQUIRE(expected_option.type() == my_option.type());
    switch (expected_option.type()) {
        case bsoncxx::type::k_int32:
            REQUIRE(expected_option.get_int32() == my_option.get_int32());
            break;
        case bsoncxx::type::k_bool:
            REQUIRE(expected_option.get_bool() == my_option.get_bool());
            break;
        case bsoncxx::type::k_string:
            REQUIRE(expected_option.get_string() == my_option.get_string());
            break;
        default:
            std::string msg =
                "option type not handled: " + bsoncxx::to_string(expected_option.type());
            throw std::logic_error(msg);
    }
}

TEST_CASE("uri_options::test_srv_options", "[uri_options]") {
    mongocxx::instance::current();
    auto test_doc = _doc_from_file("/srv-options.json");

    auto tests = test_doc["tests"].get_array().value;

    for (const auto& it : tests) {
        auto doc = it.get_document().value;
        auto test = URIOptionsTest::parse(doc);
        SECTION(std::string(test.description)) {
            try {
                mongocxx::uri my_uri{test.uri};
                REQUIRE(test.valid);

                auto my_options = my_uri.options();
                for (const auto& expected_option : test.options) {
                    auto key = std::string(expected_option.key());
                    std::transform(
                        key.begin(), key.end(), key.begin(), [](int c) { return std::tolower(c); });
                    assert_elements_equal(expected_option, my_options[key]);
                }
            } catch (mongocxx::logic_error& e) {
                bool should_throw = !test.valid || test.warning;
                REQUIRE(should_throw);
            }
        }
    }
}

}  // namespace
