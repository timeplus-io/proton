// Copyright 2015 MongoDB Inc.
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

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/document/element.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <bsoncxx/test/catch.hh>
#include <mongocxx/instance.hpp>
#include <mongocxx/validation_criteria.hpp>
#include <third_party/catch/include/helpers.hpp>

namespace {
using namespace mongocxx;
using namespace bsoncxx;

using builder::basic::kvp;
using builder::basic::make_document;

TEST_CASE("validation_criteria accessors/mutators", "[validation_criteria]") {
    instance::current();

    validation_criteria criteria;

    auto doc = make_document(kvp("email", make_document(kvp("$exists", true))));

    CHECK_OPTIONAL_ARGUMENT(criteria, rule, doc.view());
    CHECK_OPTIONAL_ARGUMENT(criteria, level, validation_criteria::validation_level::k_off);
    CHECK_OPTIONAL_ARGUMENT(criteria, action, validation_criteria::validation_action::k_warn);
}

TEST_CASE("validation_criteria equals", "[validation_criteria]") {
    instance::current();

    validation_criteria criteria1;
    validation_criteria criteria2;

    REQUIRE(criteria1 == criteria2);
}

TEST_CASE("validation_criteria inequals", "[validation_criteria]") {
    instance::current();

    validation_criteria criteria1;
    criteria1.level(validation_criteria::validation_level::k_strict);
    validation_criteria criteria2;

    REQUIRE(criteria1 != criteria2);
}

TEST_CASE("validation_criteria can be exported to a document", "[validation_criteria]") {
    instance::current();

    validation_criteria criteria;

    auto doc = make_document(kvp("email", make_document(kvp("$exists", true))));

    criteria.level(validation_criteria::validation_level::k_strict);
    criteria.action(validation_criteria::validation_action::k_warn);
    criteria.rule(doc.view());

    auto criteria_doc = criteria.to_document_deprecated();
    auto criteria_view = criteria_doc.view();

    document::element ele;

    ele = criteria_view["validationLevel"];
    REQUIRE(ele);
    REQUIRE(ele.type() == type::k_string);
    REQUIRE(bsoncxx::string::to_string(ele.get_string().value) == "strict");

    ele = criteria_view["validationAction"];
    REQUIRE(ele);
    REQUIRE(ele.type() == type::k_string);
    REQUIRE(bsoncxx::string::to_string(ele.get_string().value) == "warn");

    ele = criteria_view["validator"];
    REQUIRE(ele);
    REQUIRE(ele.type() == type::k_document);
    REQUIRE(ele.get_document().value == doc);
}
}  // namespace
