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
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/private/suppress_deprecation_warnings.hh>
#include <bsoncxx/string/to_string.hpp>
#include <bsoncxx/test/catch.hh>
#include <bsoncxx/types.hpp>
#include <bsoncxx/types/bson_value/view.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/options/create_collection.hpp>
#include <third_party/catch/include/helpers.hpp>

namespace {
using namespace bsoncxx;
using namespace mongocxx;

using builder::basic::kvp;
using builder::basic::make_document;

TEST_CASE("create_collection accessors/mutators", "[create_collection]") {
    instance::current();

    options::create_collection_deprecated cc;

    auto collation = make_document(kvp("locale", "en_US"));
    auto storage_engine = make_document(
        kvp("wiredTiger", make_document(kvp("configString", "block_compressor=zlib"))));
    auto validation = validation_criteria{}.rule(make_document(kvp("a", 1)));

    CHECK_OPTIONAL_ARGUMENT(cc, capped, true);
    BSONCXX_SUPPRESS_DEPRECATION_WARNINGS_BEGIN;
    BSONCXX_SUPPRESS_DEPRECATION_WARNINGS_END;
    CHECK_OPTIONAL_ARGUMENT(cc, size, 5);
    CHECK_OPTIONAL_ARGUMENT(cc, max, 2);
    CHECK_OPTIONAL_ARGUMENT(cc, collation, collation.view());
    CHECK_OPTIONAL_ARGUMENT(cc, storage_engine, storage_engine.view());
    CHECK_OPTIONAL_ARGUMENT(cc, no_padding, true);

    // We verify the accessors/mutators of 'validation_criteria' manually here, since the
    // validation_criteria class doesn't support equality (and therefore can't be used with
    // CHECK_OPTIONAL_ARGUMENT()).
    SECTION("has validation_criteria disengaged") {
        REQUIRE(!cc.validation_criteria());
    }
    SECTION("has a method to set the validation_criteria") {
        cc.validation_criteria(validation);
        REQUIRE(*cc.validation_criteria()->rule() == *validation.rule());
        REQUIRE(!cc.validation_criteria()->level());
        REQUIRE(!cc.validation_criteria()->action());
    }
}

TEST_CASE("create_collection can be exported to a document", "[create_collection]") {
    instance::current();

    options::create_collection_deprecated cc;

    auto collation_en_US = make_document(kvp("locale", "en_US"));
    auto rule = make_document(kvp("brain", make_document(kvp("$exists", true))));

    validation_criteria validation;
    validation.rule(rule.view());
    validation.level(validation_criteria::validation_level::k_strict);

    cc.validation_criteria(validation);
    cc.capped(true);
    cc.size(256);
    cc.max(100);
    cc.collation(collation_en_US.view());
    cc.no_padding(true);

    auto doc = cc.to_document_deprecated();
    document::view doc_view{doc.view()};

    // capped field is set to true
    document::element capped{doc_view["capped"]};
    REQUIRE(capped);
    REQUIRE(capped.type() == type::k_bool);
    REQUIRE(capped.get_bool() == true);

    // autoIndexId should not be set
    document::element autoIndex{doc_view["autoIndexId"]};
    REQUIRE(!autoIndex);

    // size should be set
    document::element size{doc_view["size"]};
    REQUIRE(size);
    REQUIRE(size.type() == type::k_int64);
    REQUIRE(size.get_int64() == 256);

    // max should be set
    document::element max{doc_view["max"]};
    REQUIRE(max);
    REQUIRE(max.type() == type::k_int64);
    REQUIRE(max.get_int64() == 100);

    // collation should be set
    document::element collation{doc_view["collation"]};
    REQUIRE(collation);
    REQUIRE(collation.type() == type::k_document);
    REQUIRE(collation.get_document().value == collation_en_US);

    // flags should be set to 0x10
    document::element padding{doc_view["flags"]};
    REQUIRE(padding);
    REQUIRE(padding.type() == type::k_int32);
    REQUIRE(padding.get_int32() == 0x10);

    // storageEngine should not be set
    document::element engine{doc_view["storageEngine"]};
    REQUIRE(!engine);

    // validator and validationLevel should be set, but not validationAction
    document::element validator{doc_view["validator"]};
    REQUIRE(validator);
    REQUIRE(validator.type() == type::k_document);
    REQUIRE(validator.get_document().value == rule);

    document::element validationLevel{doc_view["validationLevel"]};
    REQUIRE(validationLevel);
    REQUIRE(validationLevel.type() == type::k_string);
    REQUIRE(bsoncxx::string::to_string(validationLevel.get_string().value) == "strict");

    document::element validationAction{doc_view["validationAction"]};
    REQUIRE(!validationAction);
}
}  // namespace
