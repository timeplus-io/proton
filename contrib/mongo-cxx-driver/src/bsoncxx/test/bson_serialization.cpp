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

#include <string>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <bsoncxx/test/catch.hh>
#include <bsoncxx/types/bson_value/view.hpp>

namespace {
using namespace bsoncxx;

using builder::basic::kvp;
using builder::basic::make_document;
using string::to_string;

namespace test {
struct Person {
    std::string first_name;
    std::string last_name;
    int age;

    bool operator==(const Person& rhs) const {
        return (first_name == rhs.first_name) && (last_name == rhs.last_name) && (age == rhs.age);
    }
    bool operator!=(const Person& rhs) const {
        return !(operator==(rhs));
    }
};

void to_bson(const Person& person, bsoncxx::document::value& bson_object) {
    bson_object = make_document(kvp("first_name", person.first_name),
                                kvp("last_name", person.last_name),
                                kvp("age", person.age));
}

void from_bson(Person& person, const bsoncxx::document::view& bson_object) {
    person.first_name = to_string(bson_object["first_name"].get_string().value);
    person.last_name = to_string(bson_object["last_name"].get_string().value);
    person.age = bson_object["age"].get_int32().value;
}
}  // namespace test

TEST_CASE("Convert between Person struct and BSON object") {
    test::Person expected_person{
        "Lelouch",
        "Lamperouge",
        18,
    };

    bsoncxx::document::value expected_doc =
        make_document(kvp("first_name", expected_person.first_name),
                      kvp("last_name", expected_person.last_name),
                      kvp("age", expected_person.age));

    SECTION("Conversion from Person struct to document::value works") {
        bsoncxx::document::value test_value{expected_person};
        REQUIRE(test_value.view() == expected_doc.view());
    }

    SECTION("Conversion from BSON object to Person struct works") {
        test::Person test_person = expected_doc.get<test::Person>();
        REQUIRE(test_person == expected_person);
    }

    SECTION("Conversion from BSON object to Person using partially constructed object") {
        test::Person other_person{"Test", "Person", 99};
        document::value other_doc = make_document(kvp("first_name", other_person.first_name),
                                                  kvp("last_name", other_person.last_name),
                                                  kvp("age", other_person.age));

        // Default-constructed person
        test::Person test_person;
        expected_doc.get(test_person);
        REQUIRE(test_person == expected_person);

        other_doc.get(test_person);
        REQUIRE(test_person == other_person);
    }
}
}  // namespace
