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

#include <chrono>

#include <bsoncxx/builder/list.hpp>
#include <bsoncxx/types.hpp>
#include <bsoncxx/types/bson_value/value.hpp>

using namespace bsoncxx;

int main(int, char**) {
    using namespace bsoncxx::builder;

    //
    // bsoncxx::builder::list, bsoncxx::builder::document, and bsoncxx::builder::array provides a
    // JSON-like interface for creating BSON objects.
    //

    // builder::document builds an empty BSON document
    builder::document doc = {};
    // builder::array builds an empty BSON array
    builder::array arr = {};

    //
    // We can append values to a document using an initializer list of key-value pairs.
    // { "hello" : "world" }
    //
    doc = {"hello", "world"};

    //
    //  Each document key must be a string type.
    //
    // {
    //   "c-style" : "with value",
    //   "basic string" : "with value"
    // }
    //
    doc = {"c-style", "with value", std::string("basic string"), "with value"};

    //
    // Each document value must be a bson_value::value or implicitly convertible to one.
    //
    // {
    //   "BSON boolean value" : false,
    //   "BSON 32-bit signed integer value" : -123,
    //   "BSON date value" : { "$date" : 123456789 },
    //   "BSON Decimal128 value" : { "$numberDecimal" : "1.844674407370955161800E-6155" },
    //   "BSON regex value with options" : { "$regex" : "any", "$options" : "imsx" }
    //  }
    //
    // clang-format off
    doc = {"BSON boolean value", false,
           "BSON 32-bit signed integer value", -123,
           "BSON date value", std::chrono::milliseconds(123456789),
           "BSON Decimal128 value", decimal128{100, 200},
           "BSON regex value with options", bson_value::value("regex", "imsx" /* opts */)};
    // clang-format on

    //
    // Nested documents can be added in-place.
    //
    // { "Answers" :
    //      { "Everything" :
    //          { "The Universe" : { "Life" : 42 } }
    //      }
    // }
    //
    doc = {"Answers", {"Everything", {"The Universe", {"Life", 42}}}};

    //
    // Each array element must be bson_value::value or implicitly convertible to one.
    //
    // { "0" : false,
    //   "1" : -123,
    //   "2" : { "$date" : 123456789 },
    //   "3" : { "$numberDecimal" : "1.844674407370955161800E-6155" },
    //   "4" : { "$regex" : "any", "$options" : "imsx" }
    // }
    //
    arr = {false,
           -123,
           std::chrono::milliseconds(123456789),
           decimal128{100, 200},
           bson_value::value("regex", "imsx" /* opts */)};

    //
    // The list builder will create a BSON document, if possible. Otherwise, it will create a BSON
    // array. A document is possible if:
    //      1. The initializer list's size is even; this implies a list of
    //         key-value pairs or an empty document if the size is zero.
    //      2. Each 'key' is a string type. In a list of key-value pairs, the 'key' is every other
    //         element starting at the 0th element.
    //
    // BSON document
    // { "pi" : 3.1415899999999998826, "e" : 2.7182800000000000296 }
    builder::list lst = {"pi", 3.14159, "e", 2.71828};
    // BSON array
    // { "0" : "Numbers", "1" : 3.1415899999999998826, "2" : 2.7182800000000000296 }
    lst = {"Numbers", 3.14159, 2.71828};

    //
    // BSON document
    //
    // { "this" : { "is" : 10 },
    //   "valid" : { "BSON" : "document" }
    //   "with" : [ 1, "nested", "array" ] }
    lst = {"this", {"is", 0xA}, "valid", {"BSON", "document"}, "with", {1, "nested", "array"}};

    //
    // BSON array
    //
    // { "0" : "this",
    //   "1" : { "will" : "be" },
    //   "2" : "an",
    //   "3" : "array",
    //   "4" : [ 1, 2, 3] }
    lst = {"this", {"will", "be"}, "an", "array", {1, 2, 3}};
}
