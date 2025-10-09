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

#include <cstdlib>

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/types.hpp>

using namespace bsoncxx;

int main(int, char**) {
    // bsoncxx::builder::basic presents a BSON-construction interface familiar to users of the
    // server's
    // BSON library or the Java driver.

    // basic::document builds a BSON document.
    auto doc = builder::basic::document{};
    // basic::array builds a BSON array
    auto arr = builder::basic::array{};

    // We append key-value pairs to a document using the kvp helper.
    using bsoncxx::builder::basic::kvp;

    doc.append(
        kvp("foo", "bar"));  // string literal value will be converted to b_string automatically
    doc.append(kvp("baz", types::b_bool{false}));
    doc.append(kvp("garply", types::b_double{3.14159}));

    // We can also pass a variable number of keys to append.
    doc.append(kvp("a key", "a value"),
               kvp("another key", "another value"),
               kvp("moar keys", "moar values"));

    // Appending to arrays is simple, just append one or more bson values.
    arr.append("hello");
    arr.append(false, types::b_bool{true}, types::b_double{1.234});

    // If we want to create a subdocument, we can pass lambda as a value with a sub_document
    // argument.

    // When append is executed, the builder will start a subdocument, call the lambda with itself
    // as a parameter, which appends the keys in-place.
    // After the lambda returns, the builder will end the subdocument.

    using bsoncxx::builder::basic::sub_array;
    using bsoncxx::builder::basic::sub_document;

    doc.append(kvp("subdocument key",
                   [](sub_document subdoc) {
                       subdoc.append(kvp("subdoc key", "subdoc value"),
                                     kvp("another subdoc key", types::b_int64{1212}));
                   }),
               kvp("subarray key", [](sub_array subarr) {
                   // subarrays work similarly
                   subarr.append(1, types::b_bool{false}, "hello", 5, [](sub_document subdoc) {
                       // nesting works too!
                       subdoc.append(kvp("such", "nesting"), kvp("much", "recurse"));
                   });
               }));

    // We can get a view of the resulting bson by calling view()
    auto v = doc.view();

    // Use 'v' so we don't get compiler warnings.
    return v.empty() ? EXIT_FAILURE : EXIT_SUCCESS;
}
