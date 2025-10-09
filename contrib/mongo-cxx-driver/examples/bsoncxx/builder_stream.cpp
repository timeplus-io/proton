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

#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include <bsoncxx/types.hpp>

using namespace bsoncxx;

int main(int, char**) {
    using builder::stream::array;
    using builder::stream::document;

    // bsoncxx::builder::stream presents an iostream like interface for succinctly
    // constructing complex BSON objects.

    // stream::document builds a BSON document
    auto doc = document{};
    // stream::array builds a BSON array
    auto arr = array{};

    // We append keys and values to documents using the '<<' operator;
    doc << "myKey"
        << "myValue";
    // We can chain any number of keys and values
    doc << "foo" << types::b_bool{false} << "baz" << types::b_int32{1234} << "quz"
        << types::b_double{1.234};

    // For arrays, everything just becomes a value
    arr << 1 << 2 << types::b_bool{true};

    // The stream namespace includes some helpers that can be used similarly
    // to the stream manipulators in <iomanip>
    // To build a subdocument, use open_document and close_document
    using builder::stream::close_document;
    using builder::stream::open_document;
    doc << "mySubDoc" << open_document << "subdoc key"
        << "subdoc value" << close_document;
    // To build a subarray, use open_array and close_array
    using builder::stream::close_array;
    using builder::stream::open_array;
    doc << "mySubArr" << open_array << 1 << types::b_bool{false} << "hello" << close_array;

    // There is a special finalize helper that converts a stream to its underlying bson value
    // this is useful for writing one-liners, for example
    using builder::stream::finalize;
    auto myQuery = document{} << "foo"
                              << "bar" << finalize;

    // There is a special concatenate helper to add all keys and corresponding values from one
    // document into another.
    using bsoncxx::builder::concatenate;
    doc << concatenate(myQuery.view());
    // `doc` now looks like:
    // {
    //   "myKey": "myValue",
    //   ...
    //   "mySubArr": [...],
    //   "foo": "bar"
    // }

    // To nest an existing bsoncxx::document::value into a builder stream, you can create a
    // types::b_document and append it. Alternatively you can open a new document and concatenate
    // the value in. Or, simplest, is just to stream in the view.
    bsoncxx::document::value nestedValue = document{} << "nested" << true << finalize;
    document topLevelDoc{};
    topLevelDoc << "subDoc1" << types::b_document{nestedValue.view()} << "subDoc2" << open_document
                << concatenate(nestedValue.view()) << close_document << "subDoc3" << nestedValue
                << finalize;

    // `topLevelDoc` now looks like:
    // {
    //     "subDoc1": {
    //         "nested": true
    //     },
    //     "subDoc2": {
    //         "nested": true
    //     },
    //     "subDoc3": {
    //         "nested": true
    //     }
    // }
}
