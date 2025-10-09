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
#include <iostream>

#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/decimal128.hpp>
#include <bsoncxx/exception/exception.hpp>
#include <bsoncxx/types.hpp>

using namespace bsoncxx;

int main(int, char**) {
    // Convert a string to BSON Decimal128.
    decimal128 d128;
    try {
        d128 = decimal128{"1.234E+3456"};
    } catch (const bsoncxx::exception& e) {
        // The example won't fail, but in general, arbitrary strings
        // might not convert properly.
        return EXIT_FAILURE;
    }

    // Add it to a BSON document.
    builder::stream::document build_doc;
    build_doc << "counter" << d128;
    auto doc = document::value{build_doc.extract()};

    // Extract a BSON Decimal128 from a document view.
    auto view = doc.view();
    auto d128copy = view["counter"].get_decimal128().value;

    // Convert it back to a string.
    std::cout << "Counter is " << d128copy.to_string() << std::endl;

    return EXIT_SUCCESS;
}
