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

#include <algorithm>
#include <iostream>
#include <iterator>
#include <vector>

#include <bsoncxx/array/view.hpp>
#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/stdx/string_view.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <bsoncxx/types.hpp>
#include <bsoncxx/types/bson_value/view.hpp>

using namespace bsoncxx;

int main(int, char**) {
    // This example will cover the read-only BSON interface.

    // Lets first build up a non-trivial BSON document using the builder interface.
    using builder::basic::kvp;
    using builder::basic::sub_array;

    auto doc = builder::basic::document{};
    doc.append(
        kvp("team", "platforms"), kvp("id", types::b_oid{oid()}), kvp("members", [](sub_array sa) {
            sa.append("tyler", "jason", "drew", "sam", "ernie", "john", "mark", "crystal");
        }));

    // document::value is an owning bson document conceptually similar to string.
    document::value value{doc.extract()};

    // document::view is a non-owning bson document conceptually similar to string_view.
    document::view view{value.view()};

    // Note: array::view and array::value are the corresponding classes for arrays.

    // we can print a view using to_json
    std::cout << to_json(view) << std::endl;

    // note that all of the interesting methods for reading BSON are defined on the view type.

    // iterate over the elements in a bson document
    for (document::element ele : view) {
        // element is non owning view of a key-value pair within a document.

        // we can use the key() method to get a string_view of the key.
        stdx::string_view field_key{ele.key()};

        std::cout << "Got key, key = " << field_key << std::endl;

        // we can use type() to get the type of the value.
        switch (ele.type()) {
            case type::k_string:
                std::cout << "Got String!" << std::endl;
                break;
            case type::k_oid:
                std::cout << "Got ObjectId!" << std::endl;
                break;
            case type::k_array: {
                std::cout << "Got Array!" << std::endl;
                // if we have a subarray, we can access it by getting a view of it.
                array::view subarr{ele.get_array().value};
                for (array::element ele : subarr) {
                    std::cout << "array element: "
                              << bsoncxx::string::to_string(ele.get_string().value) << std::endl;
                }
                break;
            }
            default:
                std::cout << "We messed up!" << std::endl;
        }

        // usually we don't need to actually use a switch statement, because we can also
        // get a variant 'value' that can hold any BSON type.
        types::bson_value::view ele_val{ele.get_value()};
    }

    // If we want to search for an element we can use operator[]
    // (we also provide a find() method that returns an iterator)
    // Note, this does a linear search so it is O(n) in the length of the BSON document.
    document::element ele{view["team"]};
    if (ele) {
        // this block will execute if ele was actually found
        std::cout << "as expected, we have a team" << std::endl;
    }

    // Because view implements begin(), end(), we can also use standard STL algorithms.

    // i.e. if we want to find the number of keys in a document we can use std::distance
    using std::begin;
    using std::end;

    auto num_keys = std::distance(begin(view), end(view));
    std::cout << "document has " << num_keys << " keys." << std::endl;

    // i.e. if we want a vector of all the keys in a document, we can use std::transform
    std::vector<std::string> doc_keys;
    std::transform(begin(view), end(view), std::back_inserter(doc_keys), [](document::element ele) {
        // note that key() returns a string_view
        return bsoncxx::string::to_string(ele.key());
    });

    std::cout << "document keys are: " << std::endl;
    for (auto key : doc_keys) {
        std::cout << key << " " << std::endl;
    }
    std::cout << std::endl;
}
