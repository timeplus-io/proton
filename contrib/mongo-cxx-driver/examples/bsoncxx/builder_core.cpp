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

#include <bsoncxx/builder/core.hpp>
#include <bsoncxx/types.hpp>

using namespace bsoncxx;

int main(int, char**) {
    // bsoncxx::builder::core is a low-level primitive that can be useful for building other
    // BSON abstractions. Most users should just use builder::stream or builder::basic.

    // the boolean argument to core should be true if the top-level BSON datum is an array.
    // if false, it will be created as a document.
    auto builder = builder::core{false};

    // When building a document, we have two ways of passing keys.

    // 1. We can pass a key using key_owned(). This is less efficient but
    // frees us from worrying about the lifetime of the key.

    // Example: if we have a stack allocated std::string, key_owned will take ownership
    // of it, so the following code is legal.
    {
        std::string temp{"foo"};
        builder.key_owned(temp);
        builder.append(types::b_bool{false});
    }

    // 2. We can pass a key using key_view(). This is more efficient (less copying)
    // but we need to manage the lifetime of the key until a matching value is appended, at which
    // point it will be copied into an internally managed buffer.

    {
        // THIS IS ILLEGAL: DO NOT DO THIS
        auto illegal_do_not_do_this = builder::core{false};
        std::string temp{"bar"};
        illegal_do_not_do_this.key_view(temp);
        // the builder has dangling ref to temp at the end of this scope!!
    }

    {
        // This is legal, because we append a corresponding value before the key goes out of scope.
        std::string temp{"baz"};
        builder.key_view(temp);
        builder.append(types::b_bool{false});
    }

    // Appending values is pretty simple.
    // Just call append() and pass a value wrapped in the corresponding BSON type.
    auto array = builder::core{true};  // we are building an array

    array.append(types::b_string{"hello"});  // append a UTF-8 string
    array.append(types::b_double{1.234});    // append a double
    array.append(types::b_int32{1234});      // append an int32
    // ... etc.
}
