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

#include <bsoncxx/string/to_string.hpp>
#include <bsoncxx/string/view_or_value.hpp>

#include <bsoncxx/config/private/prelude.hh>

namespace bsoncxx {
namespace v_noabi {
namespace string {

view_or_value view_or_value::terminated() const {
    // If we do not own our string, we cannot guarantee that it is null-terminated,
    // so make an owned copy.
    if (!is_owning()) {
        return {string::to_string(view())};
    }

    // If we are owning, return a view_or_value viewing our string
    return {view()};
}

const char* view_or_value::data() const {
    return view().data();
}

}  // namespace string
}  // namespace v_noabi
}  // namespace bsoncxx
