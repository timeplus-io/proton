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

#include <bsoncxx/private/itoa.hh>
#include <bsoncxx/test/catch.hh>

namespace {
TEST_CASE("util::itoa is equivalent to to_string(int)", "[bsoncxx::util::itoa]") {
// Cygwin doesn't have std::to_string, see:
// https://sourceware.org/ml/cygwin/2015-10/msg00446.html
#if !defined(__CYGWIN__)
    for (std::uint32_t i = 0; i <= 10000; i++) {
        bsoncxx::itoa val(i);
        std::string str = std::to_string(i);
        REQUIRE(val.length() == str.length());
        REQUIRE(std::string(val.c_str()) == str);
    }
#endif
}
}  // namespace
