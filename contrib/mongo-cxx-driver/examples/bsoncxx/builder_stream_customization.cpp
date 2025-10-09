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

#include <map>
#include <vector>

#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>

using namespace bsoncxx;

// concatenates arbitrary ranges into an array context
template <typename begin_t, typename end_t>
class range_array_appender {
   public:
    range_array_appender(begin_t begin, end_t end)
        : _begin(std::move(begin)), _end(std::move(end)) {}

    void operator()(bsoncxx::builder::stream::array_context<> ac) const {
        for (auto iter = _begin; iter != _end; ++iter) {
            ac = ac << *iter;
        }
    }

   private:
    begin_t _begin;
    end_t _end;
};

template <typename begin_t, typename end_t>
range_array_appender<begin_t, end_t> make_range_array_appender(begin_t&& begin, end_t&& end) {
    return range_array_appender<begin_t, end_t>(std::forward<begin_t>(begin),
                                                std::forward<end_t>(end));
}

// concatenates arbitrary ranges into an key context
template <typename begin_t, typename end_t>
class range_kvp_appender {
   public:
    range_kvp_appender(begin_t begin, end_t end) : _begin(std::move(begin)), _end(std::move(end)) {}

    void operator()(bsoncxx::builder::stream::key_context<> ac) const {
        for (auto iter = _begin; iter != _end; ++iter) {
            ac = ac << iter->first << iter->second;
        }
    }

   private:
    begin_t _begin;
    end_t _end;
};

template <typename begin_t, typename end_t>
range_kvp_appender<begin_t, end_t> make_range_kvp_appender(begin_t&& begin, end_t&& end) {
    return range_kvp_appender<begin_t, end_t>(std::forward<begin_t>(begin),
                                              std::forward<end_t>(end));
}

int main(int, char**) {
    using builder::stream::array;
    using builder::stream::document;
    using builder::stream::finalize;

    // bsoncxx::builder::stream presents an iostream like interface for succinctly
    // constructing complex BSON objects.  It also allows for interesting
    // primitives by writing your own callables with interesting signatures.

    // Some key value pairs we'd like to append
    std::map<std::string, int> some_kvps = {{"a", 1}, {"b", 2}, {"c", 3}};

    // Adapt our kvps
    auto doc = document() << make_range_kvp_appender(some_kvps.begin(), some_kvps.end())
                          << finalize;
    // Now doc = {
    //     "a" : 1,
    //     "b" : 2,
    //     "c" : 3
    // }

    // Some values we'd like to append;
    std::vector<int> some_numbers = {1, 2, 3};

    // Adapt our values
    auto arr = array() << make_range_array_appender(some_numbers.begin(), some_numbers.end())
                       << finalize;
    // Now arr = {
    //     "0" : 1,
    //     "1" : 2,
    //     "2" : 3
    // }
}
