// Copyright 2022-present MongoDB Inc.
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

#include <limits>

#include <mongocxx/private/numeric_casting.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {

bool size_t_to_int64_safe(const std::size_t in, int64_t& out) {
    if (sizeof(in) >= sizeof(int64_t)) {
        if (in > static_cast<std::size_t>(std::numeric_limits<int64_t>::max())) {
            return false;
        }
    }
    out = static_cast<int64_t>(in);
    return true;
}

bool int64_to_int32_safe(const int64_t in, int32_t& out) {
    if (in > static_cast<int64_t>(std::numeric_limits<int32_t>::max())) {
        return false;
    }
    if (in < static_cast<int64_t>(std::numeric_limits<int32_t>::min())) {
        return false;
    }
    out = static_cast<int32_t>(in);
    return true;
}

bool int32_to_size_t_safe(const int32_t in, std::size_t& out) {
    if (in < 0) {
        return false;
    }
    if (sizeof(in) > sizeof(std::size_t)) {
        if (in > static_cast<std::int32_t>(std::numeric_limits<std::size_t>::max())) {
            return false;
        }
    }
    out = static_cast<std::size_t>(in);
    return true;
}

bool int64_to_size_t_safe(const int64_t in, std::size_t& out) {
    if (in < 0) {
        return false;
    }
    if (sizeof(in) > sizeof(std::size_t)) {
        if (in > static_cast<std::int64_t>(std::numeric_limits<std::size_t>::max())) {
            return false;
        }
    }
    out = static_cast<std::size_t>(in);
    return true;
}

}  // namespace mongocxx
