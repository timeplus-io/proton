// Copyright 2017 MongoDB Inc.
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

#include <mongocxx/private/conversions.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace libmongoc {
namespace conversions {

mongoc_read_mode_t read_mode_t_from_read_mode(read_preference::read_mode read_mode) {
    switch (read_mode) {
        case read_preference::read_mode::k_primary:
            return MONGOC_READ_PRIMARY;
        case read_preference::read_mode::k_primary_preferred:
            return MONGOC_READ_PRIMARY_PREFERRED;
        case read_preference::read_mode::k_secondary:
            return MONGOC_READ_SECONDARY;
        case read_preference::read_mode::k_secondary_preferred:
            return MONGOC_READ_SECONDARY_PREFERRED;
        case read_preference::read_mode::k_nearest:
            return MONGOC_READ_NEAREST;
    }

    MONGOCXX_UNREACHABLE;
}

read_preference::read_mode read_mode_from_read_mode_t(mongoc_read_mode_t read_mode) {
    switch (read_mode) {
        case MONGOC_READ_PRIMARY:
            return read_preference::read_mode::k_primary;
        case MONGOC_READ_PRIMARY_PREFERRED:
            return read_preference::read_mode::k_primary_preferred;
        case MONGOC_READ_SECONDARY:
            return read_preference::read_mode::k_secondary;
        case MONGOC_READ_SECONDARY_PREFERRED:
            return read_preference::read_mode::k_secondary_preferred;
        case MONGOC_READ_NEAREST:
            return read_preference::read_mode::k_nearest;
    }

    MONGOCXX_UNREACHABLE;
}

}  // namespace conversions
}  // namespace libmongoc
}  // namespace mongocxx
