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

#pragma once

#include <mongocxx/private/libmongoc.hh>
#include <mongocxx/read_preference.hpp>
#include <mongocxx/test_util/export_for_testing.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace libmongoc {
namespace conversions {

MONGOCXX_TEST_API mongoc_read_mode_t
read_mode_t_from_read_mode(read_preference::read_mode read_mode);

MONGOCXX_TEST_API read_preference::read_mode read_mode_from_read_mode_t(
    mongoc_read_mode_t read_mode);

}  // namespace conversions
}  // namespace libmongoc
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
