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

#pragma once

#include <cstddef>
#include <cstdint>

#include <mongocxx/test_util/export_for_testing.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {

// size_t_to_int64_safe checks if @in is within the bounds of an int64_t.
// If yes, it safely casts into @out and returns true.
// If no, @out is not modified and returns false.
MONGOCXX_TEST_API
bool size_t_to_int64_safe(const std::size_t in, int64_t& out);

// int64_to_int32_safe checks if @in is within the bounds of an int32_t.
// If yes, it safely casts into @out and returns true.
// If no, @out is not modified and returns false.
MONGOCXX_TEST_API
bool int64_to_int32_safe(const int64_t in, int32_t& out);

// int32_to_size_t_safe checks if @in is within the bounds of an size_t.
// If yes, it safely casts into @out and returns true.
// If no, @out is not modified and returns false.
MONGOCXX_TEST_API
bool int32_to_size_t_safe(const int32_t in, std::size_t& out);

// int64_to_size_t_safe checks if @in is within the bounds of an size_t.
// If yes, it safely casts into @out and returns true.
// If no, @out is not modified and returns false.
MONGOCXX_TEST_API
bool int64_to_size_t_safe(const int64_t in, std::size_t& out);

}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
