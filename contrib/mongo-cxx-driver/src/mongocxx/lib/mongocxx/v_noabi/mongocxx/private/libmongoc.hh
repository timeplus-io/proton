// Copyright 2014 MongoDB Inc.
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

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wconversion"
#elif defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#elif (_MSC_VER)
// TODO: CXX-1366 Disable MSVC warnings for libmongoc
#endif

#include <mongoc/mongoc.h>

#if defined(__clang__)
#pragma clang diagnostic pop
#elif defined(__GNUC__)
#pragma GCC diagnostic pop
#elif (_MSC_VER)
// TODO: CXX-1366 Disable MSVC warnings for libmongoc
#endif

#include <mongocxx/test_util/export_for_testing.hh>
#include <mongocxx/test_util/mock.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace libmongoc {

#ifdef MONGOCXX_TESTING

#if defined(__GNUC__) && (__GNUC__ >= 6) && !defined(__clang__)
// See https://jira.mongodb.com/browse/CXX-1453 and
// https://gcc.gnu.org/bugzilla/show_bug.cgi?id=81605 The basic issue
// is that GCC sees the visibility attributes on the mongoc functions,
// and considers them part of the type, and then emits a silly
// diagnostic stating that the attribute was ignored.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wignored-attributes"
#endif

#define MONGOCXX_LIBMONGOC_SYMBOL(name) \
    extern MONGOCXX_TEST_API mongocxx::test_util::mock<decltype(&mongoc_##name)>& name;
#include "libmongoc_symbols.hh"
#undef MONGOCXX_LIBMONGOC_SYMBOL

#if defined(__GNUC__) && (__GNUC__ >= 6) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif

#else

#define MONGOCXX_LIBMONGOC_SYMBOL(name) constexpr auto name = mongoc_##name;
#include "libmongoc_symbols.hh"
#undef MONGOCXX_LIBMONGOC_SYMBOL

#endif

}  // namespace libmongoc
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
