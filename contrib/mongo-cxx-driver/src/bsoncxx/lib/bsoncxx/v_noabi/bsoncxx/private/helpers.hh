// Copyright 2015-present MongoDB Inc.
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

#include <bsoncxx/document/value.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/oid.hpp>
#include <bsoncxx/private/libbson.hh>

#include <bsoncxx/config/private/prelude.hh>

namespace bsoncxx {
namespace helpers {

inline bsoncxx::v_noabi::document::view view_from_bson_t(const bson_t* bson) {
    return {::bson_get_data(bson), bson->len};
}

inline bsoncxx::v_noabi::document::value value_from_bson_t(const bson_t* bson) {
    return bsoncxx::v_noabi::document::value{view_from_bson_t(bson)};
}

/*
Construct an oid from a bson_oid_t (which is a C API type that we don't want to
expose to the world).

Note: passing a nullptr is unguarded
Note: Deduction guides aren't yet available to us, so a factory it is! This is
something that can be improved as part of CXX-2350 (migration to more recent C++
standards).
*/
inline bsoncxx::v_noabi::oid make_oid(const bson_oid_t* bson_oid) {
    return bsoncxx::v_noabi::oid(reinterpret_cast<const char*>(bson_oid),
                                 bsoncxx::v_noabi::oid::size());
}

}  // namespace helpers
}  // namespace bsoncxx

#include <bsoncxx/config/private/postlude.hh>
