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

#include <mongocxx/private/libmongoc.hh>
#include <mongocxx/uri.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

class uri::impl {
   public:
    impl(mongoc_uri_t* uri) : uri_t(uri) {}
    ~impl() {
        libmongoc::uri_destroy(uri_t);
    }
    mongoc_uri_t* uri_t;
};

}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
