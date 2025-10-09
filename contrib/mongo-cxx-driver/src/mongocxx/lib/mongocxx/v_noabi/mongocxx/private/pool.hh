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

#pragma once

#include <list>
#include <utility>

#include <mongocxx/pool.hpp>
#include <mongocxx/private/libmongoc.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

class pool::impl {
   public:
    impl(mongoc_client_pool_t* pool) : client_pool_t(pool) {}

    ~impl() {
        libmongoc::client_pool_destroy(client_pool_t);
    }

    mongoc_client_pool_t* client_pool_t;
    std::list<bsoncxx::v_noabi::string::view_or_value> tls_options;
    options::apm listeners;
};

}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
