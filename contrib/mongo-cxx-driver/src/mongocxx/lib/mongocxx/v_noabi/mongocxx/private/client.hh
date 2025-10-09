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

#include <list>

#include <mongocxx/client.hpp>
#include <mongocxx/private/libmongoc.hh>
#include <mongocxx/private/write_concern.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

class client::impl {
   public:
    impl(mongoc_client_t* client) : client_t(client) {}

    ~impl() {
        libmongoc::client_destroy(client_t);
    }

    mongoc_client_t* client_t;
    std::list<bsoncxx::v_noabi::string::view_or_value> tls_options;
    options::apm listeners;
};

}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
