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

#include <mongocxx/client.hpp>
#include <mongocxx/database.hpp>
#include <mongocxx/private/client.hh>
#include <mongocxx/private/libmongoc.hh>
#include <mongocxx/private/write_concern.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

class database::impl {
   public:
    impl(mongoc_database_t* db, const mongocxx::v_noabi::client::impl* client, std::string name)
        : database_t(db), client_impl(client), name(std::move(name)) {}

    impl(const impl& i)
        : database_t{libmongoc::database_copy(i.database_t)},
          client_impl{i.client_impl},
          name{i.name} {}

    impl& operator=(const impl& i) {
        if (this != &i) {
            libmongoc::database_destroy(database_t);
            database_t = libmongoc::database_copy(i.database_t);
            client_impl = i.client_impl;
            name = i.name;
        }

        return *this;
    }

    ~impl() {
        libmongoc::database_destroy(database_t);
    }

    mongoc_database_t* database_t;
    const mongocxx::v_noabi::client::impl* client_impl;
    std::string name;
};

}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
