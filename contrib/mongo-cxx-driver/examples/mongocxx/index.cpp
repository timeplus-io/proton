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

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/stdx/make_unique.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

int main(int, char**) {
    // The mongocxx::instance constructor and destructor initialize and shut down the driver,
    // respectively. Therefore, a mongocxx::instance must be created before using the driver and
    // must remain alive for as long as the driver is in use.
    mongocxx::instance inst{};
    mongocxx::client conn{mongocxx::uri{}};

    auto db = conn["test"];
    try {
        db["restaurants"].drop();
    } catch (const std::exception&) {
        // Collection did not exist.
    }

    // Create a single field index.
    { db["restaurants"].create_index(make_document(kvp("cuisine", 1)), {}); }

    // Create a compound index.
    {
        db["restaurants"].drop();
        db["restaurants"].create_index(make_document(kvp("cuisine", 1), kvp("address.zipcode", -1)),
                                       {});
    }

    // Create a unique index.
    {
        db["restaurants"].drop();
        mongocxx::options::index index_options{};
        index_options.unique(true);
        db["restaurants"].create_index(make_document(kvp("website", 1)), index_options);
    }

    // Create an index with storage engine options
    {
        db["restaurants"].drop();
        mongocxx::options::index index_options{};
        std::unique_ptr<mongocxx::options::index::wiredtiger_storage_options> wt_options =
            bsoncxx::stdx::make_unique<mongocxx::options::index::wiredtiger_storage_options>();
        wt_options->config_string("block_allocation=first");
        index_options.storage_options(std::move(wt_options));
        db["restaurants"].create_index(make_document(kvp("cuisine", 1)), index_options);
    }
}
