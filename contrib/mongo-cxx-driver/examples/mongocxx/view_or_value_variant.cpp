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

#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/stdx/make_unique.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>

using bsoncxx::builder::basic::array;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

int main(int, char**) {
    // The mongocxx::instance constructor and destructor initialize and shut down the driver,
    // respectively. Therefore, a mongocxx::instance must be created before using the driver and
    // must remain alive for as long as the driver is in use.
    mongocxx::instance inst{};
    mongocxx::client conn{mongocxx::uri{}};
    auto db = conn["test"];

    // Many driver methods take a bsoncxx::document::view_or_value as
    // a parameter. You can use these methods with either a document::view
    // or a document::value.

    // Document::views can be passed in directly:
    {
        bsoncxx::document::value command = make_document(kvp("ping", 1));
        bsoncxx::document::view command_view = command.view();
        auto res = db.run_command(command_view);
    }

    // Document::values can be passed in one of two ways:

    // 1. Pass a view of the document::value
    {
        bsoncxx::document::value command = make_document(kvp("ping", 1));
        auto res = db.run_command(command.view());
    }

    // 2. Pass ownership of the document::value into the method
    {
        bsoncxx::document::value command = make_document(kvp("ping", 1));
        auto res = db.run_command(std::move(command));
    }

    // Temporary document::values are captured and owned by the view_or_value type
    {
        auto res = db.run_command(make_document(kvp("ping", 1)));

        // NOTE: there is no need to call .view() on a temporary document::value in
        // a call like this, and doing so could result in a use-after-free error.
        // BAD:
        // auto res = db.run_command(make_document(kvp("ping", 1)).view());
    }
}
