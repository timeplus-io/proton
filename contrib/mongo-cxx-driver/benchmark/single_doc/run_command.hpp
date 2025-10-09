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

#include "../microbench.hpp"

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>

namespace benchmark {

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

class run_command : public microbench {
   public:
    run_command()
        : microbench{"TestRunCommand",
                     0.13,
                     std::set<benchmark_type>{benchmark_type::run_command_bench}},
          _conn{mongocxx::uri{}} {
        _db = _conn["perftest"];
    }

   protected:
    void task();

   private:
    mongocxx::client _conn;
    mongocxx::database _db;
};
void run_command::task() {
    auto command = make_document(kvp("hello", true));
    for (std::int32_t i = 0; i < 10000; i++) {
        _db.run_command(command.view());
    }
}
}  // namespace benchmark
