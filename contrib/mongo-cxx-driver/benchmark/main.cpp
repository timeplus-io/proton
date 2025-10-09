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

static_assert(__cplusplus >= 201703L, "requires C++17 or higher");

#include <chrono>
#include <iostream>
#include <stdexcept>

#include "benchmark_runner.hpp"
#include <bsoncxx/stdx/string_view.hpp>
#include <mongocxx/instance.hpp>

using namespace benchmark;

int main(int argc, char* argv[]) {
    mongocxx::instance instance;
    std::set<benchmark_type> types;

    if (argc > 1) {
        if (bsoncxx::stdx::string_view(argv[1]) == "all") {
            for (const auto& [name, type] : names_types) {
                types.insert(type);
            }
        } else {
            for (int x = 1; x < argc; ++x) {
                std::string type{argv[x]};
                auto it = names_types.find(type);

                if (it == names_types.end()) {
                    throw std::runtime_error("Invalid benchmark: " + type);
                }
                types.insert(it->second);
            }
        }

        if (types.empty()) {
            throw std::runtime_error("No valid benchmarks specified");
        }
    }

    benchmark_runner runner{types};

    runner.run_microbenches();
    runner.write_scores();
}
