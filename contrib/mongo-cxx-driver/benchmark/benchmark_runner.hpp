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

#include <chrono>

#include "microbench.hpp"
#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/instance.hpp>

namespace benchmark {

class benchmark_runner {
   public:
    benchmark_runner(std::set<benchmark_type> types = {});

    void run_microbenches();

    void write_scores();

    double calculate_bson_bench_score();

    double calculate_single_bench_score();

    double calculate_multi_bench_score();

    double calculate_parallel_bench_score();

    double calculate_read_bench_score();

    double calculate_write_bench_score();

    double calculate_driver_bench_score();

   private:
    double calculate_average(benchmark_type);

    std::chrono::time_point<std::chrono::system_clock> _start_time;
    std::chrono::time_point<std::chrono::system_clock> _end_time;
    std::vector<std::unique_ptr<microbench>> _microbenches;
    std::set<benchmark_type> _types;
};
}  // namespace benchmark