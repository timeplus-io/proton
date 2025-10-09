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

#include <algorithm>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "score_recorder.hpp"
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/stdx/string_view.hpp>

namespace benchmark {

enum class benchmark_type {
    bson_bench,
    single_bench,
    multi_bench,
    parallel_bench,
    read_bench,
    write_bench,
    run_command_bench,
};

static const std::unordered_map<benchmark_type, std::string> type_names = {
    {benchmark_type::bson_bench, "BSONBench"},
    {benchmark_type::single_bench, "SingleBench"},
    {benchmark_type::multi_bench, "MultiBench"},
    {benchmark_type::parallel_bench, "ParallelBench"},
    {benchmark_type::read_bench, "ReadBench"},
    {benchmark_type::write_bench, "WriteBench"},
    {benchmark_type::run_command_bench, "RunCommandBench"}};

static const std::unordered_map<std::string, benchmark_type> names_types = {
    {"BSONBench", benchmark_type::bson_bench},
    {"SingleBench", benchmark_type::single_bench},
    {"MultiBench", benchmark_type::multi_bench},
    {"ParallelBench", benchmark_type::parallel_bench},
    {"ReadBench", benchmark_type::read_bench},
    {"WriteBench", benchmark_type::write_bench},
    {"RunCommandBench", benchmark_type::run_command_bench}};

constexpr std::chrono::milliseconds mintime{60000};
constexpr std::chrono::milliseconds maxtime{300000};

constexpr std::int32_t max_iter = 100;
constexpr std::int32_t iterations = 10000;

class microbench {
   public:
    microbench() : _score{0} {}

    microbench(std::string&& name, double task_size, std::set<benchmark_type> tags = {})
        : _score{task_size}, _tags{tags}, _name{std::move(name)} {}

    virtual ~microbench() = default;

    void run();

    std::string get_name() {
        return _name;
    }

    benchmark::score_recorder& get_results() {
        return _score;
    }

    const std::set<benchmark_type>& get_tags() {
        return _tags;
    }

    bool has_tag(benchmark_type tag) {
        return _tags.find(tag) != _tags.end();
    }

   protected:
    virtual void setup() {}

    virtual void before_task() {}

    virtual void task() = 0;

    virtual void after_task() {}

    virtual void teardown() {}

    benchmark::score_recorder _score;
    std::set<benchmark_type> _tags;
    std::string _name;
};

std::vector<std::string> parse_json_file_to_strings(const std::string& json_file);

std::vector<bsoncxx::document::value> parse_json_file_to_documents(const std::string& json_file);

std::vector<std::string> parse_documents_to_bson(const std::vector<bsoncxx::document::value>& docs);
}  // namespace benchmark
