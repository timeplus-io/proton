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
// limitations under the mLicense.

#include "microbench.hpp"

#include <fstream>
#include <iostream>

#include <bsoncxx/json.hpp>

namespace benchmark {

bool finished_running(const std::chrono::duration<std::uint32_t, std::milli>& curr_time,
                      std::uint32_t iter) {
    return (curr_time > maxtime || (curr_time > mintime && iter > max_iter));
}

void microbench::run() {
    setup();

    std::uint32_t iteration = 0;
    for (iteration = 0; !finished_running(_score.get_execution_time(), iteration); iteration++) {
        before_task();

        _score.start_sample();
        task();
        _score.end_sample();

        after_task();
    }
    teardown();
}

std::vector<std::string> parse_json_file_to_strings(const std::string& json_file) {
    std::vector<std::string> jsons;
    std::ifstream stream{"data/benchmark/" + json_file};

    if (!stream) {
        throw std::runtime_error("Failed to open " + json_file);
    }

    while (stream.is_open() && !stream.eof()) {
        std::string s;
        std::getline(stream, s);
        jsons.push_back(s);
    }
    return jsons;
}

std::vector<bsoncxx::document::value> parse_json_file_to_documents(const std::string& json_file) {
    std::vector<bsoncxx::document::value> docs;
    std::ifstream stream{"data/benchmark/" + json_file};

    if (!stream) {
        throw std::runtime_error("Failed to open " + json_file);
    }

    while (stream.is_open() && !stream.eof()) {
        std::string s;
        std::getline(stream, s);

        if (s.length() > 0) {
            docs.push_back(bsoncxx::from_json(bsoncxx::stdx::string_view{s}));
        }
    }
    return docs;
}

std::vector<std::string> parse_documents_to_bson(
    const std::vector<bsoncxx::document::value>& docs) {
    std::vector<std::string> bsons;
    for (std::uint32_t i = 0; i < docs.size(); i++) {
        bsons.push_back(bsoncxx::to_json(docs[i]));
    }
    return bsons;
}
}  // namespace benchmark
