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

#include <iostream>

#include "../microbench.hpp"
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>

namespace benchmark {

class bson_encoding : public microbench {
   public:
    bson_encoding() = delete;

    bson_encoding(std::string name, double task_size, std::string json_file)
        : microbench{std::move(name),
                     task_size,
                     std::set<benchmark_type>{benchmark_type::bson_bench}},
          _json_file{std::move(json_file)} {}

   protected:
    void setup();
    void task();
    void teardown();

   private:
    std::string _json_file;
    bsoncxx::stdx::optional<bsoncxx::document::value> _doc;
};

void bson_encoding::setup() {
    _doc = parse_json_file_to_documents(_json_file)[0];
}

void visit_document(bsoncxx::document::view doc) {
    for (auto&& it : doc) {
        if (it.type() == bsoncxx::type::k_document) {
            auto sub_doc = it.get_document();
            visit_document(sub_doc.view());
        }
    }
}

// Mirroring mongo-c-driver's interpretation of the spec.
void bson_encoding::task() {
    for (std::uint32_t i = 0; i < iterations; i++) {
        visit_document(_doc->view());
    }
}

void bson_encoding::teardown() {}
}  // namespace benchmark
