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

#include <iomanip>
#include <sstream>
#include <thread>
#include <vector>

#include <bsoncxx/string/to_string.hpp>
#include <mongocxx/gridfs/bucket.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>
#include <mongocxx/uri.hpp>

namespace benchmark {

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

class gridfs_multi_import : public microbench {
   public:
    static const std::uint32_t TOTAL_FILES{50};

    gridfs_multi_import() = delete;

    // The task size comes from the Driver Perfomance Benchmarking Reference Doc.
    gridfs_multi_import(std::string dir,
                        std::uint32_t thread_num = std::thread::hardware_concurrency() * 2)
        : microbench{"TestGridFsMultiImport",
                     262.144,
                     std::set<benchmark_type>{benchmark_type::parallel_bench,
                                              benchmark_type::write_bench}},
          _directory{std::move(dir)},
          _pool{mongocxx::uri{}},
          _thread_num{thread_num} {}

    void setup();

    void before_task();

    void teardown();

   protected:
    void task();

   private:
    void concurrency_task(std::uint32_t start_file, std::uint32_t num_files);

    std::string _directory;
    mongocxx::pool _pool;
    std::uint32_t _thread_num;
};

void gridfs_multi_import::setup() {
    auto conn = _pool.acquire();

    (*conn)["perftest"].drop();
}

void gridfs_multi_import::before_task() {
    auto conn = _pool.acquire();
    auto db = (*conn)["perftest"];
    auto bucket = db.gridfs_bucket();
    db[bsoncxx::string::to_string(bucket.bucket_name()) + ".chunks"].drop();
    db[bsoncxx::string::to_string(bucket.bucket_name()) + ".files"].drop();
    auto uploader = bucket.open_upload_stream("one_byte_gridfs_file");
    std::uint8_t byte[1] = {72};
    uploader.write(byte, 1);
    uploader.close();
}

void gridfs_multi_import::teardown() {
    auto conn = _pool.acquire();
    (*conn)["perftest"].drop();
}

void gridfs_multi_import::task() {
    std::div_t result =
        std::div(static_cast<std::int32_t>(TOTAL_FILES), static_cast<std::int32_t>(_thread_num));
    std::uint32_t num_each = static_cast<std::uint32_t>(result.quot);
    if (result.rem != 0) {
        num_each++;
    }

    std::vector<std::thread> threads;
    for (std::uint32_t i = 0; i < TOTAL_FILES; i += num_each) {
        threads.push_back(std::thread{
            [i, num_each, this] { concurrency_task(i, std::min(TOTAL_FILES - i, num_each)); }});
    }
    for (std::uint32_t i = 0; i < threads.size(); i++) {
        threads[i].join();
    }
}

void gridfs_multi_import::concurrency_task(std::uint32_t start_file, std::uint32_t num_files) {
    for (std::uint32_t i = start_file; i < start_file + num_files; i++) {
        std::stringstream ss;
        ss << _directory << "/file" << std::setfill('0') << std::setw(2) << i << ".txt";
        std::string file_name = ss.str();
        std::ifstream stream{file_name};

        auto client = _pool.acquire();
        auto bucket = (*client)["perftest"].gridfs_bucket();
        auto uploader = bucket.upload_from_stream(file_name, &stream);
    }
}
}  // namespace benchmark
