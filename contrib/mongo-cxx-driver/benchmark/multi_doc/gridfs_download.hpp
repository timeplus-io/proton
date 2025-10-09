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
#include <fstream>

#include "../microbench.hpp"
#include <bsoncxx/stdx/make_unique.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/gridfs/bucket.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>

namespace benchmark {

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;
using bsoncxx::stdx::make_unique;

class gridfs_download : public microbench {
   public:
    // The task size comes from the Driver Perfomance Benchmarking Reference Doc.
    gridfs_download(std::string file_name)
        : microbench{"TestGridFsDownload",
                     52.43,
                     std::set<benchmark_type>{benchmark_type::multi_bench,
                                              benchmark_type::read_bench}},
          _conn{mongocxx::uri{}},
          _file_name{std::move(file_name)} {}

    void setup();

    void teardown();

   protected:
    void task();

   private:
    mongocxx::client _conn;
    mongocxx::gridfs::bucket _bucket;
    bsoncxx::stdx::optional<bsoncxx::types::bson_value::view> _id;
    std::string _file_name;
};

void gridfs_download::setup() {
    mongocxx::database db = _conn["perftest"];
    db.drop();
    std::ifstream stream{_file_name};
    _bucket = db.gridfs_bucket();
    auto result = _bucket.upload_from_stream(_file_name, &stream);
    _id = result.id();
}

void gridfs_download::teardown() {
    _conn["perftest"].drop();
}

void gridfs_download::task() {
    auto downloader = _bucket.open_download_stream(_id.value());
    auto file_length = downloader.file_length();

    auto buffer_size = std::min(file_length, static_cast<std::int64_t>(downloader.chunk_size()));
    auto buffer = make_unique<std::uint8_t[]>(static_cast<std::size_t>(buffer_size));

    while ([[maybe_unused]] auto length_read =
               downloader.read(buffer.get(), static_cast<std::size_t>(buffer_size))) {
    }
}
}  // namespace benchmark
