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

#include <bsoncxx/test/catch.hh>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/gridfs/downloader.hpp>
#include <mongocxx/instance.hpp>

namespace {
using namespace mongocxx;

TEST_CASE("mongocxx::gridfs::downloader default constructor makes invalid downloader",
          "[gridfs::downloader]") {
    instance::current();

    gridfs::downloader downloader;
    REQUIRE(!downloader);
    std::uint8_t c;
    REQUIRE_THROWS_AS(downloader.read(&c, 1), logic_error);
}
}  // namespace
