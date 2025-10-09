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
#include <mongocxx/instance.hpp>
#include <mongocxx/options/gridfs/bucket.hpp>
#include <mongocxx/read_concern.hpp>
#include <mongocxx/read_preference.hpp>
#include <mongocxx/write_concern.hpp>
#include <third_party/catch/include/helpers.hpp>

namespace {
using namespace mongocxx;

TEST_CASE("options::gridfs::bucket accessors/mutators", "[options::gridfs::bucket]") {
    instance::current();

    options::gridfs::bucket bucket_options;

    read_concern rc;
    rc.acknowledge_level(read_concern::level::k_majority);

    read_preference rp;
    rp.mode(read_preference::read_mode::k_nearest);

    write_concern wc;
    wc.acknowledge_level(write_concern::level::k_unacknowledged);

    CHECK_OPTIONAL_ARGUMENT(bucket_options, bucket_name, "foo");
    CHECK_OPTIONAL_ARGUMENT(bucket_options, chunk_size_bytes, 100);
    CHECK_OPTIONAL_ARGUMENT(bucket_options, read_concern, rc);
    CHECK_OPTIONAL_ARGUMENT(bucket_options, read_preference, rp);
    CHECK_OPTIONAL_ARGUMENT(bucket_options, write_concern, wc);
}
}  // namespace
