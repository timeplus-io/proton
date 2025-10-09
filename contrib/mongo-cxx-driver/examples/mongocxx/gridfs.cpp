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

#include <algorithm>
#include <iostream>
#include <ostream>

#include <bsoncxx/json.hpp>
#include <bsoncxx/stdx/make_unique.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/gridfs/bucket.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>

using namespace mongocxx;

using bsoncxx::stdx::make_unique;

int main() {
    // The mongocxx::instance constructor and destructor initialize and shut down the driver,
    // respectively. Therefore, a mongocxx::instance must be created before using the driver and
    // must remain alive for as long as the driver is in use.
    mongocxx::instance inst{};
    mongocxx::client conn{mongocxx::uri{}};
    auto db = conn["test"];
    auto bucket = db.gridfs_bucket();

    // "sample_gridfs_file" is the name of the GridFS file stored on the server. GridFS filenames
    // are not unique.
    auto uploader = bucket.open_upload_stream("sample_gridfs_file");

    // ASCII for "HelloWorld"
    std::uint8_t bytes[10] = {72, 101, 108, 108, 111, 87, 111, 114, 108, 100};

    // Write 50 bytes to the file.
    for (auto i = 0; i < 5; ++i) {
        uploader.write(bytes, 10);
    }

    auto result = uploader.close();

    // This is the unique id of the uploaded file, which is needed to download the file. Note that
    // the return type, bsoncxx::types::bson_value::view, is a view type and is only valid as long
    // as `result`
    // is still in scope.
    bsoncxx::types::bson_value::view id = result.id();

    auto downloader = bucket.open_download_stream(id);
    auto file_length = downloader.file_length();
    auto bytes_counter = 0;

    auto buffer_size = std::min(file_length, static_cast<std::int64_t>(downloader.chunk_size()));
    auto buffer = make_unique<std::uint8_t[]>(static_cast<std::size_t>(buffer_size));

    while (auto length_read =
               downloader.read(buffer.get(), static_cast<std::size_t>(buffer_size))) {
        bytes_counter += static_cast<std::int32_t>(length_read);

        // Do something with the contents of buffer.
    }

    std::cout << "total bytes in file: " << bytes_counter << std::endl;
}
