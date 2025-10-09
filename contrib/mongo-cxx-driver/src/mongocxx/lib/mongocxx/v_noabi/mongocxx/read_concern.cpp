// Copyright 2015 MongoDB Inc.
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

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/stdx/make_unique.hpp>
#include <bsoncxx/string/to_string.hpp>
#include <mongocxx/exception/error_code.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/private/libmongoc.hh>
#include <mongocxx/private/read_concern.hh>
#include <mongocxx/read_concern.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

read_concern::read_concern() : _impl{stdx::make_unique<impl>(libmongoc::read_concern_new())} {}

read_concern::read_concern(std::unique_ptr<impl>&& implementation)
    : _impl{std::move(implementation)} {}

read_concern::read_concern(read_concern&&) noexcept = default;
read_concern& read_concern::operator=(read_concern&&) noexcept = default;

read_concern::read_concern(const read_concern& other)
    : _impl(stdx::make_unique<impl>(libmongoc::read_concern_copy(other._impl->read_concern_t))) {}

read_concern& read_concern::operator=(const read_concern& other) {
    _impl = stdx::make_unique<impl>(libmongoc::read_concern_copy(other._impl->read_concern_t));
    return *this;
}

read_concern::~read_concern() = default;

void read_concern::acknowledge_level(read_concern::level rc_level) {
    switch (rc_level) {
        case read_concern::level::k_local:
            libmongoc::read_concern_set_level(_impl->read_concern_t,
                                              MONGOC_READ_CONCERN_LEVEL_LOCAL);
            break;
        case read_concern::level::k_majority:
            libmongoc::read_concern_set_level(_impl->read_concern_t,
                                              MONGOC_READ_CONCERN_LEVEL_MAJORITY);
            break;
        case read_concern::level::k_linearizable:
            libmongoc::read_concern_set_level(_impl->read_concern_t,
                                              MONGOC_READ_CONCERN_LEVEL_LINEARIZABLE);
            break;
        case read_concern::level::k_server_default:
            // libmongoc uses a NULL level to mean "use the server's default read_concern."
            libmongoc::read_concern_set_level(_impl->read_concern_t, NULL);
            break;
        case read_concern::level::k_available:
            libmongoc::read_concern_set_level(_impl->read_concern_t,
                                              MONGOC_READ_CONCERN_LEVEL_AVAILABLE);
            break;
        case read_concern::level::k_snapshot:
            libmongoc::read_concern_set_level(_impl->read_concern_t,
                                              MONGOC_READ_CONCERN_LEVEL_SNAPSHOT);
            break;
        default:
            throw exception{error_code::k_unknown_read_concern};
    }
}

void read_concern::acknowledge_string(stdx::string_view rc_string) {
    // libmongoc uses a NULL level to mean "use the server's default read_concern."
    libmongoc::read_concern_set_level(
        _impl->read_concern_t,
        rc_string.empty() ? NULL : bsoncxx::v_noabi::string::to_string(rc_string).data());
}

read_concern::level read_concern::acknowledge_level() const {
    auto level = libmongoc::read_concern_get_level(_impl->read_concern_t);
    if (!level) {
        return read_concern::level::k_server_default;
    }
    if (strcmp(MONGOC_READ_CONCERN_LEVEL_LOCAL, level) == 0) {
        return read_concern::level::k_local;
    } else if (strcmp(MONGOC_READ_CONCERN_LEVEL_MAJORITY, level) == 0) {
        return read_concern::level::k_majority;
    } else if (strcmp(MONGOC_READ_CONCERN_LEVEL_LINEARIZABLE, level) == 0) {
        return read_concern::level::k_linearizable;
    } else if (strcmp(MONGOC_READ_CONCERN_LEVEL_AVAILABLE, level) == 0) {
        return read_concern::level::k_available;
    } else if (strcmp(MONGOC_READ_CONCERN_LEVEL_SNAPSHOT, level) == 0) {
        return read_concern::level::k_snapshot;
    } else {
        return read_concern::level::k_unknown;
    }
}

stdx::string_view read_concern::acknowledge_string() const {
    auto level = libmongoc::read_concern_get_level(_impl->read_concern_t);
    if (!level) {
        return "";
    }
    return {stdx::string_view{level}};
}

bsoncxx::v_noabi::document::value read_concern::to_document() const {
    using bsoncxx::v_noabi::builder::basic::kvp;

    auto level = libmongoc::read_concern_get_level(_impl->read_concern_t);

    bsoncxx::v_noabi::builder::basic::document doc;
    if (level) {
        doc.append(kvp("level", level));
    };

    return doc.extract();
}

bool MONGOCXX_CALL operator==(const read_concern& lhs, const read_concern& rhs) {
    return lhs.acknowledge_level() == rhs.acknowledge_level();
}

bool MONGOCXX_CALL operator!=(const read_concern& lhs, const read_concern& rhs) {
    return !(lhs == rhs);
}

}  // namespace v_noabi
}  // namespace mongocxx
