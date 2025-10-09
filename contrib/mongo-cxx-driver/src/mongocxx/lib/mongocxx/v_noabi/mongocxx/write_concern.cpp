// Copyright 2014 MongoDB Inc.
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
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/private/libmongoc.hh>
#include <mongocxx/private/write_concern.hh>
#include <mongocxx/write_concern.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

write_concern::write_concern() : _impl{stdx::make_unique<impl>(libmongoc::write_concern_new())} {}

write_concern::write_concern(std::unique_ptr<impl>&& implementation) {
    _impl.reset(implementation.release());
}

write_concern::write_concern(write_concern&&) noexcept = default;
write_concern& write_concern::operator=(write_concern&&) noexcept = default;

write_concern::write_concern(const write_concern& other)
    : _impl(stdx::make_unique<impl>(libmongoc::write_concern_copy(other._impl->write_concern_t))) {}

write_concern& write_concern::operator=(const write_concern& other) {
    _impl.reset(stdx::make_unique<impl>(libmongoc::write_concern_copy(other._impl->write_concern_t))
                    .release());
    return *this;
}

write_concern::~write_concern() = default;

void write_concern::journal(bool journal) {
    libmongoc::write_concern_set_journal(_impl->write_concern_t, journal);
}

void write_concern::nodes(std::int32_t confirm_from) {
    if (confirm_from < 0) {
        throw mongocxx::v_noabi::logic_error{error_code::k_invalid_parameter};
    }
    libmongoc::write_concern_set_w(_impl->write_concern_t, confirm_from);
}

void write_concern::acknowledge_level(write_concern::level confirm_level) {
    std::int32_t w = 0;
    switch (confirm_level) {
        case write_concern::level::k_default:
            w = MONGOC_WRITE_CONCERN_W_DEFAULT;
            break;
        case write_concern::level::k_majority:
            w = MONGOC_WRITE_CONCERN_W_MAJORITY;
            break;
        case write_concern::level::k_unacknowledged:
            w = MONGOC_WRITE_CONCERN_W_UNACKNOWLEDGED;
            break;
        case write_concern::level::k_acknowledged:
            w = 1;
            break;
        case write_concern::level::k_tag:
            // no exception for setting tag if it's set
            if (libmongoc::write_concern_get_w(_impl->write_concern_t) !=
                MONGOC_WRITE_CONCERN_W_TAG) {
                throw exception{error_code::k_unknown_write_concern};
            } else {
                return;
            }
    }
    libmongoc::write_concern_set_w(_impl->write_concern_t, w);
}

void write_concern::tag(stdx::string_view confirm_from) {
    libmongoc::write_concern_set_wtag(_impl->write_concern_t,
                                      bsoncxx::v_noabi::string::to_string(confirm_from).data());
}

void write_concern::majority(std::chrono::milliseconds timeout) {
    const auto count = timeout.count();
    if ((count < 0) || (count >= std::numeric_limits<std::int32_t>::max()))
        throw logic_error{error_code::k_invalid_parameter};

    libmongoc::write_concern_set_wmajority(_impl->write_concern_t,
                                           static_cast<std::int32_t>(count));
}

void write_concern::timeout(std::chrono::milliseconds timeout) {
    const auto count = timeout.count();
    if ((count < 0) || (count >= std::numeric_limits<std::int32_t>::max()))
        throw logic_error{error_code::k_invalid_parameter};

    libmongoc::write_concern_set_wtimeout(_impl->write_concern_t, static_cast<std::int32_t>(count));
}

bool write_concern::journal() const {
    return libmongoc::write_concern_get_journal(_impl->write_concern_t);
}

stdx::optional<std::int32_t> write_concern::nodes() const {
    std::int32_t w = libmongoc::write_concern_get_w(_impl->write_concern_t);
    return w >= 0 ? stdx::optional<std::int32_t>{w} : stdx::nullopt;
}

write_concern::level write_concern::acknowledge_level() const {
    std::int32_t w = libmongoc::write_concern_get_w(_impl->write_concern_t);
    if (w >= 1)
        return write_concern::level::k_acknowledged;
    switch (w) {
        case MONGOC_WRITE_CONCERN_W_UNACKNOWLEDGED:
            return write_concern::level::k_unacknowledged;
        case MONGOC_WRITE_CONCERN_W_DEFAULT:
            return write_concern::level::k_default;
        case MONGOC_WRITE_CONCERN_W_MAJORITY:
            return write_concern::level::k_majority;
        case MONGOC_WRITE_CONCERN_W_TAG:
            return write_concern::level::k_tag;
        default:
            MONGOCXX_UNREACHABLE;
    }
}

stdx::optional<std::string> write_concern::tag() const {
    const char* tag_str = libmongoc::write_concern_get_wtag(_impl->write_concern_t);
    return tag_str ? stdx::make_optional<std::string>(tag_str) : stdx::nullopt;
}

bool write_concern::majority() const {
    return libmongoc::write_concern_get_wmajority(_impl->write_concern_t);
}

std::chrono::milliseconds write_concern::timeout() const {
    return std::chrono::milliseconds(libmongoc::write_concern_get_wtimeout(_impl->write_concern_t));
}

bool write_concern::is_acknowledged() const {
    return libmongoc::write_concern_is_acknowledged(_impl->write_concern_t);
}

bsoncxx::v_noabi::document::value write_concern::to_document() const {
    using bsoncxx::v_noabi::builder::basic::kvp;
    using bsoncxx::v_noabi::builder::basic::make_document;

    bsoncxx::v_noabi::builder::basic::document doc;

    if (auto ns = nodes()) {
        doc.append(kvp("w", *ns));
    } else {
        switch (acknowledge_level()) {
            case write_concern::level::k_unacknowledged:
                doc.append(kvp("w", 0));
                break;
            case write_concern::level::k_default:
                // "Commands supporting a write concern MUST NOT send the default write concern to
                // the server." See Spec 135.
                break;
            case write_concern::level::k_majority:
                doc.append(kvp("w", "majority"));
                break;
            case write_concern::level::k_tag:
                if (auto t = tag()) {
                    doc.append(kvp("w", *t));
                }
            default:
                break;
        }
    }

    if (libmongoc::write_concern_journal_is_set(_impl->write_concern_t)) {
        doc.append(kvp("j", journal()));
    }

    std::int32_t count;
    if ((count = static_cast<std::int32_t>(timeout().count())) > 0) {
        doc.append(kvp("wtimeout", bsoncxx::v_noabi::types::b_int32{count}));
    }

    return doc.extract();
}

bool MONGOCXX_CALL operator==(const write_concern& lhs, const write_concern& rhs) {
    return std::forward_as_tuple(lhs.journal(),
                                 lhs.nodes(),
                                 lhs.acknowledge_level(),
                                 lhs.tag(),
                                 lhs.majority(),
                                 lhs.timeout()) == std::forward_as_tuple(rhs.journal(),
                                                                         rhs.nodes(),
                                                                         rhs.acknowledge_level(),
                                                                         rhs.tag(),
                                                                         rhs.majority(),
                                                                         rhs.timeout());
}

bool MONGOCXX_CALL operator!=(const write_concern& lhs, const write_concern& rhs) {
    return !(lhs == rhs);
}

}  // namespace v_noabi
}  // namespace mongocxx
