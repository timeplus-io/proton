// Copyright 2018-present MongoDB Inc.
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

#include <bsoncxx/document/view.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/change_stream.hpp>
#include <mongocxx/exception/private/mongoc_error.hh>
#include <mongocxx/exception/query_exception.hpp>
#include <mongocxx/private/libbson.hh>
#include <mongocxx/private/libmongoc.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

class change_stream::impl {
   public:
    // lifecycle of the cursor
    // k_started means that libmongoc::change_stream_next has been called at least once.
    // k_pending means it hasn't
    // k_dead means that an error was indicated by a call to next
    enum class state { k_pending, k_started, k_dead };

    explicit impl(mongoc_change_stream_t* change_stream)
        : change_stream_(change_stream), status_{state::k_pending}, exhausted_{true} {}

    // no copy or move
    impl(impl&) = delete;
    impl(impl&&) = delete;
    void operator=(const impl&) = delete;
    void operator=(impl&&) = delete;

    ~impl() {
        libmongoc::change_stream_destroy(this->change_stream_);
    }

    bool has_started() const {
        return status_ >= state::k_started;
    }

    bool is_dead() const {
        return status_ == state::k_dead;
    }

    bool is_exhausted() const {
        return exhausted_;
    }

    void mark_dead() {
        mark_nothing_left();
        status_ = state::k_dead;
    }

    void mark_nothing_left() {
        doc_ = bsoncxx::v_noabi::document::view{};
        exhausted_ = true;
        status_ = state::k_pending;
    }

    void mark_started() {
        status_ = state::k_started;
        exhausted_ = false;
    }

    void advance_iterator() {
        const bson_t* out;

        // Happy-case.
        if (libmongoc::change_stream_next(this->change_stream_, &out)) {
            this->doc_ = bsoncxx::v_noabi::document::view{bson_get_data(out), out->len};
            return;
        }

        // Check for errors or just nothing left.
        bson_error_t error;
        if (libmongoc::change_stream_error_document(this->change_stream_, &error, &out)) {
            this->mark_dead();
            this->doc_ = bsoncxx::v_noabi::document::view{};
            mongocxx::libbson::scoped_bson_t scoped_error_reply{};
            bson_copy_to(out, scoped_error_reply.bson_for_init());
            throw_exception<query_exception>(scoped_error_reply.steal(), error);
        }

        // Just nothing left.
        this->mark_nothing_left();
    }

    bsoncxx::v_noabi::document::view& doc() {
        return this->doc_;
    }

    stdx::optional<bsoncxx::v_noabi::document::view> get_resume_token() {
        auto token = libmongoc::change_stream_get_resume_token(this->change_stream_);
        if (!token) {
            return {};
        }

        return {bsoncxx::v_noabi::document::view{bson_get_data(token), token->len}};
    }

   private:
    mongoc_change_stream_t* const change_stream_;
    bsoncxx::v_noabi::document::view doc_;
    state status_;
    bool exhausted_;
};

}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
