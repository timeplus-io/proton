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

#pragma once

#include <bsoncxx/document/view.hpp>
#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/cursor.hpp>
#include <mongocxx/private/libmongoc.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

class cursor::impl {
   public:
    // States represent a one-way, ordered lifecycle of a cursor. k_started means that
    // libmongoc::cursor_next has been called at least once.  However, for a tailable
    // cursor, the cursor resets to k_pending on exhaustion so that it can resume later.
    enum class state { k_pending = 0, k_started = 1, k_dead = 2 };

    impl(mongoc_cursor_t* cursor, bsoncxx::v_noabi::stdx::optional<cursor::type> cursor_type)
        : cursor_t(cursor),
          status{cursor ? state::k_pending : state::k_dead},
          exhausted(!cursor),
          tailable{cursor && cursor_type &&
                   (*cursor_type == cursor::type::k_tailable ||
                    *cursor_type == cursor::type::k_tailable_await)} {}

    ~impl() {
        libmongoc::cursor_destroy(cursor_t);
    }

    bool has_started() const {
        return status >= state::k_started;
    }

    bool is_dead() const {
        return status == state::k_dead;
    }

    bool is_exhausted() const {
        return exhausted;
    }

    bool is_tailable() const {
        return tailable;
    }

    void mark_dead() {
        mark_nothing_left();
        status = state::k_dead;
    }

    void mark_nothing_left() {
        doc = bsoncxx::v_noabi::document::view{};
        exhausted = true;
        status = tailable ? state::k_pending : state::k_dead;
    }

    void mark_started() {
        status = state::k_started;
        exhausted = false;
    }

    mongoc_cursor_t* cursor_t;
    bsoncxx::v_noabi::document::view doc;
    state status;
    bool exhausted;
    bool tailable;
};

}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
