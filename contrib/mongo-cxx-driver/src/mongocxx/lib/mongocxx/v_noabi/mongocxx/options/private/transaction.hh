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

#include <chrono>
#include <memory>

#include <bsoncxx/stdx/make_unique.hpp>
#include <mongocxx/private/read_concern.hh>
#include <mongocxx/private/read_preference.hh>
#include <mongocxx/private/write_concern.hh>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace options {

class transaction::impl {
   public:
    impl()
        : _transaction_opt_t(unique_transaction_opt{libmongoc::transaction_opts_new(),
                                                    &mongoc_transaction_opts_destroy}) {}

    impl(mongoc_transaction_opt_t* txn_opts)
        : _transaction_opt_t(unique_transaction_opt{libmongoc::transaction_opts_clone(txn_opts),
                                                    &mongoc_transaction_opts_destroy}) {}

    impl(const impl& other)
        : _transaction_opt_t(unique_transaction_opt{
              libmongoc::transaction_opts_clone(other._transaction_opt_t.get()),
              &mongoc_transaction_opts_destroy}) {}

    impl& operator=(const impl& other) {
        _transaction_opt_t = unique_transaction_opt{
            libmongoc::transaction_opts_clone(other._transaction_opt_t.get()),
            &mongoc_transaction_opts_destroy};
        return *this;
    }

    impl(impl&&) noexcept = default;
    impl& operator=(impl&&) noexcept = default;

    void read_concern(const mongocxx::v_noabi::read_concern& rc) {
        libmongoc::transaction_opts_set_read_concern(_transaction_opt_t.get(),
                                                     rc._impl->read_concern_t);
    }

    stdx::optional<mongocxx::v_noabi::read_concern> read_concern() const {
        auto rc = libmongoc::transaction_opts_get_read_concern(_transaction_opt_t.get());
        if (!rc) {
            return {};
        }
        mongocxx::v_noabi::read_concern rci(
            stdx::make_unique<read_concern::impl>(libmongoc::read_concern_copy(rc)));
        return stdx::optional<mongocxx::v_noabi::read_concern>(std::move(rci));
    }

    void write_concern(const mongocxx::v_noabi::write_concern& wc) {
        libmongoc::transaction_opts_set_write_concern(_transaction_opt_t.get(),
                                                      wc._impl->write_concern_t);
    }

    stdx::optional<mongocxx::v_noabi::write_concern> write_concern() const {
        auto wc = libmongoc::transaction_opts_get_write_concern(_transaction_opt_t.get());
        if (!wc) {
            return {};
        }
        mongocxx::v_noabi::write_concern wci(
            stdx::make_unique<write_concern::impl>(libmongoc::write_concern_copy(wc)));
        return stdx::optional<mongocxx::v_noabi::write_concern>(std::move(wci));
    }

    void read_preference(const mongocxx::v_noabi::read_preference& rp) {
        libmongoc::transaction_opts_set_read_prefs(_transaction_opt_t.get(),
                                                   rp._impl->read_preference_t);
    }

    stdx::optional<mongocxx::v_noabi::read_preference> read_preference() const {
        auto rp = libmongoc::transaction_opts_get_read_prefs(_transaction_opt_t.get());
        if (!rp) {
            return {};
        }
        mongocxx::v_noabi::read_preference rpi(
            stdx::make_unique<read_preference::impl>(libmongoc::read_prefs_copy(rp)));
        return stdx::optional<mongocxx::v_noabi::read_preference>(std::move(rpi));
    }

    void max_commit_time_ms(std::chrono::milliseconds ms) {
        libmongoc::transaction_opts_set_max_commit_time_ms(_transaction_opt_t.get(), ms.count());
    }

    stdx::optional<std::chrono::milliseconds> max_commit_time_ms() const {
        auto ms = libmongoc::transaction_opts_get_max_commit_time_ms(_transaction_opt_t.get());
        if (!ms) {
#if !defined(__clang__) && defined(__GNUC__) && (__cplusplus >= 201709L)
// Silence false positive with g++ 10.2.1 on Debian 11.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
            return {};
#if !defined(__clang__) && defined(__GNUC__) && (__cplusplus >= 201709L)
#pragma GCC diagnostic pop
#endif
        }
        return {std::chrono::milliseconds{ms}};
    }

    mongoc_transaction_opt_t* get_transaction_opt_t() const noexcept {
        return _transaction_opt_t.get();
    }

   private:
    using unique_transaction_opt =
        std::unique_ptr<mongoc_transaction_opt_t, decltype(&mongoc_transaction_opts_destroy)>;

    unique_transaction_opt _transaction_opt_t;
};

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
