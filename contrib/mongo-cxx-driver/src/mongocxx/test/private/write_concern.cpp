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

#include <bsoncxx/test/catch.hh>
#include <mongocxx/instance.hpp>
#include <mongocxx/private/libmongoc.hh>
#include <mongocxx/private/write_concern.hh>
#include <mongocxx/write_concern.hpp>

namespace {
using namespace mongocxx;

TEST_CASE("creation of write_concern passes universal parameters to c-driver's methods",
          "[write_concern][base][c-driver]") {
    instance::current();

    SECTION("when journal is requested, mongoc_write_concern_set_journal is called with true") {
        bool journal_called = false;
        bool journal_value = false;
        auto mock_instance = libmongoc::write_concern_set_journal.create_instance();
        mock_instance->visit([&](mongoc_write_concern_t*, bool journal) {
            journal_called = true;
            journal_value = journal;
        });
        write_concern wc{};
        wc.journal(true);
        write_concern{wc};
        REQUIRE(journal_called == true);
        REQUIRE(journal_value == true);
    }

    SECTION("when a timeout is set, mongoc_write_concern_set_wtimeout is called with that value") {
        bool wtimeout_called = false;
        int wtimeout_value = 0;
        auto mock_instance = libmongoc::write_concern_set_wtimeout.create_instance();
        mock_instance->visit([&](mongoc_write_concern_t*, int wtimeout) {
            wtimeout_called = true;
            wtimeout_value = wtimeout;
        });
        write_concern wc{};
        wc.timeout(std::chrono::seconds(1));
        write_concern{wc};
        REQUIRE(wtimeout_called == true);
        REQUIRE(wtimeout_value == 1000);
    }
}

TEST_CASE("write_concern is called with w MAJORITY", "[write_concern][base][c-driver]") {
    instance::current();

    bool w_called = false, wmajority_called = false, wtag_called = false;
    auto w_instance = libmongoc::write_concern_set_w.create_instance();
    auto wmajority_instance = libmongoc::write_concern_set_wmajority.create_instance();
    auto wtag_instance = libmongoc::write_concern_set_wtag.create_instance();
    w_instance->visit([&](mongoc_write_concern_t*, int) { w_called = true; });
    wmajority_instance->visit([&](mongoc_write_concern_t*, int) { wmajority_called = true; });
    wtag_instance->visit([&](mongoc_write_concern_t*, const char*) { wtag_called = true; });

    write_concern wc{};
    wc.majority(std::chrono::milliseconds(100));
    write_concern{wc};

    SECTION("mongoc_write_concern_set_wmajority is called") {
        REQUIRE(wmajority_called == true);
    }

    SECTION("mongoc_write_concern_set_w is not called") {
        REQUIRE(w_called == false);
    }

    SECTION("mongoc_write_concern_set_wtag is not called") {
        REQUIRE(wtag_called == false);
    }
}

TEST_CASE("write_concern is called with a number of necessary confirmations",
          "[write_concern][base][c-driver]") {
    instance::current();

    bool w_called = false, wmajority_called = false, wtag_called = false;
    int w_value = 0;
    const int expected_w = 5;
    auto w_instance = libmongoc::write_concern_set_w.create_instance();
    auto wmajority_instance = libmongoc::write_concern_set_wmajority.create_instance();
    auto wtag_instance = libmongoc::write_concern_set_wtag.create_instance();
    w_instance->visit([&](mongoc_write_concern_t*, int w) {
        w_called = true;
        w_value = w;
    });
    wmajority_instance->visit([&](mongoc_write_concern_t*, int) { wmajority_called = true; });
    wtag_instance->visit([&](mongoc_write_concern_t*, const char*) { wtag_called = true; });

    write_concern wc{};
    wc.nodes(expected_w);
    write_concern{wc};

    SECTION("mongoc_write_concern_set_w is called with that number") {
        REQUIRE(w_called == true);
        REQUIRE(w_value == expected_w);
    }

    SECTION("mongoc_write_concern_set_wmajority is not called") {
        REQUIRE(wmajority_called == false);
    }

    SECTION("mongoc_write_concern_set_wtag is not called") {
        REQUIRE(wtag_called == false);
    }
}

TEST_CASE("write_concern is called with a tag", "[write_concern][base][c-driver]") {
    instance::current();

    bool w_called = false, wmajority_called = false, wtag_called = false;
    std::string wtag_value;
    const std::string expected_wtag("MultiDataCenter");
    auto w_instance = libmongoc::write_concern_set_w.create_instance();
    auto wmajority_instance = libmongoc::write_concern_set_wmajority.create_instance();
    auto wtag_instance = libmongoc::write_concern_set_wtag.create_instance();
    w_instance->visit([&](mongoc_write_concern_t*, int) { w_called = true; });
    wmajority_instance->visit([&](mongoc_write_concern_t*, int) { wmajority_called = true; });
    wtag_instance->visit([&](mongoc_write_concern_t*, const char* wtag) {
        wtag_called = true;
        wtag_value = wtag;
    });

    write_concern wc{};
    wc.tag(expected_wtag);
    write_concern{wc};

    SECTION("mongoc_write_concern_set_w is not called") {
        REQUIRE(w_called == false);
    }

    SECTION("mongoc_write_concern_set_wmajority is not called") {
        REQUIRE(wmajority_called == false);
    }

    SECTION("mongoc_write_concern_set_wtag is not called") {
        REQUIRE(wtag_called == true);
        REQUIRE(wtag_value == expected_wtag);
    }
}
}  // namespace
