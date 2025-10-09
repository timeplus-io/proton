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

#include <bsoncxx/document/value.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/private/libbson.hh>
#include <bsoncxx/stdx/optional.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/test_util/export_for_testing.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace libbson {

//
// Class that wraps and manages libmongoc's bson_t structures. It is useful for converting between
// mongocxx's bson::document::view() and libmongoc's bson_t when communicating with the underlying
// driver. It is an RAII style class that will destroy an initialized bson_t when destructed.
//
// Libmongoc's bson_destroy will not be called on the bson_t upon destruction unless either
// init(), flag_init(), init_from_static(), or bson_for_init() are called during a scoped_bson_t's
// lifetime after which the internal bson_t is considered initialized.
//
// Initialization of a scoped bson_t depends on how it is expected to be used.
//
// If the bson_t will be used in a read-only fashion then init_from_static should be called.
//
// If this bson_t will be used by a function that calls bson_init itself then flag_init()
// should be called after such use. If this bson_t will be used by a function that does
// not call init itself (expecting an already initialized bson_t) then init() could be called
// instead.
//
class MONGOCXX_TEST_API scoped_bson_t {
   public:
    //
    // Constructs a new scoped_bson_t having a non-initialized internal bson_t.
    //
    scoped_bson_t();

    //
    // Constructs a new scoped_bson_t from the provided document.
    //
    // The internal bson_t is considered initialized.
    //
    explicit scoped_bson_t(bsoncxx::document::view_or_value doc);

    //
    // Initializes a bson_t from the provided document.
    //
    // The internal bson_t is considered initialized.
    //
    void init_from_static(bsoncxx::document::view_or_value doc);

    //
    // Constructs a new scoped_bson_t from the provided document.
    //
    // The internal bson_t is considered initialized.
    //
    explicit scoped_bson_t(bsoncxx::document::view doc);

    //
    // Initializes a bson_t from the provided document.
    //
    // The internal bson_t is considered initialized.
    //
    void init_from_static(bsoncxx::document::view doc);

    //
    // Constructs a new scoped_bson_t from the provided document.
    //
    // The internal bson_t is considered initialized.
    //
    explicit scoped_bson_t(bsoncxx::document::value doc);

    //
    // Initializes a bson_t from the provided document.
    //
    // The internal bson_t is considered initialized.
    //
    void init_from_static(bsoncxx::document::value doc);

    //
    // Constructs a new scoped_bson_t from the provided optional document.
    //
    // The internal bson_t is initialized if the optional contains a document.
    //
    explicit scoped_bson_t(
        bsoncxx::v_noabi::stdx::optional<bsoncxx::v_noabi::document::view_or_value> doc);

    //
    // Initializes a bson_t from the provided optional document.
    //
    // The internal bson_t is initialized if the optional contains a document.
    //
    void init_from_static(
        bsoncxx::v_noabi::stdx::optional<bsoncxx::v_noabi::document::view_or_value> doc);

    //
    // Initialize the internal bson_t.
    //
    // This is equivalent to calling libmongoc's bson_init() and informing this scoped_bson_t
    // instance that it should call bson_destroy on the internal bson_t when destructed.
    //
    void init();

    //
    // Marks this bson_t as initialized (presumably by another libmongoc function).
    //
    // This C++ class has no way of knowing that the C driver has initialized this bson_t
    // internally (possibly as a side effect of a function call) so this is a way of explictly
    // saying so.
    //
    void flag_init();

    ~scoped_bson_t();

    scoped_bson_t(const scoped_bson_t& rhs) = delete;
    scoped_bson_t& operator=(const scoped_bson_t& rhs) = delete;
    scoped_bson_t(scoped_bson_t&& rhs) = delete;
    scoped_bson_t& operator=(scoped_bson_t&& rhs) = delete;

    //
    // Get a pointer to the wrapped internal bson_t structure.  If not
    // initialized, nullptr is returned.  To use this method to take
    // ownership of a bson_t (by passing the pointer returned here as the
    // output argument of another function), be sure to call flag_init()
    // first.
    //
    bson_t* bson();

    //
    // First calls flag_init() then returns a pointer to the wrapped internal bson_t structure.
    //
    bson_t* bson_for_init();

    bsoncxx::v_noabi::document::view view();
    bsoncxx::v_noabi::document::value steal();

   private:
    bson_t _bson;
    bool _is_initialized = false;

    // If we are passed a value created on-the-fly, we'll need to own this.
    bsoncxx::v_noabi::stdx::optional<bsoncxx::v_noabi::document::view_or_value> _doc;
};

}  // namespace libbson
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
