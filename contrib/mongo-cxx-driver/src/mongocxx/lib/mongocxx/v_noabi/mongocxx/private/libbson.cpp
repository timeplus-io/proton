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

#include <mongocxx/private/libbson.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace libbson {

namespace {

void doc_to_bson_t(const bsoncxx::v_noabi::document::view& doc, bson_t* bson) {
    // While bson_init_static is documented as returning false if the bson_t was unable to be
    // initialized, this only occurs when the length of the data passed in is less than five. We
    // assume that the data from the bsoncxx::v_noabi::document::view is valid and that
    // bson_init_static will not be changed to fail to initialize the bson_t in any other case.
    bson_init_static(bson, doc.data(), doc.length());
}

}  // namespace

scoped_bson_t::scoped_bson_t(bsoncxx::document::view_or_value doc)
    : _is_initialized{true}, _doc{std::move(doc)} {
    doc_to_bson_t(*_doc, &_bson);
}

void scoped_bson_t::init_from_static(bsoncxx::document::view_or_value doc) {
    _is_initialized = true;
    _doc = std::move(doc);
    doc_to_bson_t(*_doc, &_bson);
}

scoped_bson_t::scoped_bson_t(bsoncxx::document::view doc)
    : scoped_bson_t(bsoncxx::document::view_or_value(doc)) {}

void scoped_bson_t::init_from_static(bsoncxx::document::view doc) {
    this->init_from_static(bsoncxx::document::view_or_value(doc));
}

scoped_bson_t::scoped_bson_t(bsoncxx::document::value doc)
    : scoped_bson_t(bsoncxx::document::view_or_value(std::move(doc))) {}

void scoped_bson_t::init_from_static(bsoncxx::document::value doc) {
    this->init_from_static(bsoncxx::document::view_or_value(std::move(doc)));
}

scoped_bson_t::scoped_bson_t(bsoncxx::stdx::optional<bsoncxx::document::view_or_value> doc) {
    if (doc) {
        this->init_from_static(std::move(*doc));
    }
}

void scoped_bson_t::init_from_static(
    bsoncxx::v_noabi::stdx::optional<bsoncxx::v_noabi::document::view_or_value> doc) {
    if (doc) {
        this->init_from_static(std::move(*doc));
    }
}

void scoped_bson_t::init() {
    _is_initialized = true;
    bson_init(&_bson);
}

void scoped_bson_t::flag_init() {
    _is_initialized = true;
}

scoped_bson_t::scoped_bson_t() : _is_initialized(false) {}

scoped_bson_t::~scoped_bson_t() {
    if (_is_initialized) {
        bson_destroy(&_bson);
    }
}

bson_t* scoped_bson_t::bson() {
    return _is_initialized ? &_bson : nullptr;
}

bson_t* scoped_bson_t::bson_for_init() {
    // C driver doesn't always return reply initted, so run an extra init
    init();
    return &_bson;
}

bsoncxx::v_noabi::document::view scoped_bson_t::view() {
    // if we were initialized with a view_or_value just use that view
    if (_doc) {
        return _doc->view();
    }
    // otherwise, if we were initialized from libmongoc, construct
    if (_is_initialized) {
        return bsoncxx::v_noabi::document::view{bson_get_data(bson()), bson()->len};
    }
    // otherwise, return an empty view
    return bsoncxx::v_noabi::document::view{};
}

namespace {

void bson_free_deleter(std::uint8_t* ptr) {
    bson_free(ptr);
}

}  // anonymous namespace

bsoncxx::v_noabi::document::value scoped_bson_t::steal() {
    if (!_is_initialized) {
        return bsoncxx::v_noabi::document::value{bsoncxx::v_noabi::document::view()};
    }

    std::uint32_t length;
    std::uint8_t* buff = bson_destroy_with_steal(bson(), true, &length);

    _is_initialized = false;

    return bsoncxx::v_noabi::document::value(buff, length, bson_free_deleter);
}

}  // namespace libbson
}  // namespace mongocxx
