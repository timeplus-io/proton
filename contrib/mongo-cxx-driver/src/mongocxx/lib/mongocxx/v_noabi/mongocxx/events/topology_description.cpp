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

#include <mongocxx/events/topology_description.hpp>
#include <mongocxx/private/libmongoc.hh>
#include <mongocxx/private/read_preference.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace events {

topology_description::server_descriptions::server_descriptions(server_descriptions&& other) noexcept
    : _container(std::move(other._container)), _sds(other._sds), _size(other._size) {
    other._sds = nullptr;
    other._size = 0u;
}

topology_description::server_descriptions& topology_description::server_descriptions::operator=(
    server_descriptions&& other) noexcept {
    auto tmp = std::move(other);
    swap(tmp);
    return *this;
}

topology_description::server_descriptions::~server_descriptions() {
    if (_sds) {
        libmongoc::server_descriptions_destroy_all(static_cast<mongoc_server_description_t**>(_sds),
                                                   _size);
    }
}

topology_description::server_descriptions::iterator
topology_description::server_descriptions::begin() noexcept {
    return _container.begin();
}

topology_description::server_descriptions::const_iterator
topology_description::server_descriptions::begin() const noexcept {
    return _container.cbegin();
}

topology_description::server_descriptions::iterator
topology_description::server_descriptions::end() noexcept {
    return _container.end();
}

topology_description::server_descriptions::const_iterator
topology_description::server_descriptions::end() const noexcept {
    return _container.cend();
}

std::size_t topology_description::server_descriptions::size() const noexcept {
    return _size;
}

topology_description::server_descriptions::server_descriptions(void* sds, std::size_t size)
    : _sds(sds), _size(size) {
    for (size_t i = 0; i < size; i++) {
        _container.emplace_back(static_cast<mongoc_server_description_t**>(_sds)[i]);
    }
}

void topology_description::server_descriptions::swap(
    topology_description::server_descriptions& other) noexcept {
    std::swap(_sds, other._sds);
    std::swap(_size, other._size);
    std::swap(_container, other._container);
}

topology_description::topology_description(void* td) : _td(td) {}

topology_description::~topology_description() = default;

bsoncxx::v_noabi::stdx::string_view topology_description::type() const {
    return libmongoc::topology_description_type(static_cast<mongoc_topology_description_t*>(_td));
}

bool topology_description::has_readable_server(
    const mongocxx::v_noabi::read_preference& pref) const {
    return libmongoc::topology_description_has_readable_server(
        static_cast<mongoc_topology_description_t*>(_td), pref._impl->read_preference_t);
}

bool topology_description::has_writable_server() const {
    return libmongoc::topology_description_has_writable_server(
        static_cast<mongoc_topology_description_t*>(_td));
}

topology_description::server_descriptions topology_description::servers() const {
    std::vector<server_description> v;
    std::size_t n;
    auto sds = libmongoc::topology_description_get_servers(
        static_cast<mongoc_topology_description_t*>(_td), &n);
    return server_descriptions{reinterpret_cast<void*>(sds), n};
}

}  // namespace events
}  // namespace v_noabi
}  // namespace mongocxx
