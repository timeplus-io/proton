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

#include <type_traits>

#include <mongocxx/model/write.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace model {

write::write(insert_one value) : _type(write_type::k_insert_one), _insert_one(std::move(value)) {
    static_assert(std::is_nothrow_move_constructible<insert_one>::value,
                  "Move-construct may throw");
    static_assert(std::is_nothrow_move_assignable<insert_one>::value, "Move-assign may throw");
}
write::write(delete_one value) : _type(write_type::k_delete_one), _delete_one(std::move(value)) {
    static_assert(std::is_nothrow_move_constructible<delete_one>::value,
                  "Move-construct may throw");
    static_assert(std::is_nothrow_move_assignable<delete_one>::value, "Move-assign may throw");
}
write::write(delete_many value) : _type(write_type::k_delete_many), _delete_many(std::move(value)) {
    static_assert(std::is_nothrow_move_constructible<delete_many>::value,
                  "Move-construct may throw");
    static_assert(std::is_nothrow_move_assignable<delete_many>::value, "Move-assign may throw");
}
write::write(update_one value) : _type(write_type::k_update_one), _update_one(std::move(value)) {
    static_assert(std::is_nothrow_move_constructible<update_one>::value,
                  "Move-construct may throw");
    static_assert(std::is_nothrow_move_assignable<update_one>::value, "Move-assign may throw");
}
write::write(update_many value) : _type(write_type::k_update_many), _update_many(std::move(value)) {
    static_assert(std::is_nothrow_move_constructible<update_many>::value,
                  "Move-construct may throw");
    static_assert(std::is_nothrow_move_assignable<update_many>::value, "Move-assign may throw");
}
write::write(replace_one value) : _type(write_type::k_replace_one), _replace_one(std::move(value)) {
    static_assert(std::is_nothrow_move_constructible<replace_one>::value,
                  "Move-construct may throw");
    static_assert(std::is_nothrow_move_assignable<replace_one>::value, "Move-assign may throw");
}

write::write(write&& rhs) noexcept {
    switch (rhs._type) {
        case write_type::k_insert_one:
            new (&_insert_one) insert_one(std::move(rhs._insert_one));
            break;
        case write_type::k_update_one:
            new (&_update_one) update_one(std::move(rhs._update_one));
            break;
        case write_type::k_update_many:
            new (&_update_many) update_many(std::move(rhs._update_many));
            break;
        case write_type::k_delete_one:
            new (&_delete_one) delete_one(std::move(rhs._delete_one));
            break;
        case write_type::k_delete_many:
            new (&_delete_many) delete_many(std::move(rhs._delete_many));
            break;
        case write_type::k_replace_one:
            new (&_replace_one) replace_one(std::move(rhs._replace_one));
            break;
    }

    _type = rhs._type;
}

void write::destroy_member() noexcept {
    switch (_type) {
        case write_type::k_insert_one:
            _insert_one.~insert_one();
            break;
        case write_type::k_update_one:
            _update_one.~update_one();
            break;
        case write_type::k_update_many:
            _update_many.~update_many();
            break;
        case write_type::k_delete_one:
            _delete_one.~delete_one();
            break;
        case write_type::k_delete_many:
            _delete_many.~delete_many();
            break;
        case write_type::k_replace_one:
            _replace_one.~replace_one();
            break;
    }
}

write& write::operator=(write&& rhs) noexcept {
    if (this == &rhs) {
        return *this;
    }

    destroy_member();

    switch (rhs._type) {
        case write_type::k_insert_one:
            new (&_insert_one) insert_one(std::move(rhs._insert_one));
            break;
        case write_type::k_update_one:
            new (&_update_one) update_one(std::move(rhs._update_one));
            break;
        case write_type::k_update_many:
            new (&_update_many) update_many(std::move(rhs._update_many));
            break;
        case write_type::k_delete_one:
            new (&_delete_one) delete_one(std::move(rhs._delete_one));
            break;
        case write_type::k_delete_many:
            new (&_delete_many) delete_many(std::move(rhs._delete_many));
            break;
        case write_type::k_replace_one:
            new (&_replace_one) replace_one(std::move(rhs._replace_one));
            break;
    }

    _type = rhs._type;

    return *this;
}

write_type write::type() const {
    return _type;
}

const insert_one& write::get_insert_one() const {
    return _insert_one;
}
const update_one& write::get_update_one() const {
    return _update_one;
}
const update_many& write::get_update_many() const {
    return _update_many;
}
const delete_one& write::get_delete_one() const {
    return _delete_one;
}
const delete_many& write::get_delete_many() const {
    return _delete_many;
}
const replace_one& write::get_replace_one() const {
    return _replace_one;
}

write::~write() {
    destroy_member();
}

}  // namespace model
}  // namespace v_noabi
}  // namespace mongocxx
