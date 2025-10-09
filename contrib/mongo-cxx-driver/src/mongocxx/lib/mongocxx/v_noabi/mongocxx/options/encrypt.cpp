// Copyright 2020 MongoDB Inc.
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

#include <utility>

#include <bsoncxx/types/private/convert.hh>
#include <mongocxx/exception/error_code.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/options/encrypt.hpp>
#include <mongocxx/private/libbson.hh>
#include <mongocxx/private/libmongoc.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace options {

encrypt& encrypt::key_id(bsoncxx::v_noabi::types::bson_value::view_or_value key_id) {
    _key_id = std::move(key_id);
    return *this;
}

const stdx::optional<bsoncxx::v_noabi::types::bson_value::view_or_value>& encrypt::key_id() const {
    return _key_id;
}

encrypt& encrypt::key_alt_name(std::string name) {
    _key_alt_name = std::move(name);
    return *this;
}

const stdx::optional<std::string>& encrypt::key_alt_name() const {
    return _key_alt_name;
}

encrypt& encrypt::algorithm(encrypt::encryption_algorithm algorithm) {
    _algorithm = algorithm;
    return *this;
}

const stdx::optional<encrypt::encryption_algorithm>& encrypt::algorithm() const {
    return _algorithm;
}

encrypt& encrypt::contention_factor(int64_t contention_factor) {
    _contention_factor = contention_factor;
    return *this;
}

const stdx::optional<int64_t>& encrypt::contention_factor() const {
    return _contention_factor;
}

encrypt& encrypt::query_type(encrypt::encryption_query_type query_type) {
    _query_type = query_type;
    return *this;
}

const stdx::optional<encrypt::encryption_query_type>& encrypt::query_type() const {
    return _query_type;
}

encrypt& encrypt::range_opts(options::range opts) {
    _range_opts = std::move(opts);
    return *this;
}

const stdx::optional<options::range>& encrypt::range_opts() const {
    return _range_opts;
}

namespace {

struct scoped_bson_value {
    bson_value_t value = {};

    // Allow obtaining a pointer to this->value even in rvalue expressions.
    bson_value_t* get() noexcept {
        return &value;
    }

    // Communicate this->value is to be initialized via the resulting pointer.
    bson_value_t* value_for_init() noexcept {
        return &this->value;
    }

    template <typename T>
    auto convert(const T& value)
        // Use trailing return type syntax to SFINAE without triggering GCC -Wignored-attributes
        // warnings due to using decltype within template parameters.
        -> decltype(bsoncxx::v_noabi::types::convert_to_libbson(std::declval<const T&>(),
                                                                std::declval<bson_value_t*>())) {
        bsoncxx::v_noabi::types::convert_to_libbson(value, &this->value);
    }

    template <typename T>
    explicit scoped_bson_value(const T& value) {
        convert(value);
    }

    explicit scoped_bson_value(const bsoncxx::v_noabi::types::bson_value::view& view) {
        // Argument order is reversed for bsoncxx::v_noabi::types::bson_value::view.
        bsoncxx::v_noabi::types::convert_to_libbson(&this->value, view);
    }

    ~scoped_bson_value() {
        bson_value_destroy(&value);
    }

    // Expectation is that value_for_init() will be used to initialize this->value.
    scoped_bson_value() = default;

    scoped_bson_value(const scoped_bson_value&) = delete;
    scoped_bson_value(scoped_bson_value&&) = delete;
    scoped_bson_value& operator=(const scoped_bson_value&) = delete;
    scoped_bson_value& operator=(scoped_bson_value&&) = delete;
};

}  // namespace

void* encrypt::convert() const {
    using libbson::scoped_bson_t;

    struct encrypt_opts_deleter {
        void operator()(mongoc_client_encryption_encrypt_opts_t* ptr) noexcept {
            libmongoc::client_encryption_encrypt_opts_destroy(ptr);
        }
    };

    auto opts_owner =
        std::unique_ptr<mongoc_client_encryption_encrypt_opts_t, encrypt_opts_deleter>(
            libmongoc::client_encryption_encrypt_opts_new());
    const auto opts = opts_owner.get();

    // libmongoc will error if both key_id and key_alt_name are set, so no need to check here.

    if (_key_id) {
        if (_key_id->view().type() != bsoncxx::v_noabi::type::k_binary) {
            throw exception{error_code::k_invalid_parameter, "key id myst be a binary value"};
        }

        auto key_id = _key_id->view().get_binary();

        if (key_id.sub_type != bsoncxx::v_noabi::binary_sub_type::k_uuid) {
            throw exception{error_code::k_invalid_parameter,
                            "key id must be a binary value with subtype 4 (UUID)"};
        }

        libmongoc::client_encryption_encrypt_opts_set_keyid(opts, scoped_bson_value(key_id).get());
    }

    if (_key_alt_name) {
        libmongoc::client_encryption_encrypt_opts_set_keyaltname(opts, _key_alt_name->c_str());
    }

    if (_algorithm) {
        switch (*_algorithm) {
            case encryption_algorithm::k_deterministic:
                libmongoc::client_encryption_encrypt_opts_set_algorithm(
                    opts, "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic");
                break;
            case encryption_algorithm::k_random:
                libmongoc::client_encryption_encrypt_opts_set_algorithm(
                    opts, "AEAD_AES_256_CBC_HMAC_SHA_512-Random");
                break;
            case encryption_algorithm::k_indexed:
                libmongoc::client_encryption_encrypt_opts_set_algorithm(
                    opts, MONGOC_ENCRYPT_ALGORITHM_INDEXED);
                break;
            case encryption_algorithm::k_unindexed:
                libmongoc::client_encryption_encrypt_opts_set_algorithm(
                    opts, MONGOC_ENCRYPT_ALGORITHM_UNINDEXED);
                break;
            case encryption_algorithm::k_range_preview:
                libmongoc::client_encryption_encrypt_opts_set_algorithm(
                    opts, MONGOC_ENCRYPT_ALGORITHM_RANGEPREVIEW);
                break;
            default:
                throw exception{error_code::k_invalid_parameter,
                                "unsupported encryption algorithm"};
        }
    } else {
        // libmongoc will error in this case, encryption algorithm must be set.
    }

    if (_contention_factor) {
        libmongoc::client_encryption_encrypt_opts_set_contention_factor(opts,
                                                                        _contention_factor.value());
    }

    if (_query_type) {
        switch (*_query_type) {
            case encryption_query_type::k_equality:
                libmongoc::client_encryption_encrypt_opts_set_query_type(
                    opts, MONGOC_ENCRYPT_QUERY_TYPE_EQUALITY);
                break;
            case encryption_query_type::k_range_preview:
                libmongoc::client_encryption_encrypt_opts_set_query_type(
                    opts, MONGOC_ENCRYPT_QUERY_TYPE_RANGEPREVIEW);
                break;
            default:
                throw exception{error_code::k_invalid_parameter, "unsupported query type"};
        }
    }

    if (_range_opts) {
        struct range_opts_deleter {
            void operator()(mongoc_client_encryption_encrypt_range_opts_t* ptr) noexcept {
                libmongoc::client_encryption_encrypt_range_opts_destroy(ptr);
            }
        };

        auto range_opts_owner =
            std::unique_ptr<mongoc_client_encryption_encrypt_range_opts_t, range_opts_deleter>(
                libmongoc::client_encryption_encrypt_range_opts_new());
        const auto range_opts = range_opts_owner.get();

        const auto& min = _range_opts->min();
        const auto& max = _range_opts->max();
        const auto& precision = _range_opts->precision();
        const auto& sparsity = _range_opts->sparsity();

        if (min) {
            libmongoc::client_encryption_encrypt_range_opts_set_min(
                range_opts, scoped_bson_value(min->view()).get());
        }

        if (max) {
            libmongoc::client_encryption_encrypt_range_opts_set_max(
                range_opts, scoped_bson_value(max->view()).get());
        }

        if (precision) {
            libmongoc::client_encryption_encrypt_range_opts_set_precision(range_opts, *precision);
        }

        if (sparsity) {
            libmongoc::client_encryption_encrypt_range_opts_set_sparsity(range_opts, *sparsity);
        }

        libmongoc::client_encryption_encrypt_opts_set_range_opts(opts, range_opts);
    }

    return opts_owner.release();
}

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx
