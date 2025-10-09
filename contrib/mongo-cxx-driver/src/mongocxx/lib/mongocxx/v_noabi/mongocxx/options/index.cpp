// Copyright 2015 MongoDB Inc.
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
#include <mongocxx/exception/error_code.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/options/index.hpp>
#include <mongocxx/private/libmongoc.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {
namespace options {

index::index() = default;

index& index::background(bool background) {
    _background = background;
    return *this;
}

index& index::unique(bool unique) {
    _unique = unique;
    return *this;
}

index& index::hidden(bool hidden) {
    _hidden = hidden;
    return *this;
}

index& index::name(bsoncxx::v_noabi::string::view_or_value name) {
    _name = std::move(name);
    return *this;
}

index& index::collation(bsoncxx::v_noabi::document::view collation) {
    _collation = collation;
    return *this;
}

index& index::sparse(bool sparse) {
    _sparse = sparse;
    return *this;
}

index& index::storage_options(std::unique_ptr<index::base_storage_options> storage_options) {
    _storage_options = std::move(storage_options);
    return *this;
}

index& index::storage_options(std::unique_ptr<index::wiredtiger_storage_options> storage_options) {
    _storage_options = std::unique_ptr<index::base_storage_options>(
        static_cast<index::base_storage_options*>(storage_options.release()));
    return *this;
}

index& index::expire_after(std::chrono::seconds expire_after) {
    _expire_after = expire_after;
    return *this;
}

index& index::version(std::int32_t version) {
    _version = version;
    return *this;
}

index& index::weights(bsoncxx::v_noabi::document::view weights) {
    _weights = weights;
    return *this;
}

index& index::default_language(bsoncxx::v_noabi::string::view_or_value default_language) {
    _default_language = std::move(default_language);
    return *this;
}

index& index::language_override(bsoncxx::v_noabi::string::view_or_value language_override) {
    _language_override = std::move(language_override);
    return *this;
}

index& index::partial_filter_expression(
    bsoncxx::v_noabi::document::view partial_filter_expression) {
    _partial_filter_expression = partial_filter_expression;
    return *this;
}

index& index::twod_sphere_version(std::uint8_t twod_sphere_version) {
    _twod_sphere_version = twod_sphere_version;
    return *this;
}

index& index::twod_bits_precision(std::uint8_t twod_bits_precision) {
    _twod_bits_precision = twod_bits_precision;
    return *this;
}

index& index::twod_location_min(double twod_location_min) {
    _twod_location_min = twod_location_min;
    return *this;
}

index& index::twod_location_max(double twod_location_max) {
    _twod_location_max = twod_location_max;
    return *this;
}

index& index::haystack_bucket_size_deprecated(double haystack_bucket_size) {
    _haystack_bucket_size = haystack_bucket_size;
    return *this;
}

index& index::haystack_bucket_size(double haystack_bucket_size) {
    return haystack_bucket_size_deprecated(haystack_bucket_size);
}

const stdx::optional<bool>& index::background() const {
    return _background;
}

const stdx::optional<bool>& index::unique() const {
    return _unique;
}

const stdx::optional<bool>& index::hidden() const {
    return _hidden;
}

const stdx::optional<bsoncxx::v_noabi::string::view_or_value>& index::name() const {
    return _name;
}

const stdx::optional<bsoncxx::v_noabi::document::view>& index::collation() const {
    return _collation;
}

const stdx::optional<bool>& index::sparse() const {
    return _sparse;
}

const std::unique_ptr<index::base_storage_options>& index::storage_options() const {
    return _storage_options;
}

const stdx::optional<std::chrono::seconds>& index::expire_after() const {
    return _expire_after;
}

const stdx::optional<std::int32_t>& index::version() const {
    return _version;
}

const stdx::optional<bsoncxx::v_noabi::document::view>& index::weights() const {
    return _weights;
}

const stdx::optional<bsoncxx::v_noabi::string::view_or_value>& index::default_language() const {
    return _default_language;
}

const stdx::optional<bsoncxx::v_noabi::string::view_or_value>& index::language_override() const {
    return _language_override;
}

const stdx::optional<bsoncxx::v_noabi::document::view>& index::partial_filter_expression() const {
    return _partial_filter_expression;
}

const stdx::optional<std::uint8_t>& index::twod_sphere_version() const {
    return _twod_sphere_version;
}

const stdx::optional<std::uint8_t>& index::twod_bits_precision() const {
    return _twod_bits_precision;
}

const stdx::optional<double>& index::twod_location_min() const {
    return _twod_location_min;
}

const stdx::optional<double>& index::twod_location_max() const {
    return _twod_location_max;
}

const stdx::optional<double>& index::haystack_bucket_size_deprecated() const {
    return _haystack_bucket_size;
}

const stdx::optional<double>& index::haystack_bucket_size() const {
    return haystack_bucket_size_deprecated();
}

index::operator bsoncxx::v_noabi::document::view_or_value() {
    using namespace bsoncxx;
    using builder::basic::kvp;
    using builder::basic::make_document;

    builder::basic::document root;

    if (_name) {
        root.append(kvp("name", *_name));
    }

    if (_background) {
        root.append(kvp("background", *_background));
    }

    if (_unique) {
        root.append(kvp("unique", *_unique));
    }

    if (_hidden) {
        root.append(kvp("hidden", *_hidden));
    }

    if (_partial_filter_expression) {
        root.append(kvp("partialFilterExpression", *_partial_filter_expression));
    }

    if (_sparse) {
        root.append(kvp("sparse", *_sparse));
    }

    if (_expire_after) {
        const auto count = _expire_after->count();
        if ((count < 0) || (count > std::numeric_limits<std::int32_t>::max())) {
            throw logic_error{error_code::k_invalid_parameter};
        }

        root.append(kvp("expireAfterSeconds", types::b_int32{static_cast<std::int32_t>(count)}));
    }

    if (_weights) {
        root.append(kvp("weights", *_weights));
    }

    if (_default_language) {
        root.append(kvp("default_language", types::b_string{*_default_language}));
    }

    if (_language_override) {
        root.append(kvp("language_override", types::b_string{*_language_override}));
    }

    if (_twod_sphere_version) {
        root.append(kvp("2dsphereIndexVersion", types::b_int32{*_twod_sphere_version}));
    }

    if (_twod_bits_precision) {
        root.append(kvp("bits", types::b_int32{*_twod_bits_precision}));
    }

    if (_twod_location_min) {
        root.append(kvp("min", types::b_double{*_twod_location_min}));
    }

    if (_twod_location_max) {
        root.append(kvp("max", types::b_double{*_twod_location_max}));
    }

    if (_haystack_bucket_size) {
        root.append(kvp("bucketSize", types::b_double{*_haystack_bucket_size}));
    }

    if (_collation) {
        root.append(kvp("collation", *_collation));
    }

    if (_storage_options) {
        if (_storage_options->type() == MONGOC_INDEX_STORAGE_OPT_WIREDTIGER) {
            const options::index::wiredtiger_storage_options* wt_options =
                static_cast<const options::index::wiredtiger_storage_options*>(
                    _storage_options.get());

            bsoncxx::v_noabi::document::view_or_value storage_doc;
            if (wt_options->config_string()) {
                storage_doc = make_document(
                    kvp("wiredTiger",
                        make_document(kvp("configString", *wt_options->config_string()))));
            } else {
                storage_doc = make_document(
                    kvp("wiredTiger", make_document(kvp("configString", types::b_null{}))));
            }

            root.append(kvp("storageEngine", storage_doc));
        }
    }
    return root.extract();
}

index::base_storage_options::~base_storage_options() = default;

index::wiredtiger_storage_options::~wiredtiger_storage_options() = default;

void index::wiredtiger_storage_options::config_string(
    bsoncxx::v_noabi::string::view_or_value config_string) {
    _config_string = std::move(config_string);
}

const stdx::optional<bsoncxx::v_noabi::string::view_or_value>&
index::wiredtiger_storage_options::config_string() const {
    return _config_string;
}

int index::wiredtiger_storage_options::type() const {
    return mongoc_index_storage_opt_type_t::MONGOC_INDEX_STORAGE_OPT_WIREDTIGER;
}

}  // namespace options
}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/postlude.hpp>
