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

#include <string>

#include <bsoncxx/exception/error_code.hpp>

#include <bsoncxx/config/private/prelude.hh>

namespace bsoncxx {
namespace v_noabi {

namespace {

class error_category_impl final : public std::error_category {
   public:
    const char* name() const noexcept override {
        return "bsoncxx";
    }

    std::string message(int condition) const noexcept override {
        switch (static_cast<error_code>(condition)) {
            case error_code::k_cannot_append_key_in_sub_array:
                return "tried to append new key while in subarray";
            case error_code::k_cannot_close_array_in_sub_document:
                return "tried to close array while in subdocument";
            case error_code::k_cannot_close_document_in_sub_array:
                return "tried to close document while in a subarray";
            case error_code::k_cannot_perform_array_operation_on_document:
                return "tried to operate on document, but this is an array";
            case error_code::k_cannot_perform_document_operation_on_array:
                return "tried to operate on array, but this is a document";
#define BSONCXX_ENUM(name, value)                  \
    case error_code::k_need_element_type_k_##name: \
        return {"expected element type k_" #name};
#include <bsoncxx/enums/type.hpp>
#undef BSONCXX_ENUM
            case error_code::k_need_key:
                return "expected a key but found none";
            case error_code::k_no_array_to_close:
                return "tried to close array, but no array was opened";
            case error_code::k_no_document_to_close:
                return "tried to close document, but no document was opened";
            case error_code::k_unmatched_key_in_builder:
                return "can't convert builder to a valid view: unmatched key";
            case error_code::k_unset_element:
                return "unset document::element";
            case error_code::k_invalid_oid:
                return "could not parse Object ID string";
            case error_code::k_json_parse_failure:
                return "could not parse JSON document";
            case error_code::k_invalid_decimal128:
                return "could not parse Decimal128 string";
            case error_code::k_internal_error:
                return "could not process bson data";
            case error_code::k_cannot_begin_appending_array:
                return "unable to begin appending an array";
            case error_code::k_cannot_begin_appending_document:
                return "unable to begin appending a document";
            case error_code::k_cannot_end_appending_array:
                return "tried to complete appending an array, but overflowed";
            case error_code::k_cannot_end_appending_document:
                return "tried to complete appending an document, but overflowed";
            case error_code::k_invalid_bson_type_id:
                return "invalid BSON type identifier";
#define BSONCXX_ENUM(name, value)            \
    case error_code::k_cannot_append_##name: \
        return {"unable to append " #name};
#include <bsoncxx/enums/type.hpp>
#undef BSONCXX_ENUM
            default:
                return "unknown bsoncxx error code";
        }
    }
};

}  // namespace

const std::error_category& BSONCXX_CALL error_category() {
    static const error_category_impl instance{};
    return instance;
}

}  // namespace v_noabi
}  // namespace bsoncxx
