#pragma once

#include <bsoncxx/document/view_or_value.hpp>
#include <mongocxx/search_index_model.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

class search_index_model::impl {
   public:
    impl(bsoncxx::v_noabi::document::view_or_value definition) : _definition(definition.view()) {}
    impl(bsoncxx::v_noabi::string::view_or_value name,
         bsoncxx::v_noabi::document::view_or_value definition)
        : _name(name), _definition(definition.view()) {}

    bsoncxx::v_noabi::stdx::optional<bsoncxx::v_noabi::string::view_or_value> _name;
    bsoncxx::v_noabi::document::view_or_value _definition;
};

}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
