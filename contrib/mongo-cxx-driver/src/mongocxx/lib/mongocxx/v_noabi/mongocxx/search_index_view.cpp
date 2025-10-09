#include <bsoncxx/stdx/make_unique.hpp>
#include <mongocxx/exception/error_code.hpp>
#include <mongocxx/exception/logic_error.hpp>
#include <mongocxx/private/search_index_view.hh>
#include <mongocxx/search_index_view.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

search_index_view::search_index_view(void* coll, void* client)
    : _impl{stdx::make_unique<impl>(static_cast<mongoc_collection_t*>(coll),
                                    static_cast<mongoc_client_t*>(client))} {}

search_index_view::search_index_view(search_index_view&&) noexcept = default;
search_index_view& search_index_view::operator=(search_index_view&&) noexcept = default;

search_index_view::search_index_view(const search_index_view& other)
    : _impl(stdx::make_unique<impl>(other._get_impl())) {}

search_index_view& search_index_view::operator=(const search_index_view& other) {
    _get_impl() = other._get_impl();
    return *this;
}

search_index_view::~search_index_view() = default;

cursor search_index_view::list(const options::aggregate& options) {
    return _get_impl().list(nullptr, options);
}

cursor search_index_view::list(const client_session& session, const options::aggregate& options) {
    return _get_impl().list(&session, options);
}

cursor search_index_view::list(bsoncxx::v_noabi::string::view_or_value name,
                               const options::aggregate& options) {
    return _get_impl().list(nullptr, name, options);
}

cursor search_index_view::list(const client_session& session,
                               bsoncxx::v_noabi::string::view_or_value name,
                               const options::aggregate& options) {
    return _get_impl().list(&session, name, options);
}

std::string search_index_view::create_one(bsoncxx::v_noabi::document::view_or_value definition) {
    return create_one(search_index_model(definition));
}

std::string search_index_view::create_one(const client_session& session,
                                          bsoncxx::v_noabi::document::view_or_value definition) {
    return create_one(session, search_index_model(definition));
}

std::string search_index_view::create_one(bsoncxx::v_noabi::string::view_or_value name,
                                          bsoncxx::v_noabi::document::view_or_value definition) {
    return create_one(search_index_model(name, definition));
}

std::string search_index_view::create_one(const client_session& session,
                                          bsoncxx::v_noabi::string::view_or_value name,
                                          bsoncxx::v_noabi::document::view_or_value definition) {
    return create_one(session, search_index_model(name, definition));
}

std::string search_index_view::create_one(const search_index_model& model) {
    return _get_impl().create_one(nullptr, model);
}

std::string search_index_view::create_one(const client_session& session,
                                          const search_index_model& model) {
    return _get_impl().create_one(&session, model);
}

std::vector<std::string> search_index_view::create_many(
    const std::vector<search_index_model>& models) {
    auto response = _get_impl().create_many(nullptr, models);
    return _create_many_helper(response["indexesCreated"].get_array().value);
}

std::vector<std::string> search_index_view::create_many(
    const client_session& session, const std::vector<search_index_model>& models) {
    auto response = _get_impl().create_many(&session, models);
    return _create_many_helper(response["indexesCreated"].get_array().value);
}

std::vector<std::string> search_index_view::_create_many_helper(
    bsoncxx::v_noabi::array::view created_indexes) {
    std::vector<std::string> search_index_names;
    for (auto&& index : created_indexes) {
        search_index_names.push_back(bsoncxx::v_noabi::string::to_string(
            index.get_document().value["name"].get_string().value));
    }
    return search_index_names;
}

void search_index_view::drop_one(bsoncxx::v_noabi::string::view_or_value name) {
    _get_impl().drop_one(nullptr, name);
}

void search_index_view::drop_one(const client_session& session,
                                 bsoncxx::v_noabi::string::view_or_value name) {
    _get_impl().drop_one(&session, name);
}

void search_index_view::update_one(bsoncxx::v_noabi::string::view_or_value name,
                                   bsoncxx::v_noabi::document::view_or_value definition) {
    _get_impl().update_one(nullptr, name, definition);
}

void search_index_view::update_one(const client_session& session,
                                   bsoncxx::v_noabi::string::view_or_value name,
                                   bsoncxx::v_noabi::document::view_or_value definition) {
    _get_impl().update_one(&session, name, definition);
}

const search_index_view::impl& search_index_view::_get_impl() const {
    if (!_impl) {
        throw mongocxx::v_noabi::logic_error{error_code::k_invalid_search_index_view};
    }
    return *_impl;
}

search_index_view::impl& search_index_view::_get_impl() {
    auto cthis = const_cast<const search_index_view*>(this);
    return const_cast<search_index_view::impl&>(cthis->_get_impl());
}

}  // namespace v_noabi
}  // namespace mongocxx
