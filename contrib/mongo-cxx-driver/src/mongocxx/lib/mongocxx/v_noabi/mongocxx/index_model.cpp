// Copyright 2017 MongoDB Inc.
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

#include <mongocxx/index_model.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

index_model::index_model(const bsoncxx::v_noabi::document::view_or_value& keys,
                         const bsoncxx::v_noabi::document::view_or_value& options)
    : _keys{keys.view()}, _options{options.view()} {}

index_model::index_model(index_model&&) noexcept = default;

index_model& index_model::operator=(index_model&&) noexcept = default;

index_model::index_model(const index_model&) = default;

index_model::~index_model() = default;

bsoncxx::v_noabi::document::view index_model::keys() const {
    return _keys.view();
}

bsoncxx::v_noabi::document::view index_model::options() const {
    return _options.view();
}

}  // namespace v_noabi
}  // namespace mongocxx
