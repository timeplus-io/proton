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

#pragma once

#include <unordered_map>

#include "entity.hh"
#include <bsoncxx/array/element.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/document/view.hpp>
#include <mongocxx/test/spec/monitoring.hh>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace operations {

struct state {
    std::unordered_map<mongocxx::cursor*, mongocxx::cursor::iterator> cursor_iters;
    std::unordered_map<mongocxx::change_stream*, mongocxx::change_stream::iterator> stream_iters;
};

bsoncxx::document::value run(entity::map& map,
                             std::unordered_map<std::string, spec::apm_checker>& apm,
                             const bsoncxx::array::element& op,
                             state& state);

bsoncxx::stdx::optional<read_concern> lookup_read_concern(bsoncxx::document::view doc);
bsoncxx::stdx::optional<write_concern> lookup_write_concern(bsoncxx::document::view doc);
bsoncxx::stdx::optional<read_preference> lookup_read_preference(bsoncxx::document::view doc);

}  // namespace operations
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
