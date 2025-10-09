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

#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/builder/basic/sub_document.hpp>
#include <bsoncxx/stdx/make_unique.hpp>
#include <mongocxx/pipeline.hpp>
#include <mongocxx/private/pipeline.hh>
#include <mongocxx/stdx.hpp>

#include <mongocxx/config/private/prelude.hh>

using bsoncxx::v_noabi::builder::basic::kvp;
using bsoncxx::v_noabi::builder::basic::sub_document;

namespace mongocxx {
namespace v_noabi {

pipeline::pipeline() : _impl(stdx::make_unique<impl>()) {}

pipeline::pipeline(pipeline&&) noexcept = default;
pipeline& pipeline::operator=(pipeline&&) noexcept = default;
pipeline::~pipeline() = default;

pipeline& pipeline::add_fields(bsoncxx::v_noabi::document::view_or_value fields_to_add) {
    _impl->sink().append([fields_to_add](sub_document sub_doc) {
        sub_doc.append(kvp("$addFields", fields_to_add));
    });

    return *this;
}

pipeline& pipeline::append_stage(bsoncxx::v_noabi::document::view_or_value stage) {
    _impl->sink().append(std::move(stage));
    return *this;
}

pipeline& pipeline::append_stages(bsoncxx::v_noabi::array::view_or_value stages) {
    for (auto&& stage : stages.view()) {
        _impl->sink().append(stage.get_document().value);
    }
    return *this;
}

pipeline& pipeline::bucket(bsoncxx::v_noabi::document::view_or_value bucket_args) {
    _impl->sink().append(
        [bucket_args](sub_document sub_doc) { sub_doc.append(kvp("$bucket", bucket_args)); });

    return *this;
}

pipeline& pipeline::bucket_auto(bsoncxx::v_noabi::document::view_or_value bucket_auto_args) {
    _impl->sink().append([bucket_auto_args](sub_document sub_doc) {
        sub_doc.append(kvp("$bucketAuto", bucket_auto_args));
    });

    return *this;
}

pipeline& pipeline::coll_stats(bsoncxx::v_noabi::document::view_or_value coll_stats_args) {
    _impl->sink().append([coll_stats_args](sub_document sub_doc) {
        sub_doc.append(kvp("$collStats", coll_stats_args));
    });

    return *this;
}

pipeline& pipeline::count(std::string field) {
    _impl->sink().append([field](sub_document sub_doc) { sub_doc.append(kvp("$count", field)); });

    return *this;
}

pipeline& pipeline::current_op(bsoncxx::v_noabi::document::view_or_value current_op_args) {
    _impl->sink().append([current_op_args](sub_document sub_doc) {
        sub_doc.append(kvp("$currentOp", current_op_args));
    });

    return *this;
}

pipeline& pipeline::facet(bsoncxx::v_noabi::document::view_or_value facet_args) {
    _impl->sink().append(
        [facet_args](sub_document sub_doc) { sub_doc.append(kvp("$facet", facet_args)); });

    return *this;
}

pipeline& pipeline::geo_near(bsoncxx::v_noabi::document::view_or_value geo_near_args) {
    _impl->sink().append(
        [geo_near_args](sub_document sub_doc) { sub_doc.append(kvp("$geoNear", geo_near_args)); });

    return *this;
}

pipeline& pipeline::graph_lookup(bsoncxx::v_noabi::document::view_or_value graph_lookup_args) {
    _impl->sink().append([graph_lookup_args](sub_document sub_doc) {
        sub_doc.append(kvp("$graphLookup", graph_lookup_args));
    });

    return *this;
}

pipeline& pipeline::group(bsoncxx::v_noabi::document::view_or_value group_args) {
    _impl->sink().append(
        [group_args](sub_document sub_doc) { sub_doc.append(kvp("$group", group_args)); });

    return *this;
}

pipeline& pipeline::index_stats() {
    _impl->sink().append([](sub_document sub_doc) {
        sub_doc.append(kvp("$indexStats", bsoncxx::v_noabi::document::view{}));
    });

    return *this;
}

pipeline& pipeline::limit(std::int32_t limit) {
    _impl->sink().append([limit](sub_document sub_doc) { sub_doc.append(kvp("$limit", limit)); });

    return *this;
}

pipeline& pipeline::list_local_sessions(
    bsoncxx::v_noabi::document::view_or_value list_local_sessions_args) {
    _impl->sink().append([list_local_sessions_args](sub_document sub_doc) {
        sub_doc.append(kvp("$listLocalSessions", list_local_sessions_args));
    });

    return *this;
}

pipeline& pipeline::list_sessions(bsoncxx::v_noabi::document::view_or_value list_sessions_args) {
    _impl->sink().append([list_sessions_args](sub_document sub_doc) {
        sub_doc.append(kvp("$listSessions", list_sessions_args));
    });

    return *this;
}

pipeline& pipeline::lookup(bsoncxx::v_noabi::document::view_or_value lookup_args) {
    _impl->sink().append(
        [lookup_args](sub_document sub_doc) { sub_doc.append(kvp("$lookup", lookup_args)); });

    return *this;
}

pipeline& pipeline::match(bsoncxx::v_noabi::document::view_or_value filter) {
    _impl->sink().append([filter](sub_document sub_doc) { sub_doc.append(kvp("$match", filter)); });

    return *this;
}

pipeline& pipeline::merge(bsoncxx::v_noabi::document::view_or_value merge_args) {
    _impl->sink().append(
        [merge_args](sub_document sub_doc) { sub_doc.append(kvp("$merge", merge_args)); });

    return *this;
}

pipeline& pipeline::out(std::string collection_name) {
    _impl->sink().append(
        [collection_name](sub_document sub_doc) { sub_doc.append(kvp("$out", collection_name)); });

    return *this;
}

pipeline& pipeline::project(bsoncxx::v_noabi::document::view_or_value projection) {
    _impl->sink().append(
        [projection](sub_document sub_doc) { sub_doc.append(kvp("$project", projection)); });

    return *this;
}

pipeline& pipeline::redact(bsoncxx::v_noabi::document::view_or_value restrictions) {
    _impl->sink().append(
        [restrictions](sub_document sub_doc) { sub_doc.append(kvp("$redact", restrictions)); });

    return *this;
}

pipeline& pipeline::replace_root(bsoncxx::v_noabi::document::view_or_value replace_root_args) {
    _impl->sink().append([replace_root_args](sub_document sub_doc) {
        sub_doc.append(kvp("$replaceRoot", replace_root_args));
    });

    return *this;
}

pipeline& pipeline::sample(std::int32_t size) {
    _impl->sink().append([size](sub_document sub_doc1) {
        sub_doc1.append(
            kvp("$sample", [size](sub_document sub_doc2) { sub_doc2.append(kvp("size", size)); }));
    });

    return *this;
}

pipeline& pipeline::skip(std::int32_t docs_to_skip) {
    _impl->sink().append(
        [docs_to_skip](sub_document sub_doc) { sub_doc.append(kvp("$skip", docs_to_skip)); });

    return *this;
}

pipeline& pipeline::sort(bsoncxx::v_noabi::document::view_or_value ordering) {
    _impl->sink().append(
        [ordering](sub_document sub_doc) { sub_doc.append(kvp("$sort", ordering)); });

    return *this;
}

pipeline& pipeline::sort_by_count(bsoncxx::v_noabi::document::view_or_value field_expression) {
    _impl->sink().append([field_expression](sub_document sub_doc) {
        sub_doc.append(kvp("$sortByCount", field_expression));
    });

    return *this;
}

pipeline& pipeline::sort_by_count(std::string field_expression) {
    _impl->sink().append([field_expression](sub_document sub_doc) {
        sub_doc.append(kvp("$sortByCount", field_expression));
    });

    return *this;
}

pipeline& pipeline::unwind(bsoncxx::v_noabi::document::view_or_value unwind_args) {
    _impl->sink().append(
        [unwind_args](sub_document sub_doc) { sub_doc.append(kvp("$unwind", unwind_args)); });

    return *this;
}

pipeline& pipeline::unwind(std::string field_name) {
    _impl->sink().append(
        [field_name](sub_document sub_doc) { sub_doc.append(kvp("$unwind", field_name)); });

    return *this;
}

bsoncxx::v_noabi::array::view pipeline::view_array() const {
    return _impl->view_array();
}

}  // namespace v_noabi
}  // namespace mongocxx
