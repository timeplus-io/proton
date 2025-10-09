#pragma once

#include <bsoncxx/builder/basic/document.hpp>
#include <mongocxx/options/aggregate.hpp>

#include <mongocxx/config/private/prelude.hh>

namespace mongocxx {
namespace v_noabi {

inline void append_aggregate_options(bsoncxx::v_noabi::builder::basic::document& builder,
                                     const options::aggregate& options) {
    using bsoncxx::v_noabi::builder::basic::kvp;

    if (const auto& allow_disk_use = options.allow_disk_use()) {
        builder.append(kvp("allowDiskUse", *allow_disk_use));
    }

    if (const auto& collation = options.collation()) {
        builder.append(kvp("collation", *collation));
    }

    if (const auto& let = options.let()) {
        builder.append(kvp("let", *let));
    }

    if (const auto& max_time = options.max_time()) {
        builder.append(kvp("maxTimeMS", bsoncxx::v_noabi::types::b_int64{max_time->count()}));
    }

    if (const auto& bypass_document_validation = options.bypass_document_validation()) {
        builder.append(kvp("bypassDocumentValidation", *bypass_document_validation));
    }

    if (const auto& hint = options.hint()) {
        builder.append(kvp("hint", hint->to_value()));
    }

    if (const auto& read_concern = options.read_concern()) {
        builder.append(kvp("readConcern", read_concern->to_document()));
    }

    if (const auto& write_concern = options.write_concern()) {
        builder.append(kvp("writeConcern", write_concern->to_document()));
    }

    if (const auto& batch_size = options.batch_size()) {
        builder.append(kvp("batchSize", *batch_size));
    }

    if (const auto& comment = options.comment()) {
        builder.append(kvp("comment", *comment));
    }
}
}  // namespace v_noabi
}  // namespace mongocxx

#include <mongocxx/config/private/postlude.hh>
