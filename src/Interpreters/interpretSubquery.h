#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>

/// proton: starts.
#include <Storages/Streaming/SeekToInfo.h>
/// proton: ends.

namespace DB
{

std::shared_ptr<InterpreterSelectWithUnionQuery> interpretSubquery(
    const ASTPtr & table_expression, ContextPtr context, size_t subquery_depth, const Names & required_source_columns);

std::shared_ptr<InterpreterSelectWithUnionQuery> interpretSubquery(const ASTPtr & table_expression, ContextPtr context, const Names & required_source_columns, const SelectQueryOptions & options);

std::shared_ptr<InterpreterSelectWithUnionQuery> interpretSubquery(const ASTPtr & table_expression, ContextPtr context, const Names & required_source_columns, const SelectQueryOptions & options,
                                                                   SeekToInfoPtr seek_to_info);
}
