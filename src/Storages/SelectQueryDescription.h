#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

namespace DB
{

/// Select query for different view in storages
struct SelectQueryDescription
{
    /// Table id for select query
    std::vector<StorageID> select_table_ids; /// proton : supports mv union
    /// Select query itself (ASTSelectWithUnionQuery)
    ASTPtr inner_query;

    /// Parse description from select query for materialized view. Also
    /// validates query.
    /// proton: starts. Rename to `getSelectQueryFromASTForView`
    static SelectQueryDescription getSelectQueryFromASTForView(const ASTPtr & select, ContextPtr context);
    /// proton: ends.

    SelectQueryDescription() = default;
    SelectQueryDescription(const SelectQueryDescription & other);
    SelectQueryDescription & operator=(const SelectQueryDescription & other);
};

}
