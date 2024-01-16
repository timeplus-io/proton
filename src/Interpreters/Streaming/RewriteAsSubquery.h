#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB
{
struct ASTTableExpression;
class ASTSelectWithUnionQuery;
struct SelectQueryInfo;

namespace Streaming
{
/// Rewrite `table/table_function` to subquery:
/// 1) `stream1`              => `(select * from stream1) as stream1`
/// 2) `stream2 as t`              => `(select * from stream2) as t`
/// 3) `table_func(...) as t1`    => `(select * from table_func(...)) as t1`
/// \return rewritten subquery (return `nullptr` if is subquery)
ASTPtr rewriteAsSubquery(ASTTableExpression & table_expression);

/// Rewrite `table/table_function` to subquery (emit changelog):
/// 1) `stream1`              => `(select * from stream1 emit changelog) as stream1`
/// 2) `stream2 as t`              => `(select * from stream2 emit changelog) as t`
/// 3) `table_func(...) as t1`    => `(select * from table_func(...) emit changelog) as t1`
/// 4) `(select * from stream1) as s`  => `(select * from stream1 emit changelog) as s`
/// \return true if rewritten subquery, otherwise false (if already is changelog subquery or skip storage/table_function)
bool rewriteAsChangelogSubquery(ASTTableExpression & table_expression, bool only_rewrite_subquery);

/// \return true if query was rewritten and false otherwise
bool rewriteSubquery(ASTSelectWithUnionQuery & query, const SelectQueryInfo & query_info);
}
}
