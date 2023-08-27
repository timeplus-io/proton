#pragma once

#include <Parsers/IAST_fwd.h>

namespace DB
{
struct ASTTableExpression;

namespace Streaming
{
/// Rewrite `table/table_function` to subquery:
/// 1) `stream1`              => `(select * from stream1) as stream1`
/// 2) `stream2 as t`              => `(select * from stream2) as t`
/// 3) `table_func(...) as t1`    => `(select * from table_func(...)) as t1`
/// Return rewritten subquery (return `nullptr` if is subquery)
ASTPtr rewriteAsSubquery(ASTTableExpression & table_expression);
}
}
