#include <Interpreters/InterpreterShowStoragePoliciesQuery.h>

#include <Interpreters/executeQuery.h>
#include <Parsers/ASTShowStoragePoliciesQuery.h>
#include <Parsers/formatAST.h>

namespace DB
{

InterpreterShowStoragePoliciesQuery::InterpreterShowStoragePoliciesQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
    : WithMutableContext(context_), query_ptr(query_ptr_)
{
}

String InterpreterShowStoragePoliciesQuery::getRewrittenQuery()
{
    const auto & query = query_ptr->as<ASTShowStoragePoliciesQuery &>();

    WriteBufferFromOwnString rewritten_query;
    rewritten_query << "SELECT * FROM system.storage_policies";

    if (!query.like.empty())
        rewritten_query << " WHERE policy_name " << (query.not_like ? "NOT " : "") << (query.case_insensitive_like ? "ILIKE " : "LIKE ")
                        << DB::quote << query.like;
    else if (query.where_expression)
        rewritten_query << " WHERE (" << query.where_expression << ")";

    if (query.limit_length)
        rewritten_query << " LIMIT " << query.limit_length;

    return rewritten_query.str();
}

BlockIO InterpreterShowStoragePoliciesQuery::execute()
{
    return executeQuery(getRewrittenQuery(), getContext(), true);
}
}
