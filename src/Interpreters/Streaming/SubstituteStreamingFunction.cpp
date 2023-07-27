#include <Interpreters/Streaming/SubstituteStreamingFunction.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int FUNCTION_NOT_ALLOWED;
}

namespace Streaming
{

std::unordered_map<String, String> StreamingFunctionData::func_map = {
    {"neighbor", "__streaming_neighbor"},
    {"row_number", "__streaming_row_number"},
    {"now64", "__streaming_now64"},
    {"now", "__streaming_now"},
};

std::set<String> StreamingFunctionData::streaming_only_func
    = {"__streaming_neighbor",
       "__streaming_row_number",
       "__streaming_now64",
       "__streaming_now",
       /// changelog_only
       "__count_retract",
       "__sum_retract",
       "__sum_kahan_retract",
       "__sum_with_overflow_retract",
       "__avg_retract",
       "__max_retract",
       "__min_retract",
       "__arg_min_retract",
       "__arg_max_retract"};

std::unordered_map<String, String> StreamingFunctionData::changelog_func_map = {
    {"count", "__count_retract"},
    {"sum", "__sum_retract"},
    {"sum_kahan", "__sum_kahan_retract"},
    {"sum_with_overflow", "__sum_with_overflow_retract"},
    {"avg", "__avg_retract"},
    {"max", "__max_retract"},
    {"min", "__min_retract"},
    {"arg_min", "__arg_min_retract"},
    {"arg_max", "__arg_max_retract"},
    {"latest", ""},
    {"earliest", ""},
    {"first_value", ""},
    {"last_value", ""},
    {"top_k", ""},
    {"min_k", ""},
    {"max_k", ""},
    {"count_distinct", ""},
    {"unique", ""},
    {"unique_exact", ""},
    {"unique_exact_if", ""},
    {"median", ""},
    {"quantile", ""},
    {"p90", ""},
    {"p95", ""},
    {"p99", ""},
    {"moving_sum", ""},
};

void StreamingFunctionData::visit(DB::ASTFunction & func, DB::ASTPtr)
{
    if (func.name == "emit_version")
    {
        emit_version = true;
        return;
    }

    if (streaming)
    {
        auto iter = func_map.find(func.name);
        if (iter != func_map.end())
        {
            func.name = iter->second;
            return;
        }

        if (is_changelog)
        {
            iter = changelog_func_map.find(func.name);
            if (iter != changelog_func_map.end())
            {
                if (!iter->second.empty())
                {
                    func.name = iter->second;
                    if (!func.arguments)
                        func.arguments = std::make_shared<ASTExpressionList>();

                    /// Make _tp_delta as the last argument to avoid unused column elimination for query like below
                    /// SELECT count(), avg(i) FROM (SELECT i, _tp_delta FROM versioned_kv) GROUP BY i; =>
                    /// SELECT __count_retract(_tp_delta), __avg_retract(i, _tp_delta) FROM (SELECT i, _tp_delta FROM versioned_kv) GROUP BY i; =>
                    if (func.name == "__count_retract" && func.arguments->children.size() > 0)
                        /// Fix for nullable since this substitution is not equal
                        func.arguments->children[0] = std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_DELTA_FLAG);
                    else
                        func.arguments->children.emplace_back(std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_DELTA_FLAG));
                }
                else
                    throw Exception(
                        ErrorCodes::NOT_IMPLEMENTED, "{} aggregation function is not supported in changelog query processing", func.name);

                return;
            }
        }
    }
    else if (streaming_only_func.contains(func.name))
        throw Exception(
            ErrorCodes::FUNCTION_NOT_ALLOWED, "{} function is private and is not supposed to be used directly in a query", func.name);
}

bool StreamingFunctionData::ignoreSubquery(const DB::ASTPtr &, const DB::ASTPtr & child)
{
    /// Don't go to FROM, JOIN, UNION since they are already handled recursively
    if (child->as<ASTTableExpression>() || child->as<ASTSelectQuery>())
        return false;

    return true;
}

}
}
