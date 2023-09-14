#include <Interpreters/Streaming/SubstituteStreamingFunction.h>

#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
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
            /// Always show original column name
            func.code_name = func.getColumnNameWithoutAlias();
            func.name = iter->second;
            return;
        }

        if (is_changelog)
        {
            iter = changelog_func_map.find(func.name);

            /// Support combinator suffix, for example:
            /// `count`                 => `__count_retract`
            /// `count_if`              => `__count_retract_if`
            /// `count_distinct`        => `__count_retract_distinct`
            /// `count_distinct_if`     => `__count_retract_distinct_if`
            String combinator_suffix;
            auto nested_func_name = func.name;
            while (iter == changelog_func_map.end())
            {
                if (auto combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix(nested_func_name))
                {
                    const std::string & combinator_name = combinator->getName();
                    /// TODO: support more combinators
                    if (combinator_name != "_if")
                        throw Exception(
                            ErrorCodes::NOT_IMPLEMENTED,
                            "{} aggregation function is not supported in changelog query processing",
                            func.name);

                    nested_func_name = nested_func_name.substr(0, nested_func_name.size() - combinator_name.size());
                    combinator_suffix = combinator_name + combinator_suffix;
                    iter = changelog_func_map.find(nested_func_name);
                    continue;
                }
                break;
            }

            if (iter != changelog_func_map.end())
            {
                if (!iter->second.empty())
                {
                    /// Always show original column name
                    func.code_name = func.getColumnNameWithoutAlias();

                    func.name = iter->second + combinator_suffix;
                    if (!func.arguments)
                        func.arguments = std::make_shared<ASTExpressionList>();

                    auto delta_pos = func.arguments->children.end();

                    /// Keep last argument always is if-condition.
                    if (func.name.ends_with("_if"))
                        --delta_pos;

                    /// Make _tp_delta as the last argument to avoid unused column elimination for query like below
                    /// SELECT count(), avg(i) FROM (SELECT i, _tp_delta FROM versioned_kv) GROUP BY i; =>
                    /// SELECT __count_retract(_tp_delta), __avg_retract(i, _tp_delta) FROM (SELECT i, _tp_delta FROM versioned_kv) GROUP BY i; =>
                    if (func.name.starts_with("__count_retract") && delta_pos - func.arguments->children.begin() > 0)
                        /// Fix for nullable since this substitution is not equal
                        func.arguments->children[0] = std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_DELTA_FLAG);
                    else
                        func.arguments->children.insert(delta_pos, std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_DELTA_FLAG));
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

void StreamingNowFunctionData::visit(DB::ASTFunction & func, DB::ASTPtr)
{
    if (func.name == "now" || func.name == "now64")
        func.name = "__streaming_" + func.name;
}
}
}
