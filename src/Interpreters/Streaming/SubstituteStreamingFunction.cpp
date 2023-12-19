#include <Interpreters/Streaming/SubstituteStreamingFunction.h>

#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/formatAST.h>
#include <Common/ProtonCommon.h>
#include <Functions/UserDefined/UserDefinedFunctionFactory.h>

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
    {"min_k", "__min_k_retract"},
    {"max_k", "__max_k_retract"},
    {"unique", ""},
    {"unique_exact", ""},
    {"median", ""},
    {"quantile", ""},
    {"p90", ""},
    {"p95", ""},
    {"p99", ""},
    {"moving_sum", ""},
};

std::optional<String> StreamingFunctionData::supportChangelog(const String & function_name)
{
    auto iter = changelog_func_map.find(function_name);

    /// Support combinator suffix, for example:
    /// `count`                 => `__count_retract`
    /// `count_if`              => `__count_retract_if`
    /// `count_distinct`        => `__count_retract_distinct_retract`
    /// `count_distinct_if`     => `__count_retract_distinct_retract_if`
    String combinator_suffix;
    constexpr std::string_view distinct_raw{"_distinct"}, distinct_retract_raw{"_distinct_retract"};
    auto nested_func_name = function_name;
    while (iter == changelog_func_map.end())
    {
        if (auto combinator = AggregateFunctionCombinatorFactory::instance().tryFindSuffix(nested_func_name))
        {
            std::string combinator_name = combinator->getName();
            /// TODO: support more combinators
            if (combinator_name != "_if" && combinator_name != distinct_raw && combinator_name != distinct_retract_raw)
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED, "{} aggregation function is not supported in changelog query processing", function_name);

            nested_func_name = nested_func_name.substr(0, nested_func_name.size() - combinator_name.size());

            /// replace `<aggr>_distinct[_combinator]` ==> `<aggr>_distinct_retract[_combinator]` for changelog query
            if (combinator_name == distinct_raw)
                combinator_name = distinct_retract_raw;
            combinator_suffix = combinator_name + combinator_suffix;
            iter = changelog_func_map.find(nested_func_name);
            continue;
        }
        break;
    }

    if (iter != changelog_func_map.end())
    {
        if (!iter->second.empty())
            return iter->second + combinator_suffix;
        else
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED, "{} aggregation function is not supported in changelog query processing", function_name);
    }

    /// UDA by default support changelog
    if (UserDefinedFunctionFactory::isAggregateFunctionName(function_name))
        return function_name;

    return {};
}

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
            /// Always show original func name
            func.covered_name = func.name;
            func.name = iter->second;
            return;
        }

        if (is_changelog)
        {
            /// Whether the function support 'retract' for changelog, also return the alias name of
            /// function used in rewritten query
            auto func_alias_name = supportChangelog(func.name);
            if (func_alias_name.has_value())
            {
                if (!func_alias_name->empty())
                {
                    /// Always show original function
                    func.code_name = DB::serializeAST(func);

                    func.name = *func_alias_name;
                    if (!func.arguments)
                        func.arguments = std::make_shared<ASTExpressionList>();

                    auto delta_pos = func.arguments->children.end();

                    /// Keep last argument always is if-condition.
                    if (func.name.ends_with("_if"))
                        --delta_pos;

                    /// Make _tp_delta as the last argument to avoid unused column elimination for query like below
                    /// SELECT count(), avg(i) FROM (SELECT i, _tp_delta FROM versioned_kv) GROUP BY i; =>
                    /// SELECT __count_retract(_tp_delta), __avg_retract(i, _tp_delta) FROM (SELECT i, _tp_delta FROM versioned_kv) GROUP BY i; =>
                    if ((func.name == "__count_retract" || func.name == "__count_retract_if") && delta_pos - func.arguments->children.begin() > 0)
                        /// Fix for nullable since this substitution is not equal
                        func.arguments->children[0] = std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_DELTA_FLAG);
                    else
                        func.arguments->children.insert(delta_pos, std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_DELTA_FLAG));
                }

                return;
            }
        }
        else
        {
            /// replace `<aggr>_distinct[_combinator]` ==> `<aggr>_distinct_streaming[_combinator]` for streaming query
            constexpr std::string_view distinct_raw{"_distinct"}, distinct_streaming_raw{"_distinct_streaming"};
            if (size_t pos = func.name.find(distinct_raw); pos != std::string::npos)
                func.name.replace(pos, distinct_raw.length(), distinct_streaming_raw);
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
