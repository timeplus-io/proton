#include "StreamingEmitInterpreter.h"

#include <Interpreters/GetAggregatesVisitor.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/Streaming/StreamingWindowCommon.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/Streaming/ASTEmitQuery.h>
#include <Parsers/queryToString.h>
#include <base/logger_useful.h>
#include <Common/IntervalKind.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

namespace
{
    std::vector<const ASTFunction *> getAggregates(ASTPtr & query, const ASTSelectQuery & select_query)
    {
        /// There can not be aggregate functions inside the WHERE and PREWHERE.
        if (select_query.where())
            assertNoAggregates(select_query.where(), "in WHERE");
        if (select_query.prewhere())
            assertNoAggregates(select_query.prewhere(), "in PREWHERE");

        GetAggregatesVisitor::Data data;
        GetAggregatesVisitor(data).visit(query);

        /// There can not be other aggregate functions within the aggregate functions.
        for (const ASTFunction * node : data.aggregates)
        {
            if (node->arguments)
            {
                for (auto & arg : node->arguments->children)
                {
                    assertNoAggregates(arg, "inside another aggregate function");
                    // We also can't have window functions inside aggregate functions,
                    // because the window functions are calculated later.
                    assertNoWindows(arg, "inside an aggregate function");
                }
            }
        }
        return data.aggregates;
    }

    bool hasAggregates(ASTPtr & query, const ASTSelectQuery & select_query)
    {
        auto aggregates = getAggregates(query, select_query);
        return (!aggregates.empty());
    }

    Int64 extractIntervalSeconds(const ASTFunction * func)
    {
        assert(func);
        auto [interval, interval_kind] = extractInterval(func);
        return intervalToSeconds(interval, interval_kind);
    }
}

StreamingEmitInterpreter::LastXRule::LastXRule(
    const Settings & settings_, Int64 & last_interval_seconds_, bool & tail_, Poco::Logger * log_)
    : settings(settings_), last_interval_seconds(last_interval_seconds_), tail(tail_), log(log_)
{
}

void StreamingEmitInterpreter::LastXRule::operator()(ASTPtr & query_)
{
    query = query_;
    auto select_query = query_->as<ASTSelectQuery>();
    if (!select_query)
        return;

    emit_query = select_query->emit();
    if (!emit_query)
        return;

    auto emit = emit_query->as<ASTEmitQuery>();
    assert(emit);

    last_interval = emit->last_interval;
    if (!last_interval)
        return;

    /// The order of window aggr / global aggr / tail matters
    if (handleWindowAggr(*select_query))
        return;

    if (handleGlobalAggr(*select_query))
        return;

    handleTail(*select_query);
}

bool StreamingEmitInterpreter::LastXRule::handleWindowAggr(ASTSelectQuery & select_query)
{
    assert(last_interval);
    auto table_expression = getTableExpression(select_query, 0);
    if (!table_expression || !table_expression->table_function)
        return false;

    ASTPtr new_emit = emit_query->clone();
    auto new_emit_query = new_emit->as<ASTEmitQuery>();
    assert(new_emit_query);
    new_emit_query->last_interval.reset();

    auto table_func = table_expression->table_function->as<ASTFunction>();
    /// window interval seconds
    Int64 window_interval_seconds = 0;
    if (isTableFunctionTumble(table_func))
    {
        /// tumble(table, [time_expr], win_interval, [timezone])
        auto win_interval = checkAndExtractTumbleArguments(table_func)[2];
        window_interval_seconds = extractIntervalSeconds(win_interval->as<ASTFunction>());
    }
    else if (isTableFunctionHop(table_func))
    {
        /// hop(table, [timestamp_column], hop_interval, hop_win_interval, [timezone])
        auto hop_interval = checkAndExtractHopArguments(table_func)[2];
        window_interval_seconds = extractIntervalSeconds(hop_interval->as<ASTFunction>());
    }
    else
        return false;

    last_interval_seconds = extractIntervalSeconds(last_interval->as<ASTFunction>());

    /// calculate settings keep_windows = ceil(last_interval / window_interval)
    UInt64 keep_windows = (std::abs(last_interval_seconds) + std::abs(window_interval_seconds) - 1) / std::abs(window_interval_seconds);
    if (keep_windows == 0 || keep_windows > settings.max_keep_windows)
        throw Exception(
            "Too big range. Try make the last range smaller or make the hop/tumble window size bigger to make 'range / window_size' less "
            "than or equal to "
                + std::to_string(settings.max_keep_windows),
            ErrorCodes::SYNTAX_ERROR);

    const auto & old_settings = select_query.settings();
    ASTPtr new_settings = old_settings ? old_settings->clone() : std::make_shared<ASTSetQuery>();
    auto & ast_set = new_settings->as<ASTSetQuery &>();

    if (ast_set.changes.tryGet("keep_windows"))
        throw Exception("The `emit last` policy conflicts with the existing 'keep_windows' setting", ErrorCodes::SYNTAX_ERROR);

    ast_set.is_standalone = false;
    ast_set.changes.emplace_back("keep_windows", keep_windows);

    if (ast_set.changes.tryGet("seek_to"))
        throw Exception("The `emit last` policy conflicts with the existing 'seek_to' setting", ErrorCodes::SYNTAX_ERROR);

    /// Seek to -3600s for example
    ast_set.changes.emplace_back("seek_to", "-" + std::to_string(last_interval_seconds) + "s");

    select_query.setExpression(ASTSelectQuery::Expression::EMIT, std::move(new_emit));
    select_query.setExpression(ASTSelectQuery::Expression::SETTINGS, std::move(new_settings));

    if (log)
        LOG_INFO(log, "(LastXForWindow) processed query: {}", queryToString(query));

    return true;
}

bool StreamingEmitInterpreter::LastXRule::handleGlobalAggr(ASTSelectQuery & select_query)
{
    assert(emit_query);
    assert(last_interval);
    auto table_expression = getTableExpression(select_query, 0);
    if (!hasAggregates(query, select_query) || !table_expression || !table_expression->database_and_table_name)
        return false;

    ASTPtr new_emit = emit_query->clone();
    auto new_emit_query = new_emit->as<ASTEmitQuery>();
    assert(new_emit_query);
    new_emit_query->last_interval.reset();

    ASTPtr periodic_interval;
    IntervalKind periodic_interval_kind = IntervalKind::Second;
    last_interval_seconds = extractIntervalSeconds(last_interval->as<ASTFunction>());
    if (new_emit_query->periodic_interval)
    {
        /// check periodic_interval is appropriate value by settings.max_keep_windows
        periodic_interval = std::move(new_emit_query->periodic_interval);
        auto [slide_interval, slide_interval_kind] = extractInterval(periodic_interval->as<ASTFunction>());
        Int64 slide_interval_seconds = intervalToSeconds(slide_interval, slide_interval_kind);
        periodic_interval_kind = slide_interval_kind;

        UInt64 keep_windows = (std::abs(last_interval_seconds) + std::abs(slide_interval_seconds) - 1) / std::abs(slide_interval_seconds);
        if (keep_windows == 0 || keep_windows > settings.max_keep_windows)
            throw Exception(
                "Too big range or too small emit interval. Make sure 'range / emit_interval' is less or equal to "
                    + std::to_string(settings.max_keep_windows),
                ErrorCodes::SYNTAX_ERROR);
    }
    else
    {
        /// if periodic_interval is omitted, we calculate a appropriate value by settings.max_keep_windows.
        Int64 slide_interval_seconds = last_interval_seconds / settings.max_keep_windows;
        auto [slide_interval, slide_interval_kind] = secondsToInterval(slide_interval_seconds == 0 ? 1 : slide_interval_seconds);
        periodic_interval = makeASTInterval(slide_interval, slide_interval_kind);
        periodic_interval_kind = slide_interval_kind;
    }

    /// To keep same scale, we convert last interval to periodic interval kind.
    auto [conv_interval, conv_interval_kind] = secondsToInterval(last_interval_seconds, periodic_interval_kind);
    last_interval = makeASTInterval(conv_interval, conv_interval_kind);

    /// Create a table function: hop(table, now(), periodic_interval, last_time_interval)
    auto table_expr = std::make_shared<ASTTableExpression>();
    table_expr->table_function = makeASTFunction(
        "hop",
        std::make_shared<ASTIdentifier>(table_expression->database_and_table_name->as<ASTTableIdentifier &>().name()),
        makeASTFunction("now"),
        periodic_interval,
        last_interval);
    table_expr->children.emplace_back(table_expr->table_function);
    auto element = std::make_shared<ASTTablesInSelectQueryElement>();
    element->table_expression = table_expr;
    element->children.emplace_back(element->table_expression);
    auto new_table = std::make_shared<ASTTablesInSelectQuery>();
    new_table->children.emplace_back(element);

    /// we add 'window_end' into groupby.
    ASTPtr new_groupby = select_query.groupBy() ? select_query.groupBy()->clone() : std::make_shared<ASTExpressionList>();
    auto new_groupby_list = new_groupby->as<ASTExpressionList>();
    assert(new_groupby_list);
    new_groupby_list->children.push_back(std::make_shared<ASTIdentifier>(STREAMING_WINDOW_END));

    select_query.setExpression(ASTSelectQuery::Expression::TABLES, std::move(new_table));
    select_query.setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(new_groupby));
    select_query.setExpression(ASTSelectQuery::Expression::EMIT, std::move(new_emit));

    const auto & old_settings = select_query.settings();
    ASTPtr new_settings = old_settings ? old_settings->clone() : std::make_shared<ASTSetQuery>();
    auto & ast_set = new_settings->as<ASTSetQuery &>();
    ast_set.is_standalone = false;
    if (ast_set.changes.tryGet("seek_to"))
        throw Exception("The `emit last` policy conflicts with the existing 'seek_to' setting", ErrorCodes::SYNTAX_ERROR);

    /// Seek to -3600s for example
    ast_set.changes.emplace_back("seek_to", "-" + std::to_string(last_interval_seconds) + "s");
    select_query.setExpression(ASTSelectQuery::Expression::SETTINGS, std::move(new_settings));

    if (log)
        LOG_INFO(log, "(LastXForGlobal) processed query: {}", queryToString(query));

    return true;
}

void StreamingEmitInterpreter::LastXRule::handleTail(ASTSelectQuery & select_query)
{
    assert(last_interval);
    assert(emit_query);

    tail = true;
    ASTPtr new_emit = emit_query->clone();
    auto new_emit_query = new_emit->as<ASTEmitQuery>();
    assert(new_emit_query);
    new_emit_query->last_interval.reset();

    last_interval_seconds = extractIntervalSeconds(last_interval->as<ASTFunction>());

    const auto & old_settings = select_query.settings();
    ASTPtr new_settings = old_settings ? old_settings->clone() : std::make_shared<ASTSetQuery>();
    auto & ast_set = new_settings->as<ASTSetQuery &>();
    ast_set.is_standalone = false;
    if (ast_set.changes.tryGet("seek_to"))
        throw Exception("The `emit last` policy conflicts with the existing 'seek_to' setting", ErrorCodes::SYNTAX_ERROR);

    /// Seek to -3600s for example
    ast_set.changes.emplace_back("seek_to", "-" + std::to_string(last_interval_seconds) + "s");

    select_query.setExpression(ASTSelectQuery::Expression::EMIT, std::move(new_emit));
    select_query.setExpression(ASTSelectQuery::Expression::SETTINGS, std::move(new_settings));

    if (log)
        LOG_INFO(log, "(LastXForWindow) processed query: {}", queryToString(query));
}
}
