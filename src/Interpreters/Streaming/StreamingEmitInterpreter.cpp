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
}

StreamingEmitInterpreter::LastXRule::LastXRule(
    const Settings & settings_, BaseScaleInterval & last_interval_bs_, bool & tail_, Poco::Logger * log_)
    : settings(settings_), last_interval_bs(last_interval_bs_), tail(tail_), log(log_)
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
    ASTPtr interval_ast;
    if (isTableFunctionTumble(table_func))
    {
        /// tumble(table, [time_expr], win_interval, [timezone])
        interval_ast = checkAndExtractTumbleArguments(table_func)[2];
    }
    else if (isTableFunctionHop(table_func))
    {
        /// hop(table, [timestamp_column], hop_interval, hop_win_interval, [timezone])
        interval_ast = checkAndExtractHopArguments(table_func)[2];
    }
    else
        return false;

    auto window_interval_bs = BaseScaleInterval::toBaseScale(extractInterval(interval_ast->as<ASTFunction>()));
    last_interval_bs = BaseScaleInterval::toBaseScale(extractInterval(last_interval->as<ASTFunction>()));
    if (window_interval_bs.scale != last_interval_bs.scale)
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "Cannot convert between win interval '{}' and last interval '{}'",
            IntervalKind(window_interval_bs.src_kind).toString(),
            IntervalKind(last_interval_bs.src_kind).toString());

    /// calculate settings keep_windows = ceil(last_interval / window_interval)
    UInt64 keep_windows
        = (std::abs(last_interval_bs.num_units) + std::abs(window_interval_bs.num_units) - 1) / std::abs(window_interval_bs.num_units);
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
    ast_set.changes.emplace_back("seek_to", "-" + last_interval_bs.toString());

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

    /// FIXME: If the global aggr has several hist tables, could foreach all tables to convert ?
    /// Example:
    ///     table1, table2                  -> hop(table1, ...), hop(table2, ...)
    ///     subquery1, subquery2            -> hop(subquery1, ...), hop(subquery2, ...)
    ///     hist(table1), hist(table2)      -> hop(hist(table1), ...), hop(hist(table2), ...)
    if (getTableExpressions(select_query).size() > 1)
        Exception("No support serveral tables in the `emit last` policy", ErrorCodes::SYNTAX_ERROR);

    auto table_expression = getTableExpression(select_query, 0);
    if (!hasAggregates(query, select_query) || !table_expression)
        return false;

    ASTPtr new_emit = emit_query->clone();
    auto new_emit_query = new_emit->as<ASTEmitQuery>();
    assert(new_emit_query);
    new_emit_query->last_interval.reset();

    ASTPtr periodic_interval;
    last_interval_bs = BaseScaleInterval::toBaseScale(extractInterval(last_interval->as<ASTFunction>()));
    if (new_emit_query->periodic_interval)
    {
        /// check periodic_interval is appropriate value by settings.max_keep_windows
        periodic_interval = std::move(new_emit_query->periodic_interval);
        auto periodic_interval_bs = BaseScaleInterval::toBaseScale(extractInterval(periodic_interval->as<ASTFunction>()));
        if (periodic_interval_bs.scale != last_interval_bs.scale)
            throw Exception(
                ErrorCodes::SYNTAX_ERROR,
                "Cannot convert between periodic interval '{}' and last interval '{}'",
                IntervalKind(periodic_interval_bs.src_kind).toString(),
                IntervalKind(last_interval_bs.src_kind).toString());

        UInt64 keep_windows
            = (std::abs(last_interval_bs.num_units) + std::abs(periodic_interval_bs.num_units) - 1) / std::abs(periodic_interval_bs.num_units);
        if (keep_windows == 0 || keep_windows > settings.max_keep_windows)
            throw Exception(
                "Too big range or too small emit interval. Make sure 'range / emit_interval' is less or equal to "
                    + std::to_string(settings.max_keep_windows),
                ErrorCodes::SYNTAX_ERROR);

        /// To keep same scale between last interval and periodic interval.
        convertToSameKindIntervalAST(periodic_interval_bs, last_interval_bs, periodic_interval, last_interval);
    }
    else
    {
        /// if periodic_interval is omitted, we calculate a appropriate value by settings.max_keep_windows.
        auto periodic_interval_bs = last_interval_bs / settings.max_keep_windows;
        periodic_interval = makeASTInterval(periodic_interval_bs.num_units == 0 ? 1 : periodic_interval_bs.num_units, periodic_interval_bs.scale);

        /// To keep same scale between last interval and periodic interval.
        if (last_interval_bs.scale != last_interval_bs.src_kind)
            last_interval = makeASTInterval(last_interval_bs.num_units, last_interval_bs.scale);
    }

    ASTPtr table;
    if (table_expression->database_and_table_name)
        table = std::make_shared<ASTIdentifier>(table_expression->database_and_table_name->as<ASTTableIdentifier &>().name());
    else if (table_expression->table_function)
        table = table_expression->table_function;
    else if (table_expression->subquery)
        table = table_expression->subquery;
    else
        throw Exception("The table is empty", ErrorCodes::SYNTAX_ERROR);

    /// Create a table function: hop(table_expression, now(), periodic_interval, last_time_interval)
    /// The table_expression can be table, hist(table) and subquery.
    auto table_expr = std::make_shared<ASTTableExpression>();
    table_expr->table_function = makeASTFunction("hop", table, makeASTFunction("now"), periodic_interval, last_interval);
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
    ast_set.changes.emplace_back("seek_to", "-" + last_interval_bs.toString());
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

    last_interval_bs = BaseScaleInterval::toBaseScale(extractInterval(last_interval->as<ASTFunction>()));

    const auto & old_settings = select_query.settings();
    ASTPtr new_settings = old_settings ? old_settings->clone() : std::make_shared<ASTSetQuery>();
    auto & ast_set = new_settings->as<ASTSetQuery &>();
    ast_set.is_standalone = false;
    if (ast_set.changes.tryGet("seek_to"))
        throw Exception("The `emit last` policy conflicts with the existing 'seek_to' setting", ErrorCodes::SYNTAX_ERROR);

    /// Seek to -3600s for example
    ast_set.changes.emplace_back("seek_to", "-" + last_interval_bs.toString());

    select_query.setExpression(ASTSelectQuery::Expression::EMIT, std::move(new_emit));
    select_query.setExpression(ASTSelectQuery::Expression::SETTINGS, std::move(new_settings));

    if (log)
        LOG_INFO(log, "(LastXForWindow) processed query: {}", queryToString(query));
}

void StreamingEmitInterpreter::checkEmitAST(ASTPtr & query)
{
    auto select_query = query->as<ASTSelectQuery>();
    if (!select_query)
        return;

    auto emit_query = select_query->emit();
    if (!emit_query)
        return;

    auto emit = emit_query->as<ASTEmitQuery>();
    assert(emit);

    if (emit->periodic_interval)
        checkIntervalAST(emit->periodic_interval, "Invalid EMIT PERIODIC interval");

    if (emit->delay_interval)
        checkIntervalAST(emit->delay_interval, "Invalid EMIT DELAY interval");

    if (emit->last_interval)
        checkIntervalAST(emit->last_interval, "Invalid EMIT LAST interval");
}

}
