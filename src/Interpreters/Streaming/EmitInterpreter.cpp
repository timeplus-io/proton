#include <Interpreters/Streaming/EmitInterpreter.h>

#include <Interpreters/GetAggregatesVisitor.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/Streaming/WindowCommon.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/Streaming/ASTEmitQuery.h>
#include <Parsers/queryToString.h>
#include <Common/IntervalKind.h>
#include <Common/ProtonCommon.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}

namespace Streaming
{
namespace
{
/// Check if we have GROUP BY and / or aggregates function
/// We allow aggregate without group by like `SELECT count() FROM device_utils`
/// We also allow GROUP BY without aggregate like `SELECT device FROM device_utils GROUP BY device`
bool hasAggregates(const ASTPtr & query, const ASTSelectQuery & select_query)
{
    GetAggregatesVisitor::Data data;
    GetAggregatesVisitor(data).visit(query);

    return !data.aggregates.empty() || select_query.groupBy() != nullptr;
}
}

EmitInterpreter::LastXRule::LastXRule(const Settings & settings_, Poco::Logger * log_) : settings(settings_), log(log_)
{
}

void EmitInterpreter::LastXRule::operator()(ASTPtr & query_)
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

    /// FIXME, for now, we may be don't need this syntax `LAST 1h ON PROC TIME`
    proc_time = emit->proc_time;

    /// The order of window aggr / global aggr / tail matters
    if (handleWindowAggr(*select_query))
        return;

    if (handleGlobalAggr(*select_query))
        return;

    handleTail(*select_query);
}

bool EmitInterpreter::LastXRule::handleWindowAggr(ASTSelectQuery & select_query)
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

    if (!proc_time)
        addEventTimePredicate(select_query);

    auto window_interval_bs = BaseScaleInterval::toBaseScale(extractInterval(interval_ast->as<ASTFunction>()));
    auto last_interval_bs = BaseScaleInterval::toBaseScale(extractInterval(last_interval->as<ASTFunction>()));
    if (window_interval_bs.scale != last_interval_bs.scale)
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "Cannot convert between win interval '{}' and last interval '{}'",
            IntervalKind(window_interval_bs.src_kind).toString(),
            IntervalKind(last_interval_bs.src_kind).toString());

    select_query.setExpression(ASTSelectQuery::Expression::EMIT, std::move(new_emit));

    if (log)
        LOG_INFO(log, "(LastXForWindow) processed query: {}", queryToString(query, true));

    return true;
}

bool EmitInterpreter::LastXRule::handleGlobalAggr(ASTSelectQuery & select_query)
{
    assert(emit_query);
    assert(last_interval);

    /// FIXME: If the global aggr has several hist tables, could foreach all tables to convert ?
    /// Example:
    ///     table1, table2                  -> hop(table1, ...), hop(table2, ...)
    ///     subquery1, subquery2            -> hop(subquery1, ...), hop(subquery2, ...)
    ///     table(table1), table(table2)      -> hop(table(table1), ...), hop(table(table2), ...)
    auto table_expressions{getTableExpressions(select_query)};
    if (table_expressions.size() > 1)
        throw Exception("No support several tables in the `emit last` policy", ErrorCodes::SYNTAX_ERROR);

    auto table_expression = table_expressions[0];
    if (!hasAggregates(query, select_query) || !table_expression)
        return false;

    ASTPtr new_emit = emit_query->clone();
    auto new_emit_query = new_emit->as<ASTEmitQuery>();
    assert(new_emit_query);
    new_emit_query->last_interval.reset();

    ASTPtr periodic_interval;
    if (new_emit_query->periodic_interval)
        periodic_interval = std::move(new_emit_query->periodic_interval);
    else
        periodic_interval = makeASTInterval(ProtonConsts::DEFAULT_PERIODIC_INTERVAL.first, ProtonConsts::DEFAULT_PERIODIC_INTERVAL.second);

    /// check periodic_interval is appropriate value by settings.max_windows
    auto last_interval_bs = BaseScaleInterval::toBaseScale(extractInterval(last_interval->as<ASTFunction>()));
    auto periodic_interval_bs = BaseScaleInterval::toBaseScale(extractInterval(periodic_interval->as<ASTFunction>()));
    if (periodic_interval_bs.scale != last_interval_bs.scale)
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "Cannot convert between periodic interval '{}' and last interval '{}'",
            IntervalKind(periodic_interval_bs.src_kind).toString(),
            IntervalKind(last_interval_bs.src_kind).toString());

    UInt64 keep_windows = (std::abs(last_interval_bs.num_units) + std::abs(periodic_interval_bs.num_units) - 1)
        / std::abs(periodic_interval_bs.num_units);
    if (keep_windows == 0 || keep_windows > settings.max_windows)
        throw Exception(
            "Too big range or too small emit interval. Make sure 'range / emit_interval' is less or equal to "
                + std::to_string(settings.max_windows),
            ErrorCodes::SYNTAX_ERROR);

    /// To keep same scale between last interval and periodic interval.
    convertToSameKindIntervalAST(periodic_interval_bs, last_interval_bs, periodic_interval, last_interval);

    ASTPtr table;
    if (table_expression->database_and_table_name)
        table = table_expression->database_and_table_name->as<ASTTableIdentifier &>().clone();
    else if (table_expression->table_function)
        table = table_expression->table_function;
    else if (table_expression->subquery)
        table = table_expression->subquery;
    else
        throw Exception("The stream is empty", ErrorCodes::SYNTAX_ERROR);

    /// Create a table function: hop(table_expression, now(), periodic_interval, last_time_interval)
    /// The table_expression can be table, table(table) and subquery.
    auto table_expr = std::make_shared<ASTTableExpression>();
    ASTPtr timestamp_expr;
    if (proc_time)
    {
        auto scale = getAutoScaleByInterval(periodic_interval_bs.num_units, periodic_interval_bs.scale);
        if (scale == 0)
            timestamp_expr = makeASTFunction("now", std::make_shared<ASTLiteral>("UTC"));
        else
            timestamp_expr = makeASTFunction("now64", std::make_shared<ASTLiteral>(scale), std::make_shared<ASTLiteral>("UTC"));
    }
    else
        timestamp_expr = std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_EVENT_TIME);

    table_expr->table_function = makeASTFunction("hop", table, std::move(timestamp_expr), periodic_interval, last_interval);

    table_expr->children.emplace_back(table_expr->table_function);
    auto element = std::make_shared<ASTTablesInSelectQueryElement>();
    element->table_expression = table_expr;
    element->children.emplace_back(element->table_expression);
    auto new_table = std::make_shared<ASTTablesInSelectQuery>();
    new_table->children.emplace_back(element);

    /// We will need add `_tp_time > now64(3,'UTC') - last_interval` to WHERE
    /// Global window is always translated to hop proctime processing with event time filtering
    addEventTimePredicate(select_query);

    /// we add 'window_end' into groupby.
    ASTPtr new_groupby = select_query.groupBy() ? select_query.groupBy()->clone() : std::make_shared<ASTExpressionList>();
    auto new_groupby_list = new_groupby->as<ASTExpressionList>();
    assert(new_groupby_list);
    new_groupby_list->children.push_back(std::make_shared<ASTIdentifier>(ProtonConsts::STREAMING_WINDOW_END));

    select_query.setExpression(ASTSelectQuery::Expression::TABLES, std::move(new_table));
    select_query.setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(new_groupby));
    select_query.setExpression(ASTSelectQuery::Expression::EMIT, std::move(new_emit));

    if (log)
        LOG_INFO(log, "(LastXForGlobal) processed query: {}", queryToString(query, true));

    return true;
}

void EmitInterpreter::LastXRule::handleTail(ASTSelectQuery & select_query)
{
    assert(last_interval);
    assert(emit_query);

    if (!proc_time)
        /// We will need add `_tp_time > now64(3,'UTC') - last_interval` to WHERE
        addEventTimePredicate(select_query);

    ASTPtr new_emit = emit_query->clone();
    auto new_emit_query = new_emit->as<ASTEmitQuery>();
    assert(new_emit_query);
    new_emit_query->last_interval.reset();

    select_query.setExpression(ASTSelectQuery::Expression::EMIT, std::move(new_emit));

    if (log)
        LOG_INFO(log, "(LastXForTail) processed query: {}", queryToString(query, true));
}

/// Add `_tp_time >= now64(3, 'UTC') - last_interval` to WHERE clause, to do two things:
/// 1) Proton seeks streaming storage back to `now64(3, 'UTC') - @last_interval` to backfill the 1 hour data.
///  (this now64(3, 'UTC') is materialized when query is issued)
/// 2) Proton filters data by using `_tp_time > now64(3, 'UTC') - @last_interval`, now() is streaming processed.
///  (this filter is also continuously applied to new data)
void EmitInterpreter::LastXRule::addEventTimePredicate(ASTSelectQuery & select_query) const
{
    auto now = makeASTFunction("now64", std::make_shared<ASTLiteral>(UInt64(3)), std::make_shared<ASTLiteral>("UTC"));
    auto minus = makeASTFunction("minus", now, last_interval);
    auto greater = makeASTFunction("greater_or_equals", std::make_shared<ASTIdentifier>(ProtonConsts::RESERVED_EVENT_TIME), minus);

    auto where = select_query.where();
    if (!where)
        /// 1. If where clause is empty, then add `WHERE _tp_time >= now64(3, 'UTC') - 1h
        select_query.setExpression(ASTSelectQuery::Expression::WHERE, greater);
    else
        /// 2. If where clause is already there , then add `WHERE (existing predicates) AND (_tp_time >= now64(3, 'UTC') - 1h)
        select_query.setExpression(ASTSelectQuery::Expression::WHERE, makeASTFunction("and", where, greater));
}

void EmitInterpreter::checkEmitAST(ASTPtr & query)
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

    if (emit->timeout_interval)
        checkIntervalAST(emit->timeout_interval, "Invalid EMIT TIMEOUT interval");
}

}
}
