#include <Interpreters/PredicateExpressionsOptimizer.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExtractExpressionInfoVisitor.h>
#include <Interpreters/PredicateRewriteVisitor.h>
#include <Interpreters/getTableExpressions.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>

/// proton: starts.
#include <Interpreters/Streaming/RewriteAsSubquery.h>
#include <Interpreters/Streaming/WindowCommon.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSubquery.h>
#include <Storages/StorageView.h>
#include <TableFunctions/TableFunctionFactory.h>
/// proton: ends.

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PredicateExpressionsOptimizer::PredicateExpressionsOptimizer(
    ContextPtr context_, const TablesWithColumns & tables_with_columns_, const Settings & settings)
    : WithContext(context_)
    , enable_optimize_predicate_expression(settings.enable_optimize_predicate_expression)
    , enable_optimize_predicate_expression_to_final_subquery(settings.enable_optimize_predicate_expression_to_final_subquery)
    , allow_push_predicate_when_subquery_contains_with(settings.allow_push_predicate_when_subquery_contains_with)
    , tables_with_columns(tables_with_columns_)
{
}

/// proton: starts.
bool PredicateExpressionsOptimizer::optimize(ASTSelectQuery & select_query, ASTPtr & optimized_proxy_stream_query)
/// proton: ends.
{
    if (!enable_optimize_predicate_expression)
        return false;

    /// proton: starts.
    if (select_query.where())
        tryMovePredicatesFromWhereToHaving(select_query);
    /// proton: ends.

    if (select_query.having() && (!select_query.group_by_with_cube && !select_query.group_by_with_rollup && !select_query.group_by_with_totals))
        tryMovePredicatesFromHavingToWhere(select_query);

    if (!select_query.tables() || select_query.tables()->children.empty())
        return false;

    if ((!select_query.where() && !select_query.prewhere()) || select_query.arrayJoinExpressionList().first)
        return false;

    const auto & tables_predicates = extractTablesPredicates(select_query.where(), select_query.prewhere());

    if (!tables_predicates.empty())
        /// proton: starts.
        return tryRewritePredicatesToTables(select_query.refTables()->children, tables_predicates, optimized_proxy_stream_query);
        /// proton: ends.

    return false;
}

static ASTs splitConjunctionPredicate(const std::initializer_list<const ASTPtr> & predicates)
{
    std::vector<ASTPtr> res;

    for (const auto & predicate : predicates)
    {
        if (!predicate)
            continue;

        res.emplace_back(predicate);

        for (size_t idx = 0; idx < res.size();)
        {
            ASTPtr expression = res.at(idx);

            if (const auto * function = expression->as<ASTFunction>(); function && function->name == "and")
            {
                res.erase(res.begin() + idx);

                for (auto & child : function->arguments->children)
                    res.emplace_back(child);

                continue;
            }
            ++idx;
        }
    }

    return res;
}

std::vector<ASTs> PredicateExpressionsOptimizer::extractTablesPredicates(const ASTPtr & where, const ASTPtr & prewhere)
{
    std::vector<ASTs> tables_predicates(tables_with_columns.size());

    for (const auto & predicate_expression : splitConjunctionPredicate({where, prewhere}))
    {
        ExpressionInfoVisitor::Data expression_info{WithContext{getContext()}, tables_with_columns};
        ExpressionInfoVisitor(expression_info).visit(predicate_expression);

        if (expression_info.is_stateful_function
            || !expression_info.is_deterministic_function
            || expression_info.is_window_function)
        {
            return {};   /// Not optimized when predicate contains stateful function or indeterministic function or window functions
        }

        /// proton: starts. Skip pushdown predicate include `window_start` or `window_end` (added by table function)
        if (expression_info.is_window_start_or_end_function)
            continue;
        /// proton: ends.

        if (!expression_info.is_array_join)
        {
            if (expression_info.unique_reference_tables_pos.size() == 1)
                tables_predicates[*expression_info.unique_reference_tables_pos.begin()].emplace_back(predicate_expression);
            else if (expression_info.unique_reference_tables_pos.empty())
            {
                for (auto & predicate : tables_predicates)
                    predicate.emplace_back(predicate_expression);
            }
        }
    }

    return tables_predicates;    /// everything is OK, it can be optimized
}

/// proton: starts.
bool PredicateExpressionsOptimizer::tryRewritePredicatesToTables(ASTs & tables_element, const std::vector<ASTs> & tables_predicates, ASTPtr & optimized_proxy_stream_query)
/// proton: ends.
{
    bool is_rewrite_tables = false;

    if (tables_element.size() != tables_predicates.size())
        throw Exception("Unexpected elements count in predicate push down: `set enable_optimize_predicate_expression = 0` to disable",
                        ErrorCodes::LOGICAL_ERROR);

    for (size_t index = tables_element.size(); index > 0; --index)
    {
        size_t table_pos = index - 1;

        /// NOTE: the syntactic way of pushdown has limitations and should be partially disabled in case of JOINs.
        ///       Let's take a look at the query:
        ///
        ///           SELECT a, b FROM (SELECT 1 AS a) ANY LEFT JOIN (SELECT 1 AS a, 1 AS b) USING (a) WHERE b = 0
        ///
        ///       The result is empty - without pushdown. But the pushdown tends to modify it in this way:
        ///
        ///           SELECT a, b FROM (SELECT 1 AS a) ANY LEFT JOIN (SELECT 1 AS a, 1 AS b WHERE b = 0) USING (a) WHERE b = 0
        ///
        ///       That leads to the empty result in the right subquery and changes the whole outcome to (1, 0) or (1, NULL).
        ///       It happens because the not-matching columns are replaced with a global default values on JOIN.
        ///       Same is true for RIGHT JOIN and FULL JOIN.

        if (const auto & table_element = tables_element[table_pos]->as<ASTTablesInSelectQueryElement>())
        {
            if (table_element->table_join && isLeft(table_element->table_join->as<ASTTableJoin>()->kind))
                continue;  /// Skip right table optimization

            if (table_element->table_join && isFull(table_element->table_join->as<ASTTableJoin>()->kind))
                break;  /// Skip left and right table optimization

            /// proton: starts. Allow push down predicates for ProxyStream
            if (table_element->table_expression && table_element->table_expression->as<ASTTableExpression &>().table_function)
            {
                is_rewrite_tables |= tryRewritePredicatesToTableFunction(
                    table_element->table_expression->as<ASTTableExpression &>(),
                    tables_predicates[table_pos],
                    tables_with_columns[table_pos],
                    /*is_left*/ table_pos == 0,
                    optimized_proxy_stream_query);
            }
            else
                is_rewrite_tables |= tryRewritePredicatesToTable(tables_element[table_pos], tables_predicates[table_pos],
                    tables_with_columns[table_pos]);
            /// proton: ends.

            if (table_element->table_join && isRight(table_element->table_join->as<ASTTableJoin>()->kind))
                break;  /// Skip left table optimization
        }
    }

    return is_rewrite_tables;
}

bool PredicateExpressionsOptimizer::tryRewritePredicatesToTable(ASTPtr & table_element, const ASTs & table_predicates, const TableWithColumnNamesAndTypes & table_columns) const
{
    if (!table_predicates.empty())
    {
        auto optimize_final = enable_optimize_predicate_expression_to_final_subquery;
        auto optimize_with = allow_push_predicate_when_subquery_contains_with;
        PredicateRewriteVisitor::Data data(getContext(), table_predicates, table_columns, optimize_final, optimize_with);

        PredicateRewriteVisitor(data).visit(table_element);
        return data.is_rewrite;
    }

    return false;
}

bool PredicateExpressionsOptimizer::tryMovePredicatesFromHavingToWhere(ASTSelectQuery & select_query)
{
    ASTs where_predicates;
    ASTs having_predicates;

    const auto & reduce_predicates = [&](const ASTs & predicates)
    {
        ASTPtr res = predicates[0];
        for (size_t index = 1; index < predicates.size(); ++index)
            res = makeASTFunction("and", res, predicates[index]);

        return res;
    };

    for (const auto & moving_predicate: splitConjunctionPredicate({select_query.having()}))
    {
        TablesWithColumns tables;
        ExpressionInfoVisitor::Data expression_info{WithContext{getContext()}, tables};
        ExpressionInfoVisitor(expression_info).visit(moving_predicate);

        /// TODO: If there is no group by, where, and prewhere expression, we can push down the stateful function
        if (expression_info.is_stateful_function)
            return false;

        if (expression_info.is_window_function)
        {
            // Window functions are not allowed in either HAVING or WHERE.
            return false;
        }

        /// proton: starts.
        /// For hop/session window, window_start/window_end are not generated before aggregation step for performance reasons.
        /// The having statement which contains window_start/window_end could not be moved to where clause.
        if (expression_info.is_window_start_or_end_function)
        {
            if (auto * table_expression = getTableExpression(select_query, 0); table_expression && table_expression->table_function)
            {
                const auto * func = table_expression->table_function->as<ASTFunction>();
                if (Streaming::isTableFunctionHop(func) || Streaming::isTableFunctionSession(func))
                    return false;
            }
        }
        /// proton: ends.

        if (expression_info.is_aggregate_function)
            having_predicates.emplace_back(moving_predicate);
        else
            where_predicates.emplace_back(moving_predicate);
    }

    if (having_predicates.empty())
        select_query.setExpression(ASTSelectQuery::Expression::HAVING, {});
    else
    {
        auto having_predicate = reduce_predicates(having_predicates);
        select_query.setExpression(ASTSelectQuery::Expression::HAVING, std::move(having_predicate));
    }

    if (!where_predicates.empty())
    {
        auto moved_predicate = reduce_predicates(where_predicates);
        moved_predicate = select_query.where() ? makeASTFunction("and", select_query.where(), moved_predicate) : moved_predicate;
        select_query.setExpression(ASTSelectQuery::Expression::WHERE, std::move(moved_predicate));
    }

    return true;
}

/// proton: starts.
/// For hop/session window, window_start/window_end are not generated before aggregation step for performance reasons
/// The predication statement which contains window_start/window_end need to be moved to having clause.
bool PredicateExpressionsOptimizer::tryMovePredicatesFromWhereToHaving(ASTSelectQuery & select_query)
{
    ASTs where_predicates;
    ASTs having_predicates;

    const auto & reduce_predicates = [&](const ASTs & predicates) {
        ASTPtr res = predicates[0];
        for (size_t index = 1; index < predicates.size(); ++index)
            res = makeASTFunction("and", res, predicates[index]);

        return res;
    };

    for (const auto & moving_predicate : splitConjunctionPredicate({select_query.where()}))
    {
        TablesWithColumns tables;
        ExpressionInfoVisitor::Data expression_info{WithContext{getContext()}, tables};
        ExpressionInfoVisitor(expression_info).visit(moving_predicate);

        if (expression_info.is_window_start_or_end_function)
        {
            if (auto * table_expression = getTableExpression(select_query, 0); table_expression && table_expression->table_function)
            {
                const auto * func = table_expression->table_function->as<ASTFunction>();
                if (Streaming::isTableFunctionHop(func) || Streaming::isTableFunctionSession(func))
                {
                    having_predicates.emplace_back(moving_predicate);
                    continue;
                }
            }
        }

        where_predicates.emplace_back(moving_predicate);
    }

    if (!having_predicates.empty())
    {
        auto moved_predicate = reduce_predicates(having_predicates);
        moved_predicate = select_query.having() ? makeASTFunction("and", select_query.having(), moved_predicate) : moved_predicate;
        select_query.setExpression(ASTSelectQuery::Expression::HAVING, std::move(moved_predicate));

        if (where_predicates.empty())
            select_query.setExpression(ASTSelectQuery::Expression::WHERE, {});
        else
        {
            auto where_predicate = reduce_predicates(where_predicates);
            select_query.setExpression(ASTSelectQuery::Expression::WHERE, std::move(where_predicate));
        }
        return true;
    }

    return false;
}

bool PredicateExpressionsOptimizer::tryRewritePredicatesToTableFunction(
    ASTTableExpression & table_expression,
    const ASTs & table_predicates,
    const TableWithColumnNamesAndTypes & table_columns,
    bool is_left,
    ASTPtr & optimized_proxy_stream_query) const
{
    assert(table_expression.table_function);
    if (table_predicates.empty())
        return false;

    ASTFunction * table_func = table_expression.table_function->as<ASTFunction>();
    if (!TableFunctionFactory::instance().isSupportSubqueryTableFunctionName(table_func->name))
        return false;

    /// NOTE: Cannot push down predicates to table function `changelog(...)`, since the results may be inconsistent.
    /// For example:
    ///     with cte as (select id, status from stream) select * from changelog(cte, id) where status = 1;
    ///     (id, status)  <<  (1, 1) (1, 0)
    /// Without pushdown results:
    ///     id      status     _tp_delta
    ///     1           1           1
    ///     1           1           -1
    /// But pushdown results:
    ///     id      status     _tp_delta
    ///     1           1           1
    /// In addition, `changelog(...)` always seek to earliest, so even if there are event predicates, still works without pushdown
    if (Streaming::isTableFunctionChangelog(table_func))
        return false;

    /// Only can push down predicates to proxy subquery or view
    /// Firstly, try extract proxy subquery from table function
    ASTPtr proxy_subquery;
    ASTPtr table_ast;
    do
    {
        assert(table_func->arguments->children.size() >= 1);
        table_ast = table_func->arguments->children[0];
        if (table_ast->as<ASTSubquery>())
        {
            proxy_subquery = table_ast;
            break;
        }

        table_func = table_ast->as<ASTFunction>();
        /// Try extract subquery for storage view
        if (!table_func)
        {
            auto storage_id = tryGetStorageID(table_ast);
            assert(storage_id.has_value());
            if (storage_id.value().database_name.empty())
                storage_id.value().database_name = getContext()->getCurrentDatabase();

            if (auto storage = DatabaseCatalog::instance().getTable(*storage_id, getContext()); storage->as<StorageView>())
            {
                proxy_subquery = std::make_shared<ASTSubquery>();
                proxy_subquery->children.emplace_back(storage->getInMemoryMetadataPtr()->getSelectQuery().inner_query->clone());
                break;
            }
        }
    } while (table_func);

    if (!proxy_subquery)
        return false;

    PredicateRewriteVisitor::Data data(
        getContext(),
        table_predicates,
        table_columns,
        enable_optimize_predicate_expression_to_final_subquery,
        allow_push_predicate_when_subquery_contains_with);

    if (is_left)
    {
        /// For left table function, save rewritten proxy stream query
        PredicateRewriteVisitor(data).visit(proxy_subquery);
        /// The `optimized_proxy_stream_query` will be used for `ProxyStream::read(...)`
        optimized_proxy_stream_query = data.is_rewrite ? proxy_subquery->as<ASTSubquery &>().children[0] : nullptr;
        return false; /// Always return false (no need to analyze again) even if the original left table ast is written, but it doesn't be used
    }
    else
    {
        /// For right table function, rewrite as subquery
        /// For exmaple:
        /// `WITH cte as (select i from t) SELECT * FROM t1 JOIN tumble(cte, ...) as t2 where t2.i > 1`
        /// => `WITH cte as (select i from t) SELECT * FROM t1 JOIN (select * from tumble(cte, ...) where i > 1) as t2 where t2.i > 1`
        /// Next, we can do further pushdown optimization in joined subquery (Same as above for left table function)
        auto subquery = Streaming::rewriteAsSubquery(table_expression);
        PredicateRewriteVisitor(data).visit(subquery);
        return true; /// Rewritten the original right table ast
    }
}
/// proton: ends.

}
