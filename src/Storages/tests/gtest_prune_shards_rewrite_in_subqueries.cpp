#include <gtest/gtest.h>

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/SizeLimits.h>
#include <Storages/PruneShardsDetail.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int TOO_DEEP_SUBQUERIES;
}
}

using namespace DB;

namespace
{

/// Registers a single empty Set under the tree-hash of an `ASTTableIdentifier` so that the
/// rewrite helper's `findStorage()` path resolves to it. This lets us deterministically trip
/// the empty-IN branch without spinning up a real subquery execution.
class EmptyInFixture
{
public:
    EmptyInFixture()
        : prepared_sets(std::make_shared<PreparedSets>())
        , table_identifier(std::make_shared<ASTTableIdentifier>("fake_empty_set"))
    {
        SizeLimits no_limits{0, 0, OverflowMode::THROW};
        auto empty_set = std::make_shared<Set>(no_limits, /*max_elements_to_fill=*/1, /*transform_null_in=*/false);

        ColumnsWithTypeAndName header{
            ColumnWithTypeAndName(std::make_shared<DataTypeInt32>(), "id"),
        };
        empty_set->setHeader(header);
        empty_set->fillSetElements();
        empty_set->finishInsert();

        prepared_sets->addFromStorage(table_identifier->getTreeHash(), std::move(empty_set));
    }

    PreparedSetsPtr prepared_sets;
    ASTPtr table_identifier;

    /// Returns a fresh `in(id, fake_empty_set)` AST whose right operand shares the registered hash.
    ASTPtr makeEmptyIn() const
    {
        ASTPtr id = std::make_shared<ASTIdentifier>("id");
        ASTPtr right = table_identifier->clone();
        return makeASTFunction("in", id, right);
    }
};

bool runRewrite(ASTPtr & root, const PreparedSetsPtr & prepared_sets)
{
    DB::detail::RewriteInSubqueriesForShardPruningResult result;
    DB::detail::rewriteInSubqueriesForShardPruning(
        root,
        prepared_sets,
        getContext().context,
        /*subquery_depth=*/0,
        /*limit=*/1024,
        result,
        /*in_conjunctive_position=*/true);
    return result.has_empty_subquery_in_conjunctive_position;
}

ContextMutablePtr makePruningContext()
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    context->setSetting("use_index_for_in_with_subqueries", Field{true});
    context->setSetting("query_mode", String{"table"});
    return context;
}

bool rewriteForShardPruning(
    ASTPtr & root,
    const PreparedSetsPtr & prepared_sets,
    const ContextPtr & context,
    size_t subquery_depth,
    size_t limit = 1024)
{
    DB::detail::RewriteInSubqueriesForShardPruningResult result;
    return DB::detail::rewriteInSubqueriesForShardPruning(
        root,
        prepared_sets,
        context,
        subquery_depth,
        limit,
        result,
        /*in_conjunctive_position=*/true);
}

bool rewriteForShardPruning(ASTPtr & root, const PreparedSetsPtr & prepared_sets, size_t limit = 1024)
{
    return rewriteForShardPruning(root, prepared_sets, makePruningContext(), /*subquery_depth=*/0, limit);
}

ASTPtr literalOne()
{
    return std::make_shared<ASTLiteral>(Field(static_cast<UInt64>(1)));
}

ASTPtr ident(const String & name)
{
    return std::make_shared<ASTIdentifier>(name);
}

ASTPtr andOf(ASTPtr a, ASTPtr b)
{
    return makeASTFunction("and", a, b);
}

ASTPtr selectQuery(const String & query)
{
    ParserSelectWithUnionQuery parser;
    return parseQuery(parser, query, /*max_query_size=*/0, /*max_parser_depth=*/0);
}

ASTPtr inWithSubquery(const String & query)
{
    return makeASTFunction("in", ident("id"), std::make_shared<ASTSubquery>(selectQuery(query)));
}

ASTPtr & inRhs(ASTPtr & node)
{
    return node->as<ASTFunction &>().arguments->children[1];
}

/// Generalized fixture used by the non-empty, NULL-element, and limit-exceeded tests.
/// The empty-IN tests above use the dedicated EmptyInFixture instead -- keeping that
/// fixture untouched avoids churn in its assertions.
struct PreparedFixture
{
    PreparedSetsPtr prepared_sets;
    ASTPtr table_identifier;

    ASTPtr makeIn() const
    {
        ASTPtr id = std::make_shared<ASTIdentifier>("id");
        ASTPtr right = table_identifier->clone();
        return makeASTFunction("in", id, right);
    }
};

PreparedFixture makeIntInFixture(const String & name, std::initializer_list<Int32> values)
{
    PreparedFixture f;
    f.prepared_sets = std::make_shared<PreparedSets>();
    f.table_identifier = std::make_shared<ASTTableIdentifier>(name);

    SizeLimits no_limits{0, 0, OverflowMode::THROW};
    auto set = std::make_shared<Set>(no_limits, /*max_elements_to_fill=*/1024, /*transform_null_in=*/false);

    auto type = std::make_shared<DataTypeInt32>();
    ColumnsWithTypeAndName header{ColumnWithTypeAndName(type, "id")};
    set->setHeader(header);
    set->fillSetElements();

    if (values.size() > 0)
    {
        auto col = type->createColumn();
        for (Int32 v : values)
            col->insert(Field(static_cast<Int64>(v)));
        ColumnsWithTypeAndName block{ColumnWithTypeAndName(std::move(col), type, "id")};
        set->insertFromBlock(block);
    }
    set->finishInsert();

    f.prepared_sets->addFromStorage(f.table_identifier->getTreeHash(), std::move(set));
    return f;
}

/// Build a Set keyed by `Nullable(Int32)` containing the values {NULL, 1}.
/// Requires `transform_null_in=true` so NULL is stored as a real set member instead
/// of being filtered at insert time -- otherwise the rewrite path would never see a NULL.
PreparedFixture makeNullableIntInFixture(const String & name)
{
    PreparedFixture f;
    f.prepared_sets = std::make_shared<PreparedSets>();
    f.table_identifier = std::make_shared<ASTTableIdentifier>(name);

    SizeLimits no_limits{0, 0, OverflowMode::THROW};
    auto set = std::make_shared<Set>(no_limits, /*max_elements_to_fill=*/1024, /*transform_null_in=*/true);

    auto nullable_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>());
    ColumnsWithTypeAndName header{ColumnWithTypeAndName(nullable_type, "id")};
    set->setHeader(header);
    set->fillSetElements();

    auto col = nullable_type->createColumn();
    col->insert(Null{});
    col->insert(Field(static_cast<Int64>(1)));
    ColumnsWithTypeAndName block{ColumnWithTypeAndName(std::move(col), nullable_type, "id")};
    set->insertFromBlock(block);
    set->finishInsert();

    f.prepared_sets->addFromStorage(f.table_identifier->getTreeHash(), std::move(set));
    return f;
}

struct RewriteOutcome
{
    bool changed;
    bool empty_in_conjunctive_position;
};

RewriteOutcome runRewriteFull(ASTPtr & root, const PreparedSetsPtr & prepared_sets, size_t limit = 1024)
{
    DB::detail::RewriteInSubqueriesForShardPruningResult result;
    bool changed = DB::detail::rewriteInSubqueriesForShardPruning(
        root,
        prepared_sets,
        getContext().context,
        /*subquery_depth=*/0,
        limit,
        result,
        /*in_conjunctive_position=*/true);
    return {changed, result.has_empty_subquery_in_conjunctive_position};
}

}

TEST(PruneShardsRewriteInSubqueries, EmptyInAtRootFlagsConjunctive)
{
    EmptyInFixture fixture;
    ASTPtr root = fixture.makeEmptyIn();
    EXPECT_TRUE(runRewrite(root, fixture.prepared_sets));
}

TEST(PruneShardsRewriteInSubqueries, EmptyInUnderAndFlagsConjunctive)
{
    EmptyInFixture fixture;
    ASTPtr root = andOf(ident("a"), fixture.makeEmptyIn());
    EXPECT_TRUE(runRewrite(root, fixture.prepared_sets));
}

TEST(PruneShardsRewriteInSubqueries, EmptyInUnderNestedAndFlagsConjunctive)
{
    EmptyInFixture fixture;
    ASTPtr inner_and = andOf(ident("y"), fixture.makeEmptyIn());
    ASTPtr root = andOf(ident("x"), inner_and);
    EXPECT_TRUE(runRewrite(root, fixture.prepared_sets));
}

TEST(PruneShardsRewriteInSubqueries, MultipleEmptyInsUnderAndFlagsConjunctive)
{
    EmptyInFixture fixture;
    ASTPtr root = andOf(fixture.makeEmptyIn(), fixture.makeEmptyIn());
    EXPECT_TRUE(runRewrite(root, fixture.prepared_sets));
}

TEST(PruneShardsRewriteInSubqueries, EmptyInUnderIfDoesNotFlag)
{
    EmptyInFixture fixture;
    ASTPtr root = makeASTFunction("if", ident("flag"), fixture.makeEmptyIn(), literalOne());
    EXPECT_FALSE(runRewrite(root, fixture.prepared_sets));
}

TEST(PruneShardsRewriteInSubqueries, EmptyInUnderMultiIfDoesNotFlag)
{
    EmptyInFixture fixture;
    ASTPtr root = makeASTFunction("multi_if", ident("c1"), literalOne(), ident("c2"), fixture.makeEmptyIn(), literalOne());
    EXPECT_FALSE(runRewrite(root, fixture.prepared_sets));
}

TEST(PruneShardsRewriteInSubqueries, EmptyInUnderOrDoesNotFlag)
{
    EmptyInFixture fixture;
    ASTPtr root = makeASTFunction("or", ident("a"), fixture.makeEmptyIn());
    EXPECT_FALSE(runRewrite(root, fixture.prepared_sets));
}

TEST(PruneShardsRewriteInSubqueries, EmptyInUnderNotDoesNotFlag)
{
    EmptyInFixture fixture;
    ASTPtr root = makeASTFunction("not", fixture.makeEmptyIn());
    EXPECT_FALSE(runRewrite(root, fixture.prepared_sets));
}

TEST(PruneShardsRewriteInSubqueries, EmptyInUnderTupleDoesNotFlag)
{
    EmptyInFixture fixture;
    ASTPtr root = makeASTFunction("tuple", literalOne(), fixture.makeEmptyIn());
    EXPECT_FALSE(runRewrite(root, fixture.prepared_sets));
}

TEST(PruneShardsRewriteInSubqueries, EmptyInUnderEqualsDoesNotFlag)
{
    EmptyInFixture fixture;
    ASTPtr root = makeASTFunction("equals", fixture.makeEmptyIn(), literalOne());
    EXPECT_FALSE(runRewrite(root, fixture.prepared_sets));
}

TEST(PruneShardsRewriteInSubqueries, EmptyInUnderAndThenIfDoesNotFlag)
{
    /// `and(a, if(flag, empty_in, 1))` -- the IN is reachable from `and`, but the path
    /// goes through `if`, which can yield a truthy value for some rows.
    EmptyInFixture fixture;
    ASTPtr inner_if = makeASTFunction("if", ident("flag"), fixture.makeEmptyIn(), literalOne());
    ASTPtr root = andOf(ident("a"), inner_if);
    EXPECT_FALSE(runRewrite(root, fixture.prepared_sets));
}

TEST(PruneShardsRewriteInSubqueries, EmptyInsMixedConjunctiveAndNonConjunctiveFlagsViaConjunctiveOne)
{
    /// One IN is purely under `and`, another is under `if`. The conjunctive one is enough to
    /// short-circuit shard selection -- the other IN's position is independent.
    EmptyInFixture fixture;
    ASTPtr inner_if = makeASTFunction("if", ident("flag"), fixture.makeEmptyIn(), literalOne());
    ASTPtr root = andOf(fixture.makeEmptyIn(), inner_if);
    EXPECT_TRUE(runRewrite(root, fixture.prepared_sets));
}

TEST(PruneShardsRewriteInSubqueries, SubqueryPlanMaterializesExplicitSetForShardPruning)
{
    auto context = makePruningContext();
    auto prepared_sets = std::make_shared<PreparedSets>();

    auto column = ColumnUInt64::create();
    column->insertValue(42);
    Block block{{std::move(column), std::make_shared<DataTypeUInt64>(), "id"}};

    auto plan = std::make_unique<QueryPlan>();
    plan->addStep(std::make_unique<ReadFromPreparedSource>(Pipe(std::make_shared<SourceFromSingleChunk>(std::move(block)))));

    auto future_set = DB::detail::addSubqueryPlanForShardPruning(
        prepared_sets,
        PreparedSets::Hash{42, 7},
        std::move(plan),
        context,
        /*limit=*/1024);

    ASSERT_NE(future_set, nullptr);

    auto set = future_set->buildOrderedSetInplace(context);
    ASSERT_NE(set, nullptr);
    ASSERT_TRUE(set->hasExplicitSetElements());
    ASSERT_EQ(set->getTotalRowCount(), 1);

    const auto elements = set->getSetElements();
    ASSERT_EQ(elements.size(), 1);
    ASSERT_EQ(elements.front()->size(), 1);
    EXPECT_EQ((*elements.front())[0].safeGet<UInt64>(), 42);
}

TEST(PruneShardsRewriteInSubqueries, AstSubqueryDbExceptionFallsBackWithoutRewrite)
{
    auto prepared_sets = std::make_shared<PreparedSets>();
    ASTPtr root = inWithSubquery("select missing_identifier");

    EXPECT_FALSE(rewriteForShardPruning(root, prepared_sets));
    EXPECT_NE(inRhs(root)->as<ASTSubquery>(), nullptr);
}

TEST(PruneShardsRewriteInSubqueries, BuildSubqueryPlanForwardsSubqueryDepth)
{
    auto context = makePruningContext();
    context->setSetting("max_subquery_depth", Field{UInt64{50}});

    ASTPtr subquery = std::make_shared<ASTSubquery>(selectQuery("select 1 as id"));

    /// The incoming depth must reach interpretSubquery(). Correct forwarding makes
    /// this too deep; a regression to hardcoded depth=1 would build a plan instead.
    try
    {
        auto plan = DB::detail::buildSubqueryPlanForShardPruning(
            subquery, context, /*subquery_depth=*/50);
        FAIL() << "Expected TOO_DEEP_SUBQUERIES, got plan initialized=" << plan->isInitialized();
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::TOO_DEEP_SUBQUERIES);
    }
}

TEST(PruneShardsRewriteInSubqueries, StreamingPlanCannotBeMaterializedForShardPruning)
{
    QueryPlan historical_plan;
    historical_plan.addStep(std::make_unique<ReadNothingStep>(Block{}, /*is_streaming=*/false));
    EXPECT_TRUE(DB::detail::canMaterializeSubqueryForShardPruning(historical_plan));

    QueryPlan streaming_plan;
    streaming_plan.addStep(std::make_unique<ReadNothingStep>(Block{}, /*is_streaming=*/true));
    EXPECT_FALSE(DB::detail::canMaterializeSubqueryForShardPruning(streaming_plan));
}

TEST(PruneShardsRewriteInSubqueries, NonEmptyInRewritesToLiteralTuple)
{
    /// Bounded non-empty prepared set must be materialized into a literal tuple in place of the
    /// original ASTTableIdentifier so that shard pruning can evaluate the sharding key over it.
    auto fixture = makeIntInFixture("fake_int_set", {1, 2});
    ASTPtr root = fixture.makeIn();

    const auto outcome = runRewriteFull(root, fixture.prepared_sets);

    EXPECT_TRUE(outcome.changed);
    EXPECT_FALSE(outcome.empty_in_conjunctive_position);

    const auto * in_fn = root->as<ASTFunction>();
    ASSERT_NE(in_fn, nullptr);
    ASSERT_EQ(in_fn->name, "in");
    ASSERT_EQ(in_fn->arguments->children.size(), 2u);

    const auto * lit = in_fn->arguments->children[1]->as<ASTLiteral>();
    ASSERT_NE(lit, nullptr);
    ASSERT_EQ(lit->value.getType(), Field::Types::Tuple);

    const auto & tuple = lit->value.get<const Tuple &>();
    ASSERT_EQ(tuple.size(), 2u);
    EXPECT_EQ(tuple[0].safeGet<Int64>(), 1);
    EXPECT_EQ(tuple[1].safeGet<Int64>(), 2);
}

TEST(PruneShardsRewriteInSubqueries, NullElementLeavesRhsUnchanged)
{
    /// With transform_null_in=true on the Set, NULL is a real set element. The rewrite must
    /// bail out and leave the RHS as the original ASTTableIdentifier so the normal IN evaluation
    /// (which knows the NULL semantics) handles it.
    auto fixture = makeNullableIntInFixture("fake_nullable_set");
    ASTPtr root = fixture.makeIn();

    const auto outcome = runRewriteFull(root, fixture.prepared_sets);

    EXPECT_FALSE(outcome.changed);
    EXPECT_FALSE(outcome.empty_in_conjunctive_position);

    const auto * in_fn = root->as<ASTFunction>();
    ASSERT_NE(in_fn, nullptr);
    EXPECT_TRUE(in_fn->arguments->children[1]->as<ASTTableIdentifier>());
    EXPECT_FALSE(in_fn->arguments->children[1]->as<ASTLiteral>());
}

TEST(PruneShardsRewriteInSubqueries, BoundedNonEmptyInExceedingLimitDoesNotRewrite)
{
    /// `total_rows > limit` is the safety valve for `optimize_skip_unused_shards_limit`. With
    /// 3 values and limit=1, the rewrite must give up and leave the RHS untouched so the normal
    /// execution path keeps full correctness.
    auto fixture = makeIntInFixture("fake_overflow_set", {1, 2, 3});
    ASTPtr root = fixture.makeIn();

    const auto outcome = runRewriteFull(root, fixture.prepared_sets, /*limit=*/1);

    EXPECT_FALSE(outcome.changed);
    EXPECT_FALSE(outcome.empty_in_conjunctive_position);

    const auto * in_fn = root->as<ASTFunction>();
    ASSERT_NE(in_fn, nullptr);
    EXPECT_TRUE(in_fn->arguments->children[1]->as<ASTTableIdentifier>());
    EXPECT_FALSE(in_fn->arguments->children[1]->as<ASTLiteral>());
}
