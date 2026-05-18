#include <gtest/gtest.h>

#include <Columns/IColumn.h>
#include <Common/tests/gtest_global_context.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/Set.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <QueryPipeline/SizeLimits.h>
#include <Storages/PruneShardsDetail.h>

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
    detail::RewriteInSubqueriesForShardPruningResult result;
    detail::rewriteInSubqueriesForShardPruning(
        root,
        prepared_sets,
        getContext().context,
        /*subquery_depth=*/0,
        /*limit=*/1024,
        result,
        /*in_conjunctive_position=*/true);
    return result.has_empty_subquery_in_conjunctive_position;
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
