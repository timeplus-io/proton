#include <Columns/ColumnsNumber.h>
#include <Core/ShuffleDescription.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LightShufflingStep.h>
#include <Processors/QueryPlan/Streaming/SubstreamShufflingStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{
Block makeHeader()
{
    Block header;
    header.insert({std::make_shared<DataTypeUInt64>()->createColumn(), std::make_shared<DataTypeUInt64>(), "k"});
    header.insert({std::make_shared<DataTypeUInt64>()->createColumn(), std::make_shared<DataTypeUInt64>(), "v"});
    return header;
}

class ProbeTransformingStep : public ITransformingStep
{
public:
    ProbeTransformingStep(const DataStream & input, bool preserves_shuffling)
        : ITransformingStep(input, input.header, makeTraits(preserves_shuffling))
    {
    }

    String getName() const override { return "ProbeTransformingStep"; }
    void transformPipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override {}

private:
    static Traits makeTraits(bool preserves_shuffling)
    {
        return {
            {
                .preserves_distinct_columns = true,
                .returns_single_stream = false,
                .preserves_number_of_streams = true,
                .preserves_sorting = true,
                .preserves_shuffling = preserves_shuffling,
            },
            {.preserves_number_of_rows = true},
        };
    }

    void updateOutputStream() override
    {
        output_stream = createOutputStream(input_streams.front(), input_streams.front().header, getDataStreamTraits());
    }
};
}

TEST(ShuffleDescription, LightShufflingStepPopulatesDescription)
{
    DataStream input{.header = makeHeader()};
    EXPECT_FALSE(input.shuffle_description.has_value());

    LightShufflingStep step(input, /*keys_=*/{"k"}, /*max_num_outputs_=*/4);
    const auto & out = step.getOutputStream();
    ASSERT_TRUE(out.shuffle_description.has_value());
    EXPECT_EQ(out.shuffle_description->kind, ShuffleDescription::Kind::Light);
    EXPECT_EQ(out.shuffle_description->keys, Names{"k"});
}

TEST(ShuffleDescription, SubstreamShufflingStepPopulatesDescription)
{
    DataStream input{.header = makeHeader()};

    Streaming::SubstreamShufflingStep step(input, /*keys_=*/{"k"}, /*max_thread_=*/4);
    const auto & out = step.getOutputStream();
    EXPECT_TRUE(out.hasSubstream());
    ASSERT_TRUE(out.shuffle_description.has_value());
    EXPECT_EQ(out.shuffle_description->kind, ShuffleDescription::Kind::Substream);
    EXPECT_EQ(out.shuffle_description->keys, Names{"k"});
}

TEST(ShuffleDescription, PreservesShufflingCarriesDescription)
{
    DataStream input{.header = makeHeader()};
    input.shuffle_description = ShuffleDescription{ShuffleDescription::Kind::Light, {"k"}};

    ProbeTransformingStep step(input, /*preserves_shuffling=*/true);
    ASSERT_TRUE(step.getOutputStream().shuffle_description.has_value());
    EXPECT_EQ(step.getOutputStream().shuffle_description->kind, ShuffleDescription::Kind::Light);
    EXPECT_EQ(step.getOutputStream().shuffle_description->keys, Names{"k"});
}

TEST(ShuffleDescription, NonPreservingStepDropsDescription)
{
    DataStream input{.header = makeHeader()};
    input.shuffle_description = ShuffleDescription{ShuffleDescription::Kind::Light, {"k"}};

    ProbeTransformingStep step(input, /*preserves_shuffling=*/false);
    EXPECT_FALSE(step.getOutputStream().shuffle_description.has_value());
}

TEST(ShuffleDescription, KeysCoveredByChecksSubsetOnly)
{
    using Kind = ShuffleDescription::Kind;

    /// Shuffle keys ⊆ consumer keys: covered (kind-agnostic; Substream subsumes Light).
    EXPECT_TRUE((ShuffleDescription{Kind::Light, {"a", "b"}}.keysCoveredBy({"a", "b", "c"})));
    EXPECT_TRUE((ShuffleDescription{Kind::Light, {"a"}}.keysCoveredBy({"a"})));
    EXPECT_TRUE((ShuffleDescription{Kind::Substream, {"a"}}.keysCoveredBy({"a", "b"})));

    /// Shuffle keys ⊄ consumer keys: not covered.
    EXPECT_FALSE((ShuffleDescription{Kind::Light, {"a", "b"}}.keysCoveredBy({"a"})));
    EXPECT_FALSE((ShuffleDescription{Kind::Light, {"a"}}.keysCoveredBy({"b"})));
    EXPECT_FALSE((ShuffleDescription{Kind::Substream, {"a"}}.keysCoveredBy({})));
}

TEST(ShuffleDescription, PropagationChainSurvivesMultipleSteps)
{
    DataStream input{.header = makeHeader()};

    LightShufflingStep shuffle(input, /*keys_=*/{"k"}, /*max_num_outputs_=*/4);
    ProbeTransformingStep watermark_like(shuffle.getOutputStream(), /*preserves_shuffling=*/true);
    ProbeTransformingStep expression_like(watermark_like.getOutputStream(), /*preserves_shuffling=*/true);

    ASSERT_TRUE(expression_like.getOutputStream().shuffle_description.has_value());
    EXPECT_EQ(expression_like.getOutputStream().shuffle_description->kind, ShuffleDescription::Kind::Light);
    EXPECT_EQ(expression_like.getOutputStream().shuffle_description->keys, Names{"k"});
}

TEST(ShuffleDescription, ExpressionStepPreservesIdentityShuffleKeys)
{
    DataStream input{.header = makeHeader()};
    input.shuffle_description = ShuffleDescription{ShuffleDescription::Kind::Light, {"k"}};

    auto actions = std::make_shared<ActionsDAG>(input.header.getColumnsWithTypeAndName());
    ExpressionStep step(input, actions);

    ASSERT_TRUE(step.getOutputStream().shuffle_description.has_value());
    EXPECT_EQ(step.getOutputStream().shuffle_description->kind, ShuffleDescription::Kind::Light);
    EXPECT_EQ(step.getOutputStream().shuffle_description->keys, Names{"k"});
}

TEST(ShuffleDescription, ExpressionStepDropsReboundShuffleKeys)
{
    DataStream input{.header = makeHeader()};
    input.shuffle_description = ShuffleDescription{ShuffleDescription::Kind::Light, {"k"}};

    auto actions = std::make_shared<ActionsDAG>(input.header.getColumnsWithTypeAndName());
    const auto & rebound_k = actions->addAlias(actions->findInOutputs("v"), "k");
    actions->getOutputs().clear();
    actions->getOutputs().push_back(&rebound_k);

    ExpressionStep step(input, actions);
    EXPECT_FALSE(step.getOutputStream().shuffle_description.has_value());
}

TEST(ShuffleDescription, JoinPreservesLeftShuffleOnlyForEnrichmentKinds)
{
    EXPECT_TRUE(joinPreservesLeftShuffle(JoinKind::Inner));
    EXPECT_TRUE(joinPreservesLeftShuffle(JoinKind::Left));
    EXPECT_TRUE(joinPreservesLeftShuffle(JoinKind::Cross));
    EXPECT_TRUE(joinPreservesLeftShuffle(JoinKind::Comma));

    /// Right/Full emit unmatched-right rows with NULL left keys → must drop.
    EXPECT_FALSE(joinPreservesLeftShuffle(JoinKind::Right));
    EXPECT_FALSE(joinPreservesLeftShuffle(JoinKind::Full));
}
