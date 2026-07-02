#include <Interpreters/Streaming/Aggregator/HybridAggregator/HybridAggregator.h>
#include <Interpreters/Streaming/Aggregator/HybridAggregator/TrackingCount.h>

#include <DataTypes/DataTypeFactory.h>

namespace DB::Streaming
{

[[nodiscard]] bool HybridAggregator::executeWithoutKeyImpl(
    HybridAggregatedDataVariants & result,
    size_t row_begin,
    size_t row_end,
    AggregateFunctionInstruction * aggregate_instructions,
    bool tracking_retracts) const
{
    auto * place = result.without_key.get();

    /// Save current state for retraction before adding values
    if (tracking_retracts && !result.without_key_retracts)
    {
        result.initWithoutKeyRetractStates(total_size_of_aggregate_states, align_aggregate_states);
        auto * dst_place = result.without_key_retracts.get();
        copyAggregateStates(dst_place, place, /*arena=*/nullptr);
        TrackingCount::merge(dst_place + tracking_count_offset, place + tracking_count_offset);
    }

    /// Adding values
    bool should_finalize = false;
    for (size_t i = 0, func_size = aggregate_functions.size(); i < func_size; ++i)
    {
        AggregateFunctionInstruction * inst = aggregate_instructions + i;

        if (inst->offsets)
            inst->batch_that->addBatchSinglePlace(
                inst->offsets[static_cast<ssize_t>(row_begin) - 1],
                inst->offsets[row_end - 1],
                place + inst->state_offset,
                inst->batch_arguments,
                /*arena=*/nullptr,
                -1,
                inst->delta_column);
        else
            inst->batch_that->addBatchSinglePlace(
                row_begin,
                row_end,
                place + inst->state_offset,
                inst->batch_arguments,
                /*arena=*/nullptr,
                -1,
                inst->delta_column);

        if (inst->batch_that->isUserDefined())
        {
            inst->batch_that->flush(place + inst->state_offset);

            if (!should_finalize)
                should_finalize = (inst->batch_that->getEmitTimes(place + inst->state_offset) > 0);
        }
    }

    if (trackingStateCount())
        TrackingCount::addBatchSinglePlace(
            row_begin, row_end, place + tracking_count_offset, aggregate_instructions ? aggregate_instructions->delta_column : nullptr);

    return should_finalize;
}

BlocksList HybridAggregator::convertToBlocksWithoutKey(HybridAggregatedDataVariants & data_variants) const
{
    if (params->tracking_updates_type == TrackingUpdatesType::UpdatesWithRetract)
        return convertToBlocksWithoutKeyForRetracts(data_variants);

    return BlocksList{doConvertOnePlace(data_variants.without_key.get(), getHeader())};
}

BlocksList HybridAggregator::convertToBlocksWithoutKeyForRetracts(HybridAggregatedDataVariants & data_variants) const
{
    assert(params->tracking_updates_type == TrackingUpdatesType::UpdatesWithRetract);

    auto res_header = getHeader();
    BlocksList blocks;
    if (data_variants.without_key_retracts)
    {
        blocks.push_back(doConvertOnePlace(data_variants.without_key_retracts.get(), res_header));
        auto retract_delta_col = ColumnInt8::create(blocks.back().rows(), static_cast<Int8>(-1));
        auto delta_col_type = DataTypeFactory::instance().get(TypeIndex::Int8);
        blocks.back().insert(ColumnWithTypeAndName{std::move(retract_delta_col), std::move(delta_col_type), "_tp_delta"});
        data_variants.resetRetractWithoutKey();
    }

    blocks.push_back(doConvertOnePlace(data_variants.without_key.get(), res_header));
    {
        auto delta_col = ColumnInt8::create(blocks.back().rows(), static_cast<Int8>(1));
        auto delta_col_type = DataTypeFactory::instance().get(TypeIndex::Int8);
        blocks.back().insert(ColumnWithTypeAndName{std::move(delta_col), std::move(delta_col_type), "_tp_delta"});
    }

    return blocks;
}

Block HybridAggregator::doConvertOnePlace(AggregateDataPtr data, const Block & res_header) const
{
    size_t rows = 1;
    auto && out_cols = prepareOutputBlockColumns(res_header, /*aggregates_pool=*/nullptr, rows);

    auto && [key_columns, raw_key_columns, final_aggregate_columns] = out_cols;

    insertAggregatesIntoColumns(data, final_aggregate_columns, /*arena=*/nullptr);

    return finalizeBlock(res_header, std::move(out_cols), rows);
}

}
