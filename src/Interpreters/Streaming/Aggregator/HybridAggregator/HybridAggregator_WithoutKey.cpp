#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Streaming/Aggregator/HybridAggregator/HybridAggregator.h>

namespace DB::Streaming
{

[[nodiscard]] bool HybridAggregator::executeWithoutKeyImpl(
    HybridAggregatedDataVariants & result, size_t row_begin, size_t row_end, AggregateFunctionInstruction * aggregate_instructions) const
{
    auto * place = result.without_key.get();

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

    return should_finalize;
}

/// \param merged_variants if \param data_variants is merged which means it is temporary used to convert to blocks
BlocksList
HybridAggregator::convertToBlocksWithoutKey(HybridAggregatedDataVariants & data_variants, bool merged_variants, bool final_) const
{
    if (params->tracking_updates_type == TrackingUpdatesType::UpdatesWithRetract)
    {
        if (merged_variants)
            return convertToBlocksWithoutKeyForRetractsMerged(data_variants, final_);
        else
            return convertToBlocksWithoutKeyForRetracts(data_variants, final_);
    }

    auto res_header = params->getHeader(input_header, final_);
    return BlocksList{doConvertOnePlace(data_variants.without_key.get(), res_header, final_)};
}

BlocksList HybridAggregator::convertToBlocksWithoutKeyForRetractsMerged(HybridAggregatedDataVariants & data_variants, bool final_) const
{
    assert(params->tracking_updates_type == TrackingUpdatesType::UpdatesWithRetract);

    auto res_header = params->getHeader(input_header, final_);
    BlocksList blocks;
    if (data_variants.without_key_retracts)
    {
        blocks.push_back(doConvertOnePlace(data_variants.without_key_retracts.get(), res_header, final_));
        auto retract_delta_col = ColumnInt8::create(blocks.back().rows(), static_cast<Int8>(-1));
        auto delta_col_type = DataTypeFactory::instance().get(TypeIndex::Int8);
        blocks.back().insert(ColumnWithTypeAndName{std::move(retract_delta_col), std::move(delta_col_type), "_tp_delta"});
    }

    blocks.push_back(doConvertOnePlace(data_variants.without_key.get(), res_header, final_));
    {
        auto delta_col = ColumnInt8::create(blocks.back().rows(), static_cast<Int8>(1));
        auto delta_col_type = DataTypeFactory::instance().get(TypeIndex::Int8);
        blocks.back().insert(ColumnWithTypeAndName{std::move(delta_col), std::move(delta_col_type), "_tp_delta"});
    }

    return blocks;
}

BlocksList HybridAggregator::convertToBlocksWithoutKeyForRetracts(HybridAggregatedDataVariants & data_variants, bool final_) const
{
    assert(params->tracking_updates_type == TrackingUpdatesType::UpdatesWithRetract);

    auto res_header = params->getHeader(input_header, final_);
    BlocksList blocks;
    if (data_variants.without_key_retracts)
    {
        blocks.push_back(doConvertOnePlace(data_variants.without_key_retracts.get(), res_header, final_));
        auto retract_delta_col = ColumnInt8::create(blocks.back().rows(), static_cast<Int8>(-1));
        auto delta_col_type = DataTypeFactory::instance().get(TypeIndex::Int8);
        blocks.back().insert(ColumnWithTypeAndName{std::move(retract_delta_col), std::move(delta_col_type), "_tp_delta"});
        /// overwrite with current aggregate states
        destroyAggregateStates(data_variants.without_key_retracts.get());
        createAggregateStates(data_variants.without_key_retracts.get());
        mergeAggregateStates(data_variants.without_key_retracts.get(), data_variants.without_key.get(), /*arena=*/nullptr);
    }
    else
    {
        /// Save the aggregate states for retraction
        data_variants.initWithoutKeyRetractStates(total_size_of_aggregate_states, align_aggregate_states);
        mergeAggregateStates(data_variants.without_key_retracts.get(), data_variants.without_key.get(), /*arena=*/nullptr);
    }

    blocks.push_back(doConvertOnePlace(data_variants.without_key.get(), res_header, final_));
    {
        auto delta_col = ColumnInt8::create(blocks.back().rows(), static_cast<Int8>(1));
        auto delta_col_type = DataTypeFactory::instance().get(TypeIndex::Int8);
        blocks.back().insert(ColumnWithTypeAndName{std::move(delta_col), std::move(delta_col_type), "_tp_delta"});
    }

    return blocks;
}

Block HybridAggregator::doConvertOnePlace(AggregateDataPtr data, const Block & res_header, bool final_) const
{
    size_t rows = 1;
    auto && out_cols = prepareOutputBlockColumns(
        res_header,
        /*aggregates_pools=*/{}, /// FIXME arenas
        final_,
        rows);

    auto && [key_columns, raw_key_columns, aggregate_columns, final_aggregate_columns, aggregate_columns_data] = out_cols;

    if (!final_)
    {
        for (size_t i = 0; i < params->aggregates_size; ++i)
            aggregate_columns_data[i]->push_back(data + offsets_of_aggregate_states[i]);
    }
    else
    {
        insertAggregatesIntoColumns(data, final_aggregate_columns, /*arena=*/nullptr);
    }

    return finalizeBlock(res_header, std::move(out_cols), final_, rows);
}

}
