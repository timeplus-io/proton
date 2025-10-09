#include <Interpreters/Streaming/Aggregator/MemoryAggregator/MemoryAggregator.h>

namespace DB::Streaming
{
[[nodiscard]] bool MemoryAggregator::executeWithoutKeyImpl(
    AggregatedDataWithoutKey res,
    size_t row_begin,
    size_t row_end,
    AggregateFunctionInstruction * aggregate_instructions,
    Arena * arena) const
{
    /// Adding values
    bool should_finalize = false;
    for (size_t i = 0, func_size = aggregate_functions.size(); i < func_size; ++i)
    {
        AggregateFunctionInstruction * inst = aggregate_instructions + i;

        if (inst->offsets)
            inst->batch_that->addBatchSinglePlace(
                inst->offsets[static_cast<ssize_t>(row_begin) - 1],
                inst->offsets[row_end - 1],
                res + inst->state_offset,
                inst->batch_arguments,
                arena,
                -1,
                inst->delta_column);
        else
            inst->batch_that->addBatchSinglePlace(
                row_begin, row_end, res + inst->state_offset, inst->batch_arguments, arena, -1, inst->delta_column);

        if (inst->batch_that->isUserDefined())
        {
            inst->batch_that->flush(res + inst->state_offset);

            if (!should_finalize)
                should_finalize = (inst->batch_that->getEmitTimes(res + inst->state_offset) > 0);
        }
    }

    if (params->tracking_updates_type == TrackingUpdatesType::Updates)
        TrackingUpdates::addBatchSinglePlace(
            row_begin, row_end, res, aggregate_instructions ? aggregate_instructions->delta_column : nullptr);
    else if (params->tracking_updates_type == TrackingUpdatesType::UpdatesWithRetract)
        TrackingUpdatesWithRetract::addBatchSinglePlace(
            row_begin, row_end, res, aggregate_instructions ? aggregate_instructions->delta_column : nullptr);

    return should_finalize;
}

Block MemoryAggregator::prepareBlockAndFillWithoutKey(
    MemoryAggregatedDataVariants & data_variants, bool final_, AggregatingConvertParams & cparams) const
{
    chassert(data_variants.aggregatorType() == AggregatorType::Memory);
    auto & variants = static_cast<MemoryAggregatedDataVariants &>(data_variants);

    switch (cparams.type)
    {
        case AggregatingConvertType::Normal:
            return prepareBlockAndFillWithoutKeyNormal(variants, final_, cparams);
        case AggregatingConvertType::Updates:
            return prepareBlockAndFillWithoutKeyForUpdates<TrackingUpdates>(variants, final_, cparams);
        case AggregatingConvertType::Retract:
            return prepareBlockAndFillWithoutKeyForRetract(variants, final_, cparams);
        case AggregatingConvertType::UpdatesAfterRetract:
            return prepareBlockAndFillWithoutKeyForUpdates<TrackingUpdatesWithRetract>(variants, final_, cparams);
    }
}

Block MemoryAggregator::prepareBlockAndFillWithoutKeyNormal(
    MemoryAggregatedDataVariants & data_variants, bool final_, AggregatingConvertParams & cparams) const
{
    chassert(cparams.type == AggregatingConvertType::Normal);

    auto res_header = params->getHeader(input_header, /*final_=*/true);
    auto data = data_variants.without_key;
    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid data variant passed.");

    size_t rows = 1;
    auto && out_cols = prepareOutputBlockColumns(res_header, data_variants.aggregates_pools, final_, rows);

    auto && [key_columns, raw_key_columns, aggregate_columns, final_aggregate_columns, aggregate_columns_data] = out_cols;

    if (!final_)
    {
        for (size_t i = 0; i < params->aggregates_size; ++i)
            aggregate_columns_data[i]->push_back(data + offsets_of_aggregate_states[i]);
    }
    else
    {
        insertAggregatesIntoColumns(data, final_aggregate_columns, data_variants.aggregates_pool);
    }

    return finalizeBlock(res_header, std::move(out_cols), final_, rows);
}

template <typename TrackingUpdatesStruct>
Block MemoryAggregator::prepareBlockAndFillWithoutKeyForUpdates(
    MemoryAggregatedDataVariants & data_variants, bool final_, AggregatingConvertParams & cparams) const
{
    chassert(
        (cparams.type == AggregatingConvertType::Updates && std::is_same_v<TrackingUpdatesStruct, TrackingUpdates>)
        || (cparams.type == AggregatingConvertType::UpdatesAfterRetract
            && std::is_same_v<TrackingUpdatesStruct, TrackingUpdatesWithRetract>));

    auto res_header = params->getHeader(input_header, final_);
    auto data = data_variants.without_key;
    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid data variant passed.");

    if (!TrackingUpdatesStruct::updated(data))
        /// No changes / updates, return empty block
        return res_header;

    TrackingUpdatesStruct::resetUpdated(data);

    size_t rows = 1;
    auto && out_cols = prepareOutputBlockColumns(res_header, data_variants.aggregates_pools, final_, rows);
    auto && [key_columns, raw_key_columns, aggregate_columns, final_aggregate_columns, aggregate_columns_data] = out_cols;

    if (!final_)
    {
        for (size_t i = 0; i < params->aggregates_size; ++i)
            aggregate_columns_data[i]->push_back(data + offsets_of_aggregate_states[i]);
    }
    else
    {
        /// Always single-thread. It's safe to pass current arena from 'aggregates_pool'.
        insertAggregatesIntoColumns(data, final_aggregate_columns, data_variants.aggregates_pool);
    }

    return finalizeBlock(res_header, std::move(out_cols), final_, rows);
}

Block MemoryAggregator::prepareBlockAndFillWithoutKeyForRetract(
    MemoryAggregatedDataVariants & data_variants, bool final_, AggregatingConvertParams & cparams) const
{
    chassert(cparams.type == AggregatingConvertType::Retract);

    auto res_header = params->getHeader(input_header, final_);
    auto without_key_state = data_variants.without_key;
    if (!TrackingUpdatesWithRetract::hasRetract(without_key_state))
        /// No changes / updates, return empty block
        return res_header;

    auto & data = TrackingUpdatesWithRetract::getRetract(without_key_state);
    if (!data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid data variant passed.");

    size_t rows = 1;
    auto && out_cols = prepareOutputBlockColumns(res_header, data_variants.aggregates_pools, final_, rows);

    auto && [key_columns, raw_key_columns, aggregate_columns, final_aggregate_columns, aggregate_columns_data] = out_cols;

    if (!final_)
    {
        for (size_t i = 0; i < params->aggregates_size; ++i)
            aggregate_columns_data[i]->push_back(data + offsets_of_aggregate_states[i]);
    }
    else
    {
        insertAggregatesIntoColumns(data, final_aggregate_columns, data_variants.aggregates_pool);
        destroyAggregateStates(data); /// Retract data is no longer needed after converted
    }

    return finalizeBlock(res_header, std::move(out_cols), final_, rows);
}

}
