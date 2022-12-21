#include "SessionHelper.h"

#include "AggregatingTransform.h"

namespace DB
{
namespace Streaming
{
namespace SessionHelper
{
template <typename T>
std::pair<std::vector<IColumn::Filter>, SessionInfos> prepareSessionImpl(
    SessionInfo & info,
    const PaddedPODArray<T> & time_vec,
    ColumnPtr & session_start_column,
    ColumnPtr & session_end_column,
    size_t num_rows,
    AggregatingTransformParamsPtr params)
{
    Block block;

    /// session_start_column could be a ColumnConst object
    const auto & session_start_vec = checkAndGetColumn<ColumnUInt8>(session_start_column.get())->getData();
    const auto & session_end_vec = checkAndGetColumn<ColumnUInt8>(session_end_column.get())->getData();

    IColumn::Filter filter(num_rows, 0);
    SessionInfos sessions_info_to_emit;
    std::vector<IColumn::Filter> session_filters;
    size_t offset = 0;
    Int64 ts_secs = time_vec[0];
    UInt8 session_start = session_start_vec[0];
    UInt8 session_end = session_end_vec[0];

    const DateLUTImpl & time_zone = DateLUT::instance("UTC");

    auto ignore_row = [&]() { filter[offset] = 0; };

    auto keep_row = [&]() {
        filter[offset] = 1;
        /// Extend session window end and timeout timestamp
        if (ts_secs > info.win_end)
        {
            info.win_end = ts_secs;
            info.timeout_ts = addTime(info.win_end, params->params.interval_kind, params->params.window_interval, time_zone, info.scale);
        }
    };

    /// Keep/ignore the start session row
    auto keep_row_if_include_start = [&]() {
        if (params->params.window_desc->start_with_boundary)
            keep_row();
        else
            ignore_row();
    };

    /// Keep/ignore the close session row
    auto keep_row_if_include_end = [&]() {
        if (params->params.window_desc->end_with_boundary)
            keep_row();
        else
        {
            ignore_row();
            /// Not keep but still need to extend the window end
            if (ts_secs > info.win_end)
                info.win_end = ts_secs;
        }
    };

    auto open_session = [&]() {
        info.win_start = ts_secs;
        info.win_end = ts_secs + 1;
        info.timeout_ts = addTime(info.win_end, params->params.interval_kind, params->params.window_interval, time_zone, info.scale);
        info.ignore_ts = addTime(info.win_start, params->params.timeout_kind, -1 * params->params.session_size, time_zone, info.scale);
        info.max_session_ts = addTime(info.win_start, params->params.timeout_kind, params->params.session_size, time_zone, info.scale);
        info.active = true;
    };

    auto close_session = [&]() {
        info.active = false;
        sessions_info_to_emit.emplace_back(info);
        session_filters.emplace_back(num_rows, 0);
        session_filters.back().swap(filter);
    };

    for (; offset < num_rows; ++offset)
    {
        ts_secs = time_vec[offset];
        session_start = session_start_vec[offset];
        session_end = session_end_vec[offset];

        if (info.active)
        {
            /// Special handlings:
            /// 1) if event_time < @ingore_ts, ingore event
            /// 2) if event_time > @timeout_ts, close session
            /// 3) if event_time > @max_session_ts, close session
            /// 4) if session end condition is true, close session
            assert(info.win_start <= info.win_end);
            if (ts_secs < info.ignore_ts)
                ignore_row();
            else if (ts_secs > info.timeout_ts || ts_secs > info.max_session_ts)
            {
                /// Timeout not keep current row.
                close_session();

                if (session_start)
                {
                    open_session();
                    keep_row_if_include_start();
                }
            }
            else if (session_end)
            {
                keep_row_if_include_end();
                close_session();

                if (session_start)
                {
                    open_session();
                    keep_row_if_include_start();
                }
            }
            else
                keep_row();
        }
        else
        {
            /// Active session window
            if (session_start)
            {
                open_session();
                keep_row_if_include_start();
            }
            else
                ignore_row();
        }
    }

    if (info.active)
        session_filters.emplace_back(std::move(filter));

    return {std::move(session_filters), std::move(sessions_info_to_emit)};
}

std::pair<std::vector<IColumn::Filter>, SessionInfos> prepareSession(
    SessionInfo & info,
    ColumnPtr & time_column,
    ColumnPtr & session_start_column,
    ColumnPtr & session_end_column,
    size_t num_rows,
    AggregatingTransformParamsPtr params)
{
    if (params->params.time_col_is_datetime64)
        return prepareSessionImpl(
            info,
            checkAndGetColumn<ColumnDecimal<DateTime64>>(time_column.get())->getData(),
            session_start_column,
            session_end_column,
            num_rows,
            params);
    else
        return prepareSessionImpl(
            info,
            checkAndGetColumn<ColumnVector<UInt32>>(time_column.get())->getData(),
            session_start_column,
            session_end_column,
            num_rows,
            params);
}

void finalizeSession(AggregatedDataVariants & variants, const SessionInfo & info, Block & final_block, AggregatingTransformParamsPtr params)
{
    /// FIXME, parallelization ? We simply don't know for now if parallelization makes sense since most of the time, we have only
    /// one project window for streaming processing
    Block header = params->aggregator.getHeader(true, false).cloneEmpty();
    auto window_start_col = header.getByName(ProtonConsts::STREAMING_WINDOW_START);
    auto window_start_col_ptr = IColumn::mutate(window_start_col.column);
    auto window_end_col = header.getByName(ProtonConsts::STREAMING_WINDOW_END);
    auto window_end_col_ptr = IColumn::mutate(window_end_col.column);

    BlocksList merged_blocks;
    if (params->final)
        merged_blocks = params->aggregator.convertToBlocksFinal(variants, ConvertAction::STREAMING_EMIT, 1);
    else
        merged_blocks = params->aggregator.convertToBlocksIntermediate(variants, ConvertAction::STREAMING_EMIT, 1);

    assert(merged_blocks.size() == 1);
    Block & merged_block = merged_blocks.back();

    /// NOTE: In case no aggregate key and no aggregate function, the merged_block shall be empty.
    /// e.g. `select window_start, window_end, date_diff('second', window_start, window_end) from session(test_session, 5s, [location='ca', location='sh')) group by window_start, window_end`
    size_t session_rows = merged_block ? merged_block.rows() : 1;
    assert(session_rows > 0);
    for (size_t i = 0; i < session_rows; i++)
    {
        if (params->params.time_col_is_datetime64)
        {
            window_start_col_ptr->insert(DecimalUtils::decimalFromComponents<DateTime64>(
                info.win_start / common::exp10_i64(info.scale), info.win_start % common::exp10_i64(info.scale), info.scale));
            window_end_col_ptr->insert(DecimalUtils::decimalFromComponents<DateTime64>(
                info.win_end / common::exp10_i64(info.scale), info.win_end % common::exp10_i64(info.scale), info.scale));
        }
        else
        {
            window_start_col_ptr->insert(info.win_start);
            window_end_col_ptr->insert(info.win_end);
        }
    }

    /// fill session info columns, i.e. 'window_start', 'window_end'
    /// FIXME, this looks pretty hacky as we need align the column positions / header here with Aggregator::Params::getHeader(...)
    merged_block.insert(0, {std::move(window_end_col_ptr), window_end_col.type, window_end_col.name});
    merged_block.insert(0, {std::move(window_start_col_ptr), window_start_col.type, window_start_col.name});

    if (final_block.rows() > 0)
    {
        assertBlocksHaveEqualStructure(merged_block, final_block, "merging buckets for streaming two level hashtable");
        for (size_t i = 0, size = final_block.columns(); i < size; ++i)
        {
            const auto source_column = merged_block.getByPosition(i).column;
            auto mutable_column = IColumn::mutate(std::move(final_block.getByPosition(i).column));
            mutable_column->insertRangeFrom(*source_column, 0, source_column->size());
            final_block.getByPosition(i).column = std::move(mutable_column);
        }
    }
    else
        final_block = std::move(merged_block);

    /// Clear and reuse current session variants
    variants.init(variants.type);
    variants.aggregates_pools = Arenas(1, std::make_shared<Arena>());
    variants.aggregates_pool = variants.aggregates_pools.back().get();
    variants.aggregator->initStatesForWithoutKeyOrOverflow(variants);
}
}
}
}
