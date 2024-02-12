#include <Processors/Transforms/Streaming/SessionHelper.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Streaming/TimeTransformHelper.h>

#include <ranges>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNSUPPORTED_WATERMARK_STRATEGY;
extern const int UNSUPPORTED_WATERMARK_EMIT_MODE;
}

namespace Streaming
{
namespace SessionHelper
{
namespace
{
template <typename TimeColumnType, IntervalKind::Kind unit>
MutableColumnPtr calculateSessionIdColumnImpl(
    SessionInfoQueue & sessions,
    SessionWindowParams & params,
    const TimeColumnType & time_column,
    const ColumnVector<UInt8> & session_start_column,
    const ColumnVector<UInt8> & session_end_column,
    size_t num_rows)
{
    MutableColumnPtr session_id_col = ColumnArray::create(time_column.cloneEmpty());
    if (unlikely(num_rows == 0))
        return session_id_col;

    auto info = !sessions.empty() ? sessions.back() : nullptr;

    session_id_col->reserve(num_rows);

    const auto & time_data = time_column.getData();
    const auto & session_start_data = session_start_column.getData();
    const auto & session_end_data = session_end_column.getData();

    auto & session_id_array = assert_cast<ColumnArray &>(*session_id_col);
    auto & session_id_data = assert_cast<TimeColumnType &>(session_id_array.getData()).getData();
    auto & session_id_offsets = session_id_array.getOffsets();

    auto event_ts = time_data[0];
    auto session_start = session_start_data[0];
    auto session_end = session_end_data[0];

    auto keep_row = [&]() {
        /// Keep first row in current window
        if (info->win_start == 0)
            info->win_start = event_ts;

        /// Extend session window end and timeout timestamp
        if (event_ts >= info->win_end)
        {
            info->win_end = event_ts + 1;
            if constexpr (std::is_same_v<TimeColumnType, ColumnDateTime64>)
                info->timeout_ts = AddTime<unit>::execute(event_ts, params.session_timeout, *params.time_zone, params.time_scale);
            else
                info->timeout_ts = AddTime<unit>::execute(event_ts, params.session_timeout, *params.time_zone);
        }

        session_id_data.emplace_back(static_cast<typename TimeColumnType::ValueType>(info->id));
    };

    auto open_session = [&]() {
        /// If last session window is empty (i.e. `info && info->win_start == 0`), we can reuse this info
        if (!info || info->win_start != 0)
        {
            SessionID new_id = sessions.empty() ? 1 : sessions.back()->id + 1;
            info = sessions.emplace_back(std::make_shared<SessionInfo>());
            info->id = new_id;
        }

        info->active = true;
        if constexpr (std::is_same_v<TimeColumnType, ColumnDateTime64>)
        {
            info->timeout_ts = AddTime<unit>::execute(event_ts, params.session_timeout, *params.time_zone, params.time_scale);
            info->max_session_ts = AddTime<unit>::execute(event_ts, params.max_session_size, *params.time_zone, params.time_scale);
        }
        else
        {
            info->timeout_ts = AddTime<unit>::execute(event_ts, params.session_timeout, *params.time_zone);
            info->max_session_ts = AddTime<unit>::execute(event_ts, params.max_session_size, *params.time_zone);
        }

        if (params.start_with_inclusion)
            keep_row();
    };

    auto try_close_session_for_timeout = [&]() -> bool {
        if (event_ts >= info->timeout_ts)
        {
            info->win_end = info->timeout_ts;
            info->active = false;
            return true;
        }
        return false;
    };

    auto try_close_session_for_oversize = [&]() -> bool {
        if (event_ts >= info->max_session_ts)
        {
            info->win_end = info->max_session_ts;
            info->active = false;
            return true;
        }
        return false;
    };

    auto try_close_session_for_predication = [&]() -> bool {
        if (session_end)
        {
            if (params.end_with_inclusion)
                keep_row();
            else
            {
                /// Not keep but still need to extend the window end
                if (event_ts > info->win_end)
                    info->win_end = event_ts;
            }

            info->active = false;
            return true;
        }
        return false;
    };

    for (size_t offset = 0; offset < num_rows; ++offset)
    {
        event_ts = time_data[offset];
        session_start = session_start_data[offset];
        session_end = session_end_data[offset];

        if (info && info->active)
        {
            /// 1) if event_time >= @max_session_ts, close session
            /// 2) if event_time >= @timeout_ts, close session
            /// 3) if session end condition is true, close session
            if (try_close_session_for_oversize() || try_close_session_for_timeout() || try_close_session_for_predication())
            {
                if (session_start)
                    open_session();
            }
            else
                keep_row();
        }
        else
        {
            /// Active session window
            if (session_start)
                open_session();
        }

        session_id_offsets.push_back(session_id_data.size());
    }

    return session_id_col;
}
}

void assignWindow(
    SessionInfoQueue & sessions,
    SessionWindowParams & params,
    Columns & columns,
    ssize_t wstart_col_pos,
    ssize_t wend_col_pos,
    size_t time_col_pos,
    size_t session_start_col_pos,
    size_t session_end_col_pos)
{
    auto num_rows = columns.at(0)->size();
    if (unlikely(num_rows == 0))
        return;

    /// FIXME: Better to handle ColumnConst.
    auto time_column = columns[time_col_pos]->convertToFullColumnIfConst();
    auto session_start_column = columns[session_start_col_pos]->convertToFullColumnIfConst();
    auto session_end_column = columns[session_end_col_pos]->convertToFullColumnIfConst();

    MutableColumnPtr session_id_col;

    if (params.time_col_is_datetime64)
    {
#define M(INTERVAL_KIND) \
    session_id_col = calculateSessionIdColumnImpl<ColumnDateTime64, INTERVAL_KIND>( \
        sessions, \
        params, \
        assert_cast<const ColumnDateTime64 &>(*time_column), \
        assert_cast<const ColumnVector<UInt8> &>(*session_start_column), \
        assert_cast<const ColumnVector<UInt8> &>(*session_end_column), \
        num_rows);

        DISPATCH_FOR_WINDOW_INTERVAL(params.interval_kind, M)
#undef M
    }
    else
    {
#define M(INTERVAL_KIND) \
    session_id_col = calculateSessionIdColumnImpl<ColumnDateTime, INTERVAL_KIND>( \
        sessions, \
        params, \
        assert_cast<const ColumnDateTime &>(*time_column), \
        assert_cast<const ColumnVector<UInt8> &>(*session_start_column), \
        assert_cast<const ColumnVector<UInt8> &>(*session_end_column), \
        num_rows);

        DISPATCH_FOR_WINDOW_INTERVAL(params.interval_kind, M)
#undef M
    }

    /// For each row, there are three cases:
    /// 1) [] - skip
    /// 2) [<session_id>] - belongs to <session_id>
    /// 3) [<session_id_1>, <session_id_2>] - both belongs to <session_id_1> and <session_id_2>
    auto & session_id_array = assert_cast<ColumnArray &>(*session_id_col);
    auto & session_id_offsets = session_id_array.getOffsets();

    /// NOTE: Assign window start/end as session_id
    /// Then the window start/end will be rewrited by session_id later
    for (size_t pos = 0; auto & column : columns)
    {
        if (pos == static_cast<size_t>(wstart_col_pos))
            column = IColumn::mutate(session_id_array.getDataPtr());
        else if (pos == static_cast<size_t>(wend_col_pos))
            column = IColumn::mutate(session_id_array.getDataPtr());
        else
            column = column->replicate(session_id_offsets);
        ++pos;
    }
}

SessionInfoPtr getLastFinalizedSession(const SessionInfoQueue & sessions)
{
    for (const auto & session : sessions | std::views::reverse)
    {
        if (!session->active)
            return session;
    }
    return nullptr;
}

SessionID removeExpiredSessions(SessionInfoQueue & sessions)
{
    auto last_expired_session_id = -1;
    while (!sessions.empty() && !sessions.front()->active)
    {
        last_expired_session_id = sessions.front()->id;
        sessions.pop_front();
    }
    return last_expired_session_id;
}

WindowsWithBuckets getWindowsWithBuckets(const SessionInfoQueue & sessions)
{
    WindowsWithBuckets windows_with_buckets;
    windows_with_buckets.reserve(sessions.size());
    for (const auto & session : sessions)
        windows_with_buckets.emplace_back(WindowWithBuckets{{session->win_start, session->win_end}, {session->id}});

    return windows_with_buckets;
}

void validateWatermarkStrategyAndEmitMode(WatermarkStrategy & strategy, WatermarkEmitMode & mode, SessionWindowParams & params)
{
    /// TODO: So far, we always push down assign session window logic for session window aggregating
    assert(params.pushdown_window_assignment);

    /// FIXME: Set default strategy, configurable in the future ?
    if (strategy == WatermarkStrategy::Unknown)
        strategy = WatermarkStrategy::Ascending;

    if (mode == WatermarkEmitMode::Unknown)
        mode = WatermarkEmitMode::None;

    /// Supported checking
    if (strategy != WatermarkStrategy::Ascending)
        throw Exception(
            ErrorCodes::UNSUPPORTED_WATERMARK_STRATEGY,
            "Unsupported watermark strategy '{} for SESSION window",
            magic_enum::enum_name(strategy));

    if (mode != WatermarkEmitMode::None && mode != WatermarkEmitMode::OnUpdate && mode != WatermarkEmitMode::Periodic && mode != WatermarkEmitMode::PeriodicOnUpdate)
        throw Exception(
            ErrorCodes::UNSUPPORTED_WATERMARK_EMIT_MODE,
            "Unsupported watermark emit mode '{}' for SESSION window",
            magic_enum::enum_name(mode));
}
}
}
}
