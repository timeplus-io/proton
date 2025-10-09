#include <Processors/Transforms/Streaming/EmitParams.h>

#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/Streaming/ASTEmitQuery.h>


namespace DB
{
namespace ErrorCodes
{
extern const int INCORRECT_QUERY;
extern const int UNSUPPORTED;
}

namespace Streaming
{

namespace
{
void mergeEmitQuerySettings(const ASTPtr & emit_query, EmitParams & params)
{
    if (!emit_query)
        return;

    params.mode = EmitMode::None;
    auto emit = emit_query->as<ASTEmitQuery>();
    chassert(emit);

    if (emit->after_window_close)
    {
        if (!params.window_params)
            throw Exception(
                ErrorCodes::INCORRECT_QUERY, "Watermark emit is only supported in streaming queries with streaming window function");

        params.mode = EmitMode::AfterWindowClose;
    }
    else if (emit->periodic_interval)
    {
        params.periodic_interval = extractInterval(emit->periodic_interval->as<ASTFunction>());
        params.mode = EmitMode::Periodic;
    }
    else if (emit->on_update)
    {
        if (emit->batch_interval)
        {
            params.periodic_interval = extractInterval(emit->batch_interval->as<ASTFunction>());
            params.mode = EmitMode::OnUpdateWithBatchInterval;
        }
        else
        {
            params.mode = EmitMode::OnUpdate;
        }
    }
    else if (emit->per_event)
    {
        params.mode = EmitMode::PerEvent;
    }
    else if (emit->key_max_span_interval)
    {
        params.mode = EmitMode::AfterKeyExpire;
        params.key_max_span_interval = extractInterval(emit->key_max_span_interval->as<ASTFunction>());

        if (emit->key_ts_col)
            params.key_ts_col_name = emit->key_ts_col->as<ASTIdentifier>()->full_name;

        params.only_max_span = emit->only_max_span;
    }
    /// For backward compatibility: `EMIT TIMEOUT 30s;`
    else if (emit->timeout_interval)
    {
        if (params.window_params)
            params.mode = EmitMode::AfterWindowClose; /// Default to `AFTER WINDOW CLOSE` for window aggr query
        else
            emit->timeout_interval.reset(); /// Ignored timeout for non-window aggr query 
    }

    if (emit->delay_interval)
    {
        if (!params.window_params)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Watermark with delay is only supported with streaming window function");

        if (params.window_params->type == WindowType::Session)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Watermark with delay is not supported for session window");

        params.delay_interval = extractInterval(emit->delay_interval->as<ASTFunction>());
    }

    if (emit->timeout_interval)
    {
        if (!params.window_params && params.mode != EmitMode::AfterKeyExpire)
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "Watermark with timeout is only supported with streaming window function or per key emit in global aggregation");

        params.timeout_interval = extractInterval(emit->timeout_interval->as<ASTFunction>());
    }

    params.repeat = emit->repeat;
    params.delta = emit->stream_mode == ASTEmitQuery::StreamMode::DELTA;
}
}

EmitParams::EmitParams(ASTPtr query, TreeRewriterResultPtr syntax_analyzer_result, WindowParamsPtr window_params_)
    : window_params(std::move(window_params_))
{
    const auto * select_query = query->as<ASTSelectQuery>();
    chassert(select_query);

    mergeEmitQuerySettings(select_query->emit(), *this);

    if (syntax_analyzer_result->aggregates.empty() && !syntax_analyzer_result->has_group_by)
    {
        /// For streaming non-aggregation query
        if (mode != EmitMode::Tail && mode != EmitMode::None)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Streaming tail mode doesn't support any watermark or periodic emit");

        /// Set default emit mode
        if (mode == EmitMode::None)
            mode = EmitMode::Tail;

        if (delta)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "EMIT DELTA is not supported in streaming tail mode");
    }
    else
    {
        /// For streaming aggregation query
        if (mode == EmitMode::Tail)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "Streaming aggregation doesn't support tail emit");

        /// Set default emit mode
        if (mode == EmitMode::None)
        {
            if (window_params)
            {
                mode = EmitMode::AfterWindowClose;
            }
            else
            {
                /// If `PERIODIC INTERVAL ...` is missing in `EMIT STREAM` query
                mode = EmitMode::Periodic;
                periodic_interval.interval = ProtonConsts::DEFAULT_PERIODIC_INTERVAL.first;
                periodic_interval.unit = ProtonConsts::DEFAULT_PERIODIC_INTERVAL.second;
            }
        }

        if (delta && window_params)
            throw Exception(ErrorCodes::INCORRECT_QUERY, "EMIT DELTA is not supported in streaming window aggregation");
    }
}

std::pair<UInt64, UInt64> EmitParams::keyMaxSpanAndTimeoutMs() const
{
    if (mode != EmitMode::AfterKeyExpire)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MAXSPAN interval is only available in `EMIT AFTER KEY EXPIRE`");

    UInt64 max_span_ms = 0;
    switch (key_max_span_interval.unit)
    {
        case IntervalKind::Kind::Millisecond:
            max_span_ms = key_max_span_interval.interval;
            break;
        case IntervalKind::Kind::Second:
            max_span_ms = key_max_span_interval.interval * 1000;
            break;
        case IntervalKind::Kind::Minute:
            max_span_ms = key_max_span_interval.interval * 1000 * 60;
            break;
        default:
            throw Exception(ErrorCodes::UNSUPPORTED, "MAXSPAN only supports millisecond, second or minute interval");
    }

    if (max_span_ms == 0)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "MAXSPAN interval shall not be zero");

    UInt64 timeout_ms = 0;
    switch (timeout_interval.unit)
    {
        case IntervalKind::Kind::Millisecond:
            timeout_ms = timeout_interval.interval;
            break;
        case IntervalKind::Kind::Second:
            timeout_ms = timeout_interval.interval * 1000;
            break;
        case IntervalKind::Kind::Minute:
            timeout_ms = timeout_interval.interval * 1000 * 60;
            break;
        default:
            throw Exception(ErrorCodes::UNSUPPORTED, "TIMEOUT only supports millisecond, second or minute interval");
    }

    if (timeout_ms != 0)
        return {max_span_ms, timeout_ms};

    return {max_span_ms, max_span_ms};
}

}

}
