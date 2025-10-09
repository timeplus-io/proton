#include <Parsers/Streaming/ASTEmitQuery.h>
#include <Parsers/Streaming/ParserEmitQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{
namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
extern const int NOT_IMPLEMENTED;
}

bool ParserEmitQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    /// EMIT [STREAM|CHANGELOG|DELTA]
    ///     - AFTER WINDOW CLOSE                <=>  (Old) `AFTER WATERMARK`
    ///         - WITH DELAY <interval>
    ///         - AND TIMEOUT <interval>
    ///     - PERIODIC <interval> [REPEAT]
    ///         - WITH DELAY <interval>
    ///         - AND TIMEOUT <interval>
    ///     - ON UPDATE
    ///         - WITH DELAY <interval>
    ///         - AND TIMEOUT <interval>
    ///     - ON UPDATE WITH BATCH <interval>   <=>  (Old) `PERIODIC <interval> ON UPDATE`
    ///         - WITH DELAY <interval>
    ///         - AND TIMEOUT <interval>
    ///     - PER EVENT
    ///         - WITH DELAY <interval>
    ///         - AND TIMEOUT <interval>
    ///     - PER KEY IDENTIFIED BY ts_col WITH MAXSPAN <interval> AND TIMEOUT <interval>
    ///
    /// (Will be deprecated)
    /// EMIT LAST <last-x> [ON PROCTIME]]
    ///
    /// For example:
    /// 1) EMIT STREAM AFTER WINDOW CLOSE WITH DELAY 1s AND TIMEOUT 5s
    /// 2) EMIT STREAM PERIODIC 1s REPEAT WITH DELAY 1s AND TIMEOUT 5s
    /// 3) EMIT ON UPDATE WITH DELAY 1s AND TIMEOUT 5s
    /// 4) EMIT ON UPDATE WITH BATCH 1s WITH DELAY 1s AND TIMEOUT 5s
    /// 5) EMIT PER EVENT WITH DELAY 1s AND TIMEOUT 5s
    /// 5) EMIT LAST 1h ON PROCTIME
    /// ...
    if (!parse_only_internals)
    {
        ParserKeyword s_emit("EMIT");
        if (!s_emit.ignore(pos, expected))
            return false;
    }

    auto query = std::make_shared<ASTEmitQuery>();
    ParserIntervalOperatorExpression interval_alias_p;

    if (ParserKeyword("STREAM").ignore(pos, expected))
        query->stream_mode = ASTEmitQuery::StreamMode::STREAM;
    else if (ParserKeyword("CHANGELOG").ignore(pos, expected))
        query->stream_mode = ASTEmitQuery::StreamMode::CHANGELOG;
    else if (ParserKeyword("DELTA").ignore(pos, expected))
        query->stream_mode = ASTEmitQuery::StreamMode::DELTA;

    /// Special case : EMIT [STREAM] LAST <last-x> [ON PROCTIME]]
    if (ParserKeyword("LAST").ignore(pos, expected))
    {
        if (query->stream_mode.value_or(ASTEmitQuery::StreamMode::STREAM) != ASTEmitQuery::StreamMode::STREAM)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "EMIT LAST clause only supports `STREAM` EMIT");

        if (!interval_alias_p.parse(pos, query->last_interval, expected))
            return false;

        if (ParserKeyword("ON").ignore(pos, expected))
        {
            if (ParserKeyword("PROCTIME").ignore(pos, expected))
                query->proc_time = true;
            else
                throw Exception(ErrorCodes::SYNTAX_ERROR, "Expect 'PROCTIME' after 'ON' in EMIT LAST clause");
        }
        node = std::move(query);
        return true;
    }

    /// EMIT [STREAM] AFTER KEY EXPIRE [IDENTIFIED BY ts_col] WITH [ONLY] MAXSPAN <interval> [AND TIMEOUT <interval>]
    if (ParserKeyword("AFTER KEY EXPIRE").ignore(pos, expected))
    {
        if (ParserKeyword("IDENTIFIED BY").ignore(pos, expected))
        {
            ParserIdentifier ts_col_p;
            if (!ts_col_p.parse(pos, query->key_ts_col, expected))
                return false;
        }

        if (!ParserKeyword("WITH MAXSPAN").ignore(pos, expected))
        {
            if (ParserKeyword("WITH ONLY MAXSPAN").ignore(pos, expected))
                query->only_max_span = true;
            else
                return false;
        }

        if (!interval_alias_p.parse(pos, query->key_max_span_interval, expected))
            return false;

        /// [AND TIMEOUT INTERVAL '5' SECONDS]
        if (ParserKeyword("AND TIMEOUT").ignore(pos, expected))
        {
            if (!interval_alias_p.parse(pos, query->timeout_interval, expected))
                return false;
        }

        node = std::move(query);
        return true;
    }

    auto parse_watermark_modifier = [&]() {
        /// [WITH DELAY INTERVAL '3' SECONDS]
        if (ParserKeyword("WITH DELAY").ignore(pos, expected))
        {
            if (!interval_alias_p.parse(pos, query->delay_interval, expected))
                return false;
        }

        /// [AND TIMEOUT INTERVAL '5' SECONDS]
        if (ParserKeyword("AND TIMEOUT").ignore(pos, expected))
        {
            if (!interval_alias_p.parse(pos, query->timeout_interval, expected))
                return false;
        }

        return true;
    };

    /// [AFTER WINDOW CLOSE]
    if (ParserKeyword("AFTER WINDOW CLOSE").ignore(pos, expected) || ParserKeyword("AFTER WATERMARK").ignore(pos, expected))
    {
        query->after_window_close = true;

        if (!parse_watermark_modifier())
            return false;
    }
    /// [PERIODIC <interval> [REPEAT]]
    else if (ParserKeyword("PERIODIC").ignore(pos, expected))
    {
        /// [PERIODIC INTERVAL '3' SECONDS]
        if (!interval_alias_p.parse(pos, query->periodic_interval, expected))
            return false;

        if (ParserKeyword("REPEAT").ignore(pos, expected))
        {
            query->repeat = true;
        }
        /// `PERIODIC <interval> ON UPDATE` is equal to `ON UPDATE WITH BATCH <interval>`
        else if (ParserKeyword("ON UPDATE").ignore(pos, expected))
        {
            query->batch_interval.swap(query->periodic_interval);
            query->on_update = true;
        }

        if (!parse_watermark_modifier())
            return false;
    }
    /// [ON UPDATE]
    else if (ParserKeyword("ON UPDATE").ignore(pos, expected))
    {
        query->on_update = true;

        // [WITH BATCH INTERVAL '3' SECONDS]
        if (ParserKeyword("WITH BATCH").ignore(pos, expected))
        {
            if (!interval_alias_p.parse(pos, query->batch_interval, expected))
                return false;
        }

        if (!parse_watermark_modifier())
            return false;
    }
    /// [PER EVENT]
    else if (ParserKeyword("PER EVENT").ignore(pos, expected))
    {
        query->per_event = true;

        if (!parse_watermark_modifier())
            return false;
    }
    else if (ParserKeyword("TIMEOUT").ignore(pos, expected))
    {
        /// For backward compatibility
        /// When `EMIT TIMEOUT 30s;` get applied to window aggregation or global aggregation
        /// `EMIT AFTER WINDOW CLOSE` or `EMIT PERIODIC` is implicitly implied
        if (!interval_alias_p.parse(pos, query->timeout_interval, expected))
            return false;
    }

    node = std::move(query);
    return true;
}

}
