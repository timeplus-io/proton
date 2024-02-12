#include <Parsers/Streaming/ParserEmitQuery.h>
#include <Parsers/Streaming/ASTEmitQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>

#include <Core/Streaming/Watermark.h>

namespace DB
{
namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}

bool ParserEmitQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[maybe_unused]] bool hint)
{
    /// EMIT [STREAM|CHANGELOG]
    ///     [AFTER WATERMARK WITH DELAY <interval>]
    ///     [PERIODIC <interval>]
    ///     [ON UPDATE]
    ///     - [[ AND ]TIMEOUT <interval>]
    ///     - [[ AND ]LAST <last-x> [ON PROCTIME]]
    /// For each sub-option with prefix `-` can be combined with 'AND', we shall select a matching mod based on the combination of parsed content finally.
    /// For example:
    /// 1) EMIT STREAM PERIODIC INTERVAL '3' SECONDS AND AFTER WATERMARK
    /// 2) EMIT STREAM AFTER WATERMARK WITH DELAY INTERVAL '3' SECONDS
    /// 3) EMIT STREAM AFTER WATERMARK AND LAST <last-x>
    /// 4) EMIT STREAM LAST 1h ON PROCTIME
    /// 5) EMIT CHANGELOG
    /// 6) EMIT STREAM ON UPDATE
    /// 7) EMIT STREAM PERIODIC 3s ON UPDATE
    /// ...
    if (!parse_only_internals)
    {
        ParserKeyword s_emit("EMIT");
        if (!s_emit.ignore(pos, expected))
            return false;
    }

    ASTEmitQuery::StreamMode stream_mode = ASTEmitQuery::StreamMode::STREAM;
    if (ParserKeyword("STREAM").ignore(pos, expected))
        stream_mode = ASTEmitQuery::StreamMode::STREAM;
    else if (ParserKeyword("CHANGELOG").ignore(pos, expected))
        stream_mode = ASTEmitQuery::StreamMode::CHANGELOG;

    bool after_watermark = false;
    ASTPtr delay_interval;

    ParserIntervalOperatorExpression interval_alias_p;
    if (ParserKeyword("AFTER").ignore(pos, expected))
    {
        if (!ParserKeyword("WATERMARK").ignore(pos, expected))
            return false;

        after_watermark = true;

        /// [WITH DELAY INTERVAL '3' SECONDS]
        if (ParserKeyword("WITH").ignore(pos, expected))
        {
            if (!ParserKeyword("DELAY").ignore(pos, expected))
                throw Exception("Expect 'WATERMARK' after 'AFTER' in EMIT clause", ErrorCodes::SYNTAX_ERROR);

            if (!interval_alias_p.parse(pos, delay_interval, expected))
                return false;
        }
    }

    ASTPtr periodic_interval;
    if (ParserKeyword("PERIODIC").ignore(pos, expected))
    {
        /// [PERIODIC INTERVAL '3' SECONDS]
        if (!interval_alias_p.parse(pos, periodic_interval, expected))
            return false;
    }

    bool on_update = false;
    if (ParserKeyword("ON").ignore(pos, expected))
    {
        if (!ParserKeyword("UPDATE").ignore(pos, expected))
            throw Exception("Expect 'UPDATE' after 'ON' in EMIT clause", ErrorCodes::SYNTAX_ERROR);

        on_update = true;
    }

    ASTPtr timeout_interval;
    ASTPtr last_interval;
    bool proctime = false;
    do
    {
        if (ParserKeyword("PERIODIC").ignore(pos, expected))
        {
            /// [PERIODIC INTERVAL '3' SECONDS]
            if (periodic_interval)
                throw Exception("Can not use repeat 'PERIODIC' in EMIT clause", ErrorCodes::SYNTAX_ERROR);

            if (!interval_alias_p.parse(pos, periodic_interval, expected))
                return false;
        }
        else if (ParserKeyword("TIMEOUT").ignore(pos, expected))
        {
            /// [TIMEOUT INTERVAL '5' SECONDS]
            if (timeout_interval)
                throw Exception("Can not use repeat 'TIMEOUT' in EMIT clause", ErrorCodes::SYNTAX_ERROR);

            if (!interval_alias_p.parse(pos, timeout_interval, expected))
                return false;
        }
        else if (ParserKeyword("LAST").ignore(pos, expected))
        {
            /// [LAST <last-x>]
            if (last_interval)
                throw Exception("Can not use repeat 'LAST' in EMIT clause", ErrorCodes::SYNTAX_ERROR);

            if (!interval_alias_p.parse(pos, last_interval, expected))
                return false;

            if (ParserKeyword("ON").ignore(pos, expected))
            {
                if (ParserKeyword("PROCTIME").ignore(pos, expected))
                    proctime = true;
                else
                    throw Exception("Expect 'PROCTIME' after 'ON' in EMIT LAST clause", ErrorCodes::SYNTAX_ERROR);
            }
        }
    } while (ParserKeyword("AND").ignore(pos, expected));

    auto query = std::make_shared<ASTEmitQuery>();
    query->stream_mode = stream_mode;
    query->after_watermark = after_watermark;
    query->delay_interval = delay_interval;
    query->periodic_interval = periodic_interval;
    query->on_update = on_update;
    query->timeout_interval = timeout_interval;
    query->last_interval = last_interval;
    query->proc_time = proctime;

    node = query;

    return true;
}

}
