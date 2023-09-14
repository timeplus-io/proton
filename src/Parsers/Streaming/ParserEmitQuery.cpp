#include "ParserEmitQuery.h"
#include "ASTEmitQuery.h"

#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

bool ParserEmitQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    /// EMIT [STREAM|CHANGELOG]
    ///         - [PERIODIC INTERVAL '3' SECONDS]
    ///         - [AFTER WATERMARK]
    ///         - [DELAY INTERVAL '3' SECONDS]
    ///         - [LAST <last-x>]
    /// For each sub-option can be combined with 'AND', we shall select a matching mod based on the combination of parsed content finally.
    /// For example:
    /// 1) EMIT STREAM PERIODIC INTERVAL '3' SECONDS AND AFTER WATERMARK
    /// 2) EMIT STREAM AFTER WATERMARK AND DELAY INTERVAL '3' SECONDS
    /// 3) EMIT STREAM AFTER WATERMARK AND LAST <last-x>
    /// 4) EMIT STREAM LAST 1h ON PROCTIME
    /// 5) EMIT CHANGELOG
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
    bool proctime = false;
    ASTPtr periodic_interval;
    ASTPtr delay_interval;
    ASTPtr last_interval;
    ASTPtr timeout_interval;

    ParserIntervalOperatorExpression interval_alias_p;
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
        else if (ParserKeyword("AFTER").ignore(pos, expected))
        {
            if (!ParserKeyword("WATERMARK").ignore(pos, expected))
                throw Exception("Expect 'WATERMARK' after 'AFTER' in EMIT clause", ErrorCodes::SYNTAX_ERROR);

            /// [AFTER WATERMARK]
            if (after_watermark)
                throw Exception("Can not use repeat 'AFTER WATERMARK' in EMIT clause", ErrorCodes::SYNTAX_ERROR);

            after_watermark = true;
        }
        else if (ParserKeyword("DELAY").ignore(pos, expected))
        {
            /// [DELAY INTERVAL '3' SECONDS]
            if (delay_interval)
                throw Exception("Can not use repeat 'DELAY' in EMIT clause", ErrorCodes::SYNTAX_ERROR);

            if (!interval_alias_p.parse(pos, delay_interval, expected))
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
                    throw Exception("Expect 'PROCTIME' after 'ON' in EMIT clause", ErrorCodes::SYNTAX_ERROR);
            }
        }
        else if (ParserKeyword("TIMEOUT").ignore(pos, expected))
        {
            /// [TIMEOUT INTERVAL '5' SECONDS]
            if (timeout_interval)
                throw Exception("Can not use repeat 'TIMEOUT' in EMIT clause", ErrorCodes::SYNTAX_ERROR);

            if (!interval_alias_p.parse(pos, timeout_interval, expected))
                return false;
        }
    } while (ParserKeyword("AND").ignore(pos, expected));

    auto query = std::make_shared<ASTEmitQuery>();
    query->stream_mode = stream_mode;
    query->after_watermark = after_watermark;
    query->proc_time = proctime;
    query->periodic_interval = periodic_interval;
    query->delay_interval = delay_interval;
    query->last_interval = last_interval;
    query->timeout_interval = timeout_interval;

    node = query;

    return true;
}

}
