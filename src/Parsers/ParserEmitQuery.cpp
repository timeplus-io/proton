#include <Parsers/ParserEmitQuery.h>
#include <Parsers/ASTEmitQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

bool ParserEmitQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// EMIT [STREAM]
    ///         - [PERIODIC INTERVAL '3' SECONDS]
    ///         - [AFTER WATERMARK]
    ///         - [DELAY INTERVAL '3' SECONDS]
    ///         - [LAST <last-x>]
    /// For each sub-option can be combined with 'AND', we shall select a matching mod based on the combination of parsed content finally.
    /// For example:
    /// 1) EMIT STREAM PERIODIC INTERVAL '3' SECONDS AND AFTER WATERMARK
    /// 2) EMIT STREAM AFTER WATERMARK AND DELAY INTERVAL '3' SECONDS
    /// 3) EMIT STREAM AFTER WATERMARK AND LAST <last-x>
    /// ...
    if (!parse_only_internals)
    {
        ParserKeyword s_emit("EMIT");
        if (!s_emit.ignore(pos, expected))
            return false;
    }

    bool streaming = false;
    if (ParserKeyword("STREAM").ignore(pos, expected))
        streaming = true;

    bool after_watermark = false;
    ASTPtr periodic_interval;
    ASTPtr delay_interval;
    ASTPtr last_interval;

    ParserIntervalOperatorExpression interval_p;
    ParserIntervalAliasExpression interval_alias_p;
    do
    {
        if (ParserKeyword("PERIODIC").ignore(pos, expected))
        {
            /// [PERIODIC INTERVAL '3' SECONDS]
            if (periodic_interval)
                throw Exception("Can not use repeat 'PERIODIC' in EMIT caluse", ErrorCodes::SYNTAX_ERROR);

            if (!interval_p.parse(pos, periodic_interval, expected))
                return false;
        }
        else if (ParserKeyword("AFTER WATERMARK").ignore(pos, expected))
        {
            /// [AFTER WATERMARK]
            if (after_watermark)
                throw Exception("Can not use repeat 'AFTER WATERMARK' in EMIT caluse", ErrorCodes::SYNTAX_ERROR);

            after_watermark = true;
        }
        else if (ParserKeyword("DELAY").ignore(pos, expected))
        {
            /// [DELAY INTERVAL '3' SECONDS]
            if (delay_interval)
                throw Exception("Can not use repeat 'DELAY' in EMIT caluse", ErrorCodes::SYNTAX_ERROR);

            if (!interval_p.parse(pos, delay_interval, expected))
                return false;
        }
        else if (ParserKeyword("LAST").ignore(pos, expected))
        {
            if (last_interval)
                throw Exception("Can not use repeat 'LAST' in EMIT caluse", ErrorCodes::SYNTAX_ERROR);

            /// [LAST <last-x>]
            if (!interval_alias_p.parse(pos, last_interval, expected))
                return false;
        }
    }
    while (ParserKeyword("AND").ignore(pos, expected));

    auto query = std::make_shared<ASTEmitQuery>();
    query->streaming = streaming;
    query->after_watermark = after_watermark;
    query->periodic_interval = periodic_interval;
    query->delay_interval = delay_interval;
    query->last_interval = last_interval;

    node = query;

    return true;
}

}
