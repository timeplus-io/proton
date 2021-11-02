#include <Parsers/ParserEmitQuery.h>
#include <Parsers/ASTEmitQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{

bool ParserEmitQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    /// EMIT [STREAM]
    /// EMIT [STREAM] PERIODIC INTERVAL '3' SECONDS
    /// EMIT [STREAM] AFTER DELAY INTERVAL '3' SECONDS
    /// EMIT [STREAM] AFTER WATERMARK
    /// EMIT [STREAM] AFTER WATERMARK AND DELAY INTERVAL '3' SECONDS
    if (!parse_only_internals)
    {
        ParserKeyword s_emit("EMIT");
        if (!s_emit.ignore(pos, expected))
            return false;
    }

    /// FIXME AND order
    ASTPtr interval;
    ASTEmitQuery::Mode mode = ASTEmitQuery::TAIL;
    bool streaming = false;

    if (ParserKeyword("STREAM").ignore(pos, expected))
    {
        streaming = true;
    }

    ParserIntervalOperatorExpression interval_p;

    if (ParserKeyword("PERIODIC").ignore(pos, expected))
    {
        if (!interval_p.parse(pos, interval, expected))
            return false;

        mode = ASTEmitQuery::Mode::PERIODIC;
    }
    else if (ParserKeyword("AFTER").ignore(pos, expected))
    {
        if (ParserKeyword("DELAY").ignore(pos, expected))
        {
            if (!interval_p.parse(pos, interval, expected))
                return false;

            mode = ASTEmitQuery::Mode::DELAY;
        }
        else if (ParserKeyword("WATERMARK").ignore(pos, expected))
        {
            if (ParserKeyword("AND").ignore(pos, expected))
            {
                if (ParserKeyword("DELAY").ignore(pos, expected))
                {
                    if (!interval_p.parse(pos, interval, expected))
                        return false;

                    mode = ASTEmitQuery::Mode::WATERMARK_WITH_DELAY;
                }
                else
                    return false;
            }
            else
                mode = ASTEmitQuery::Mode::WATERMARK;
        }
        else
            return false;
    }

    if (!streaming && mode == ASTEmitQuery::Mode::TAIL)
        return false;

    auto query = std::make_shared<ASTEmitQuery>();
    query->streaming = streaming;
    query->mode = mode;
    query->interval = interval;
    node = query;

    return true;
}

}
