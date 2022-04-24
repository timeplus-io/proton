#include <Parsers/IParserBase.h>


namespace DB
{
bool IParserBase::parse(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    if (hint)
        expected.add(pos, getName());

    return wrapParseImpl(pos, IncreaseDepthTag{}, [&]
    {
        bool res = parseImpl(pos, node, expected, hint);
        if (!res)
            node = nullptr;
        return res;
    });
}

}
