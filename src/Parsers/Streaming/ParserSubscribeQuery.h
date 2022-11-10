
#include <Parsers/IParserBase.h>

namespace DB
{
namespace Streaming
{
class ParserSubscribeQuery final : public IParserBase
{
protected:
    const char * getName() const override { return "SUBSCRIBE TO SELECT query, possibly with UNION"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};

class ParserUnsubscribeQuery final : public IParserBase
{
protected:
    const char * getName() const override { return "UNSUBSCRIBE FROM SELECT query, possibly with UNION SETTINGS query_id=uuid"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};

class ParserRecoverQuery final : public IParserBase
{
protected:
    const char * getName() const override { return "RECOVER FROM SELECT query, possibly with UNION SETTINGS query_id=uuid"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};
}
}
