
#include <Parsers/IParserBase.h>

namespace DB
{
namespace Streaming
{
class ParserPauseMaterializedViewQuery final : public IParserBase
{
protected:
    const char * getName() const override { return "PAUSE MATERIALIZED VIEW query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};

class ParserUnpauseMaterializedViewQuery final : public IParserBase
{
protected:
    const char * getName() const override { return "UNPAUSE MATERIALIZED VIEW query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};
}
}
