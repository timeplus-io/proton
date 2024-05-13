
#include <Parsers/IParserBase.h>

namespace DB
{
namespace Streaming
{
class ParserMaterializedViewCommandQuery final : public IParserBase
{
protected:
    const char * getName() const override { return "MATERIALIZED VIEW command query"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};
}
}
