#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// Query like this:
/// DROP EXTERNAL TABLE [IF NOT EXISTS] [db.]name
class ParserDropExternalTableQuery : public DB::IParserBase
{
protected:
    const char * getName() const override { return "DROP EXTERNAL TABLE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};

}
