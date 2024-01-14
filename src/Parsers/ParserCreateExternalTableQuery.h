#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// Query like this:
/// CREATE [OR REPLACE] EXTERNAL TABLE [IF NOT EXISTS] [db.]name
/// [SETTINGS name = value, ...]
class ParserCreateExternalTableQuery : public DB::IParserBase
{
protected:
    const char * getName() const override { return "CREATE EXTERNAL TABLE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};

}
