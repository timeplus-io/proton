#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// Query like this:
/// CREATE [OR REPLACE] DISK [IF NOT EXISTS] name disk(type = ..., )
class ParserCreateDiskQuery : public DB::IParserBase
{
protected:
    const char * getName() const override { return "CREATE DISK query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};

}
