#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// CREATE|ATTACH STREAMING VIEW [IF NOT EXISTS] [db.]name [UUID 'uuid'] [INTO [db.]name] [view_properties] AS SELECT ...
class ParserCreateStreamingViewQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE STREAMING VIEW query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
