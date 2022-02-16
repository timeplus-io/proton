#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// CREATE|ATTACH MATERIALIZED VIEW [IF NOT EXISTS] [db.]name [UUID 'uuid'] [INTO [db.]name] [view_properties] AS SELECT ...
class ParserCreateMaterializedViewQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE MATERIALIZED VIEW query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
