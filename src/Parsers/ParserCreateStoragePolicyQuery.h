#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

/// Query like this:
/// CREATE [OR REPLACE] STORAGE POLICY [IF NOT EXISTS] name AS $$
///     volumes:
///         hot:
///             disk: default
///         cold:
///             disk: minio
///
/// $$;
class ParserCreateStoragePolicyQuery : public DB::IParserBase
{
protected:
    const char * getName() const override { return "CREATE STORAGE POLICY query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint) override;
};

}
