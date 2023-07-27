#pragma once

#include <TableFunctions/TableFunctionProxyBase.h>

#include <Parsers/IAST_fwd.h>

namespace DB
{
class ASTFunction;

namespace Streaming
{
class TableFunctionChangelog final : public TableFunctionProxyBase
{
public:
    explicit TableFunctionChangelog(const String & name_);

private:
    const char * getStorageTypeName() const override { return "changelog"; }
    void parseArguments(const ASTPtr & func_ast, ContextPtr context) override;
    std::pair<ASTs, std::optional<bool>> checkAndExtractArguments(ASTFunction * node) const;

private:
    String help_message;
};
}
}
