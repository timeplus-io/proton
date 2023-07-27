#pragma once

#include <TableFunctions/TableFunctionProxyBase.h>

#include <Parsers/IAST_fwd.h>

namespace DB
{
class ASTFunction;

namespace Streaming
{
class TableFunctionDedup final : public TableFunctionProxyBase
{
public:
    explicit TableFunctionDedup(const String & name_);

private:
    const char * getStorageTypeName() const override { return "dedup"; }
    void parseArguments(const ASTPtr & func_ast, ContextPtr context) override;
    ASTs checkAndExtractArguments(ASTFunction * node) const;

private:
    String help_message;
};
}
}
