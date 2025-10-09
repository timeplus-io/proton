#pragma once

#include <TableFunctions/TableFunctionProxyBase.h>

namespace DB
{
class ASTFunction;

namespace Streaming
{
class TableFunctionRowify final : public TableFunctionProxyBase
{
public:
    explicit TableFunctionRowify(const String & name_);

private:
    const char * getStorageTypeName() const override { return "rowify"; }
    void parseArguments(const ASTPtr & func_ast, ContextPtr context) override;

private:
    String help_message;
};
}
}
