#pragma once

#include <TableFunctions/TableFunctionProxyBase.h>

namespace DB
{
namespace Streaming
{
class TableFunctionHist final : public TableFunctionProxyBase
{
public:
    explicit TableFunctionHist(const String & name_);

private:
    const char * getStorageTypeName() const override { return "table"; }
    void parseArguments(const ASTPtr & func_ast, ContextPtr context) override;
    StoragePtr calculateColumnDescriptions(ContextPtr context) override;
    bool supportsStreamingQuery() const { return false; }

private:
    String help_message;
};
}
}
