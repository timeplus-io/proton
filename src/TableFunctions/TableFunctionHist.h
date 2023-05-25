#pragma once

#include "TableFunctionProxyBase.h"

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
    String functionNamePrefix() const override { return "__table("; }

private:
    String help_message;
};
}
}
