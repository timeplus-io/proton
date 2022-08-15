#pragma once

#include "TableFunctionWindow.h"

namespace DB
{
namespace Streaming
{
class TableFunctionHop final : public TableFunctionWindow
{
public:
    explicit TableFunctionHop(const String & name_);

private:
    const char * getStorageTypeName() const override { return "hop"; }
    void parseArguments(const ASTPtr & func_ast, ContextPtr context) override;
    DataTypePtr getElementType(const DataTypeTuple * tuple) const override;
    ASTs checkAndExtractArguments(ASTFunction * node) const override;
    void postArgs(ASTs & args) const override;
    String functionNamePrefix() const override;
};
}
}
