#pragma once

#include "TableFunctionWindow.h"

namespace DB
{
class TableFunctionTumble final : public TableFunctionWindow
{
public:
    explicit TableFunctionTumble(const String & name_);

private:
    const char * getStorageTypeName() const override { return "tumble"; }
    void parseArguments(const ASTPtr & func_ast, ContextPtr context) override;
    DataTypePtr getElementType(const DataTypeTuple * tuple) const override;
    ASTs checkAndExtractArguments(ASTFunction * node) const override;
    void postArgs(ASTs & args) const override;
    String functionNamePrefix() const override;
};
}
