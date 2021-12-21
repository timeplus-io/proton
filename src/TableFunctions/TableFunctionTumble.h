#pragma once

#include "TableFunctionProxyBase.h"

namespace DB
{
class TableFunctionTumble final : public TableFunctionProxyBase
{
public:
    explicit TableFunctionTumble(const String & name_);

private:
    const char * getStorageTypeName() const override { return "tumble"; }
    void parseArguments(const ASTPtr & func_ast, ContextPtr context) override;
    DataTypePtr getElementType(const DataTypeTuple * tuple) const override;
};
}
