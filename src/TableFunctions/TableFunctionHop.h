#pragma once

#include "TableFunctionProxyBase.h"

namespace DB
{
class TableFunctionHop final : public TableFunctionProxyBase
{
public:
    explicit TableFunctionHop(const String & name_);

private:
    const char * getStorageTypeName() const override { return "hop"; }
    void parseArguments(const ASTPtr & func_ast, ContextPtr context) override;
    DataTypePtr getElementType(const DataTypeTuple * tuple) const override;
};
}
