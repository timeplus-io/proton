#pragma once

#include "TableFunctionProxyBase.h"

namespace DB
{
class TableFunctionSession final : public TableFunctionProxyBase
{
public:
    explicit TableFunctionSession(const String & name_);

private:
    const char * getStorageTypeName() const override { return "session"; }
    void parseArguments(const ASTPtr & func_ast, ContextPtr context) override;
    DataTypePtr getElementType(const DataTypeTuple * tuple) const override;
    void handleResultType(const ColumnWithTypeAndName & type_and_name) override;
};
}
