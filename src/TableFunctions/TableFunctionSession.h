#pragma once

#include "TableFunctionWindow.h"

namespace DB
{
namespace Streaming
{
class TableFunctionSession final : public TableFunctionWindow
{
public:
    explicit TableFunctionSession(const String & name_);

private:
    const char * getStorageTypeName() const override { return "session"; }
    void parseArguments(const ASTPtr & func_ast, ContextPtr context) override;
    DataTypePtr getElementType(const DataTypeTuple * tuple) const override;
    void handleResultType(const ColumnWithTypeAndName & type_and_name) override;
    ASTs checkAndExtractArguments(ASTFunction * node) const override;
    String functionNamePrefix() const override;
    void validateWindow(FunctionDescriptionPtr desc) const override;
};
}
}
