#pragma once

#include "TableFunctionStreamingWindow.h"

namespace DB
{
class TableFunctionHop final : public TableFunctionStreamingWindow
{
public:
    explicit TableFunctionHop(const String & name_);

private:
    const char * getStorageTypeName() const override { return "hop"; }
    void parseArguments(const ASTPtr & func_ast, ContextPtr context) override;
    void handleResultType(const ColumnWithTypeAndName & type_and_name) override;
};
}
