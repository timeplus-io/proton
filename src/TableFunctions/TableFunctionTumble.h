#pragma once

#include "TableFunctionStreamingWindow.h"

namespace DB
{
class TableFunctionTumble final : public TableFunctionStreamingWindow
{
public:
    explicit TableFunctionTumble(const String & name_);

private:
    const char * getStorageTypeName() const override { return "tumble"; }
    void parseArguments(const ASTPtr & func_ast, ContextPtr context) override;
    void handleResultType(const ColumnWithTypeAndName & type_and_name) override;
};
}
