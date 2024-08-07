#pragma once

#include <TableFunctions/TableFunctionWindow.h>

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
    ASTs checkAndExtractArguments(ASTFunction * node) const override;
    void postArgs(ASTs & args) const override;
    NamesAndTypesList getAdditionalResultColumns(const ColumnsWithTypeAndName & arguments) const final;
};
}
}
