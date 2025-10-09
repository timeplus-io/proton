#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{

class ASTShowCreateTaskQuery : public ASTQueryWithOutput
{
public:
    ASTPtr database;
    ASTPtr name;

    String getDatabase() const;
    String getName() const;

    String getID(char /*delim*/) const override { return "SHOW CREATE TASK"; }

    ASTPtr clone() const override;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};
}
