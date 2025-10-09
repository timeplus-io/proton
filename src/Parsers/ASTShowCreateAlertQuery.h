#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{

class ASTShowCreateAlertQuery : public ASTQueryWithOutput
{
public:
    ASTPtr database;
    ASTPtr name;

    String getDatabase() const;
    String getName() const;

    String getID(char /*delim*/) const override { return "SHOW CREATE ALERT"; }

    ASTPtr clone() const override;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};
}
