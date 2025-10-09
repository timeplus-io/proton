#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{
/// SHOW ALERTS [FROM <db>]
class ASTShowAlertsQuery : public ASTQueryWithOutput
{
public:
    std::optional<String> database;

    String getID(char) const override { return "ShowAlertsQuery"; }
    ASTPtr clone() const override;
    void formatQueryImpl(const FormatSettings &, FormatState &, FormatStateStacked) const override;
};
}
