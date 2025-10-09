#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSetQuery.h>


namespace DB
{
/// SHOW TASKS [FROM <db>]
class ASTShowTasksQuery : public ASTQueryWithOutput
{
public:
    std::optional<String> database;

    String getID(char) const override { return "ShowTasks"; }
    ASTPtr clone() const override;
    void formatQueryImpl(const FormatSettings &, FormatState &, FormatStateStacked) const override;
};
}
