#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/IAST.h>

namespace DB
{
class ASTShowCreateDiskQuery : public ASTQueryWithOutput
{
public:
    ASTPtr disk_name;

    String getID(char) const override { return "ShowCreateDiskQuery"; }
    ASTPtr clone() const override;
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
    String getDiskName() const;
};
}
