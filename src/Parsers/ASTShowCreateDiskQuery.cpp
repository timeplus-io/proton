#include <Parsers/ASTShowCreateDiskQuery.h>

#include <Parsers/ASTIdentifier.h>
#include <Common/quoteString.h>

namespace DB
{
String ASTShowCreateDiskQuery::getDiskName() const
{
    String name;
    tryGetIdentifierNameInto(disk_name, name);
    return name;
}

ASTPtr ASTShowCreateDiskQuery::clone() const
{
    auto res = std::make_shared<ASTShowCreateDiskQuery>(*this);
    res->children.clear();
    if (disk_name)
    {
        res->disk_name = disk_name->clone();
        res->children.push_back(res->disk_name);
    }
    return res;
}

void ASTShowCreateDiskQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW CREATE DISK " << (settings.hilite ? hilite_none : "")
                  << backQuoteIfNeed(getDiskName());
}
}
