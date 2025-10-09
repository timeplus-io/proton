#include <Common/quoteString.h>
#include "Parsers/ASTIdentifier_fwd.h"
#include <IO/Operators.h>
#include <Parsers/ASTCreateDiskQuery.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{
ASTPtr ASTCreateDiskQuery::clone() const
{
    auto res = std::make_shared<ASTCreateDiskQuery>(*this);
    res->children.clear();

    res->name = name;

    res->disk_func = disk_func->clone();
    res->children.push_back(res->disk_func);
    return res;
}

void ASTCreateDiskQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState &  state, IAST::FormatStateStacked  frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "CREATE ";

    if (or_replace)
        settings.ostr << "OR REPLACE ";

    settings.ostr << "DISK ";

    if (if_not_exists)
        settings.ostr << "IF NOT EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "");

    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(name) << (settings.hilite ? hilite_none : "");

    formatOnCluster(settings);

    settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS" << (settings.hilite ? hilite_none : "");

    settings.ostr << ' ';
    disk_func->formatImpl(settings, state, frame);
}
}
