#include <Parsers/ASTDropDiskQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTDropDiskQuery::clone() const
{
    return std::make_shared<ASTDropDiskQuery>(*this);
}

void ASTDropDiskQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "DROP DISK ";

    if (if_exists)
        settings.ostr << "IF EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "");
    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(name) << (settings.hilite ? hilite_none : "");
    formatOnCluster(settings);
}

}
