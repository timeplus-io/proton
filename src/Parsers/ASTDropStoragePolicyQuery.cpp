#include <Parsers/ASTDropStoragePolicyQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTDropStoragePolicyQuery::clone() const
{
    return std::make_shared<ASTDropStoragePolicyQuery>(*this);
}

void ASTDropStoragePolicyQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "DROP STORAGE POLICY ";

    if (if_exists)
        settings.ostr << "IF EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "");
    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(name) << (settings.hilite ? hilite_none : "");
    formatOnCluster(settings);
}

}
