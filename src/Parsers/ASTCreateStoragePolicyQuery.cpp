#include <Common/quoteString.h>
#include "Parsers/ASTIdentifier_fwd.h"
#include <IO/Operators.h>
#include <Parsers/ASTCreateStoragePolicyQuery.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{
ASTPtr ASTCreateStoragePolicyQuery::clone() const
{
    auto res = std::make_shared<ASTCreateStoragePolicyQuery>(*this);
    res->children.clear();

    res->name = name;

    res->yaml_config = yaml_config;
    return res;
}

void ASTCreateStoragePolicyQuery::formatImpl(const IAST::FormatSettings & settings, IAST::FormatState &  /*state*/, IAST::FormatStateStacked  /*frame*/) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "CREATE ";

    if (or_replace)
        settings.ostr << "OR REPLACE ";

    settings.ostr << "STORAGE POLICY ";

    if (if_not_exists)
        settings.ostr << "IF NOT EXISTS ";

    settings.ostr << (settings.hilite ? hilite_none : "");

    settings.ostr << (settings.hilite ? hilite_identifier : "") << backQuoteIfNeed(name) << (settings.hilite ? hilite_none : "");

    formatOnCluster(settings);

    settings.ostr << (settings.hilite ? hilite_keyword : "") << " AS" << (settings.hilite ? hilite_none : "");

    /// Do not format the source of the schema.
    settings.ostr << fmt::format("\n$$\n{}\n$$\n", yaml_config);
}
}
