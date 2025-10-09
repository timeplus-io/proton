#include <Parsers/ASTShowStoragePoliciesQuery.h>

namespace DB
{

ASTPtr ASTShowStoragePoliciesQuery::clone() const
{
    auto res = std::make_shared<ASTShowStoragePoliciesQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}

void ASTShowStoragePoliciesQuery::formatLike(const FormatSettings & settings) const
{
    if (!like.empty())
        settings.ostr << (settings.hilite ? hilite_keyword : "") << (not_like ? " NOT" : "")
                      << (case_insensitive_like ? " ILIKE " : " LIKE ") << (settings.hilite ? hilite_none : "") << DB::quote << like;
}

void ASTShowStoragePoliciesQuery::formatLimit(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (limit_length)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " LIMIT " << (settings.hilite ? hilite_none : "");
        limit_length->formatImpl(settings, state, frame);
    }
}

void ASTShowStoragePoliciesQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW STORAGE POLICIES " << (settings.hilite ? hilite_none : "");

    formatLike(settings);

    if (where_expression)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " WHERE " << (settings.hilite ? hilite_none : "");
        where_expression->formatImpl(settings, state, frame);
    }

    formatLimit(settings, state, frame);
}

}
