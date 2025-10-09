#include <Parsers/ASTShowDisksQuery.h>

namespace DB
{

ASTPtr ASTShowDisksQuery::clone() const
{
    auto res = std::make_shared<ASTShowDisksQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    return res;
}

void ASTShowDisksQuery::formatLike(const FormatSettings & settings) const
{
    if (!like.empty())
        settings.ostr
            << (settings.hilite ? hilite_keyword : "")
            << (not_like ? " NOT" : "")
            << (case_insensitive_like ? " ILIKE " : " LIKE ")
            << (settings.hilite ? hilite_none : "")
            << DB::quote << like;
}

void ASTShowDisksQuery::formatLimit(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (limit_length)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " LIMIT " << (settings.hilite ? hilite_none : "");
        limit_length->formatImpl(settings, state, frame);
    }
}

void ASTShowDisksQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{

    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW DISKS" <<  (settings.hilite ? hilite_none : "");

    formatLike(settings);

    if (where_expression)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << " WHERE " << (settings.hilite ? hilite_none : "");
        where_expression->formatImpl(settings, state, frame);
    }

    formatLimit(settings, state, frame);
}

}
