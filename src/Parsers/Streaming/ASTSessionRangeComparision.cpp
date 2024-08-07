#include "ASTSessionRangeComparision.h"

#include <IO/Operators.h>
#include <Common/SipHash.h>

namespace DB
{
ASTPtr ASTSessionRangeComparision::clone() const
{
    auto res = std::make_shared<ASTSessionRangeComparision>(*this);
    res->children.clear();
    for (auto & child : children)
        res->children.push_back(child->clone());

    return res;
}

void ASTSessionRangeComparision::updateTreeHashImpl(SipHash & hash_state) const
{
    assert(children.size() == 2);

    children[0]->updateTreeHashImpl(hash_state);
    children[1]->updateTreeHashImpl(hash_state);

    hash_state.update(start_with_inclusion);
    hash_state.update(end_with_inclusion);

    IAST::updateTreeHashImpl(hash_state);
}

void ASTSessionRangeComparision::formatImplWithoutAlias(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    assert(children.size() == 2);

    settings.ostr << (settings.hilite ? hilite_keyword : "") << (start_with_inclusion ? "[" : "(") << (settings.hilite ? hilite_none : "");
    children[0]->format(settings);
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "," << (settings.hilite ? hilite_none : "");
    children[1]->format(settings);
    settings.ostr << (settings.hilite ? hilite_keyword : "") << (end_with_inclusion ? "]" : ")") << (settings.hilite ? hilite_none : "");
}

void ASTSessionRangeComparision::appendColumnNameImpl(WriteBuffer & ostr) const
{
    assert(children.size() == 2);

    writeString((start_with_inclusion ? "[" : "("), ostr);
    children[0]->appendColumnName(ostr);
    writeString(", ", ostr);
    children[1]->appendColumnName(ostr);
    writeString((end_with_inclusion ? "]" : ")"), ostr);
}

}
