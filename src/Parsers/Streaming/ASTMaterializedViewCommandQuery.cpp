#include "ASTMaterialiedViewCommandQuery.h"

#include <IO/Operators.h>

namespace DB
{
namespace Streaming
{
ASTPtr ASTMaterialiedViewCommandQuery::clone() const
{
    auto res = std::make_shared<ASTMaterialiedViewCommandQuery>();
    res->type = type;
    res->sync = sync;
    res->mvs = mvs->clone();
    res->children.push_back(res->mvs);
    return res;
}

void ASTMaterialiedViewCommandQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << Poco::toUpper(magic_enum::enum_name(type)) << " MATERIALIZED VIEW ";
    if (mvs)
        mvs->formatImpl(settings, state, frame);

    settings.ostr << (settings.hilite ? hilite_keyword : "") << (sync ? " SYNC" : " ASYNC");
}
}
}
