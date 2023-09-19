#include <Parsers/Streaming/ASTJavaScriptFunction.h>

#include <IO/Operators.h>
#include <Common/SipHash.h>

namespace DB
{
void ASTJavaScriptFunction::formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    format.ostr << (format.hilite ? hilite_keyword : "") << "$$ " << (format.hilite ? hilite_none : "") << "\n";
    format.ostr << source << "\n";
    format.ostr << (format.hilite ? hilite_keyword : "") << "$$" << (format.hilite ? hilite_none : "");
}

void ASTJavaScriptFunction::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(source);

    IAST::updateTreeHashImpl(hash_state);
}

}
