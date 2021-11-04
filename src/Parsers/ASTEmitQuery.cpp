#include <Parsers/ASTEmitQuery.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>

namespace DB
{
void ASTEmitQuery::formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    if (streaming)
        format.ostr << (format.hilite ? hilite_keyword : "") << "STREAM " << (format.hilite ? hilite_none : "");

    switch (mode)
    {
        case Mode::NONE:
            break;
        case Mode::TAIL:
            break;
        case Mode::DELAY:
            format.ostr << (format.hilite ? hilite_keyword : "") << "AFTER DELAY " << (format.hilite ? hilite_none : "");
            interval->format(format);
            break;
        case Mode::PERIODIC:
            format.ostr << (format.hilite ? hilite_keyword : "") << "PERIODIC " << (format.hilite ? hilite_none : "");
            interval->format(format);
            break;
        case Mode::WATERMARK:
            format.ostr << (format.hilite ? hilite_keyword : "") << "AFTER WATERMARK " << (format.hilite ? hilite_none : "");
            break;
        case Mode::WATERMARK_WITH_DELAY:
            format.ostr << (format.hilite ? hilite_keyword : "") << "AFTER WATERMARK AND DELAY " << (format.hilite ? hilite_none : "");
            interval->format(format);
            break;
    }
}

void ASTEmitQuery::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(streaming);
    hash_state.update(mode);

    if (interval)
        interval->updateTreeHashImpl(hash_state);

    IAST::updateTreeHashImpl(hash_state);
}

}
