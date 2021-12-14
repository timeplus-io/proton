#include <Parsers/ASTEmitQuery.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>

namespace DB
{
void ASTEmitQuery::formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    if (streaming)
        format.ostr << (format.hilite ? hilite_keyword : "") << "STREAM " << (format.hilite ? hilite_none : "");

    int elems = 0;

    if (periodic_interval)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << (elems ? " AND " : "") << "PERIODIC " << (format.hilite ? hilite_none : "");
        periodic_interval->format(format);
        ++elems;
    }

    if (after_watermark)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << (elems ? " AND " : "") << "AFTER WATERMARK " << (format.hilite ? hilite_none : "");
        ++elems;
    }

    if (delay_interval)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << (elems ? " AND " : "") << "DELAY " << (format.hilite ? hilite_none : "");
        delay_interval->format(format);
        ++elems;
    }

    if (last_interval)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << (elems ? " AND " : "") << "LAST " << (format.hilite ? hilite_none : "");
        last_interval->format(format);
        ++elems;
    }
}

void ASTEmitQuery::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(streaming);
    hash_state.update(after_watermark);

    if (periodic_interval)
        periodic_interval->updateTreeHashImpl(hash_state);

    if (delay_interval)
        delay_interval->updateTreeHashImpl(hash_state);
    
    if (last_interval)
        last_interval->updateTreeHashImpl(hash_state);

    IAST::updateTreeHashImpl(hash_state);
}

}
