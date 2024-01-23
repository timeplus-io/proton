#include <IO/Operators.h>
#include <Parsers/Streaming/ASTEmitQuery.h>
#include <Common/SipHash.h>

namespace DB
{
void ASTEmitQuery::formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    format.ostr << (format.hilite ? hilite_keyword : "") << magic_enum::enum_name(stream_mode) << " " << (format.hilite ? hilite_none : "");

    int elems = 0;
    if (watermark_strategy != Streaming::WatermarkStrategy::Unknown)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << "AFTER WATERMARK" << (format.hilite ? hilite_none : "");
        if (watermark_strategy == Streaming::WatermarkStrategy::Ascending)
        {
            format.ostr << (format.hilite ? hilite_keyword : "") << " WITHOUT DELAY" << (format.hilite ? hilite_none : "");
        }
        else if (watermark_strategy == Streaming::WatermarkStrategy::BoundedOutOfOrderness)
        {
            format.ostr << (format.hilite ? hilite_keyword : "") << " WITH DELAY " << (format.hilite ? hilite_none : "");
            delay_interval->format(format);
        }
        ++elems;
    }

    if (periodic_interval)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << (elems ? " AND " : "") << "PERIODIC " << (format.hilite ? hilite_none : "");
        periodic_interval->format(format);
        ++elems;
    }

    if (on_update)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << (elems ? " " : "") << "ON UPDATE" << (format.hilite ? hilite_none : "");
        ++elems;
    }

    if (timeout_interval)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << (elems ? " AND " : "") << "TIMEOUT " << (format.hilite ? hilite_none : "");
        timeout_interval->format(format);
        ++elems;
    }

    if (last_interval)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << (elems ? " AND " : "") << "LAST " << (format.hilite ? hilite_none : "");
        last_interval->format(format);
        ++elems;

        if (proc_time)
            format.ostr << (format.hilite ? hilite_keyword : "") << " ON PROCTIME" << (format.hilite ? hilite_none : "");
    }

}

void ASTEmitQuery::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(stream_mode);

    hash_state.update(watermark_strategy);

    if (delay_interval)
        delay_interval->updateTreeHashImpl(hash_state);

    if (periodic_interval)
        periodic_interval->updateTreeHashImpl(hash_state);

    hash_state.update(on_update);

    if (timeout_interval)
        timeout_interval->updateTreeHashImpl(hash_state);

    if (last_interval)
        last_interval->updateTreeHashImpl(hash_state);

    hash_state.update(proc_time);

    IAST::updateTreeHashImpl(hash_state);
}

}
