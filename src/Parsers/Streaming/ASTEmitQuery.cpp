#include <IO/Operators.h>
#include <Parsers/Streaming/ASTEmitQuery.h>
#include <Common/SipHash.h>

namespace DB
{
void ASTEmitQuery::formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const
{
    const char * extra_space = "";
    if (stream_mode)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << magic_enum::enum_name(stream_mode.value())
                    << (format.hilite ? hilite_none : "");
        extra_space = " ";
    }

    if (last_interval)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << extra_space << "LAST " << (format.hilite ? hilite_none : "");
        last_interval->format(format);

        if (proc_time)
            format.ostr << (format.hilite ? hilite_keyword : "") << " ON PROCTIME" << (format.hilite ? hilite_none : "");

        return;
    }

    if (key_max_span_interval)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << extra_space << "AFTER KEY EXPIRE" << (format.hilite ? hilite_none : "");

        if (key_ts_col)
        {
            format.ostr << (format.hilite ? hilite_keyword : "") << " IDENTIFIED BY " << (format.hilite ? hilite_none : "");
            key_ts_col->format(format);
        }

        if (only_max_span)
            format.ostr << (format.hilite ? hilite_keyword : "") << " WITH ONLY MAXSPAN " << (format.hilite ? hilite_none : "");
        else
            format.ostr << (format.hilite ? hilite_keyword : "") << " WITH MAXSPAN " << (format.hilite ? hilite_none : "");

        key_max_span_interval->format(format);

        if (timeout_interval)
        {
            format.ostr << (format.hilite ? hilite_keyword : "") << " AND TIMEOUT " << (format.hilite ? hilite_none : "");
            timeout_interval->format(format);
        }

        return;
    }

    if (after_window_close)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << extra_space << "AFTER WINDOW CLOSE" << (format.hilite ? hilite_none : "");
    }
    else if (periodic_interval)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << extra_space << "PERIODIC " << (format.hilite ? hilite_none : "");
        periodic_interval->format(format);

        if (repeat)
            format.ostr << (format.hilite ? hilite_keyword : "") << " REPEAT";
    }
    else if (on_update)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << extra_space << "ON UPDATE" << (format.hilite ? hilite_none : "");

        if (batch_interval)
        {
            format.ostr << (format.hilite ? hilite_keyword : "") << " WITH BATCH " << (format.hilite ? hilite_none : "");
            batch_interval->format(format);
        }
    }
    else if (per_event)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << extra_space << "PER EVENT" << (format.hilite ? hilite_none : "");
    }
    /// For backward compatibility: `EMIT TIMEOUT 30s;`
    else if (timeout_interval)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << extra_space << "TIMEOUT " << (format.hilite ? hilite_none : "");
        timeout_interval->format(format);
        return;
    }

    if (delay_interval)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << " WITH DELAY " << (format.hilite ? hilite_none : "");
        delay_interval->format(format);
    }

    if (timeout_interval)
    {
        format.ostr << (format.hilite ? hilite_keyword : "") << " AND TIMEOUT " << (format.hilite ? hilite_none : "");
        timeout_interval->format(format);
    }
}

ASTPtr ASTEmitQuery::clone() const
{
    auto ast = std::make_shared<ASTEmitQuery>(*this);
    if (periodic_interval)
        ast->periodic_interval = periodic_interval->clone();

    if (batch_interval)
        ast->batch_interval = batch_interval->clone();

    if (delay_interval)
        ast->delay_interval = delay_interval->clone();

    if (timeout_interval)
        ast->timeout_interval = timeout_interval->clone();

    if (last_interval)
        ast->last_interval = last_interval->clone();

    if (key_ts_col)
        ast->key_ts_col = key_ts_col->clone();

    if (key_max_span_interval)
        ast->key_max_span_interval = key_max_span_interval->clone();

    return ast;
}

void ASTEmitQuery::updateTreeHashImpl(SipHash & hash_state) const
{
    if (stream_mode)
        hash_state.update(stream_mode.value());

    hash_state.update(after_window_close);

    if (delay_interval)
        delay_interval->updateTreeHashImpl(hash_state);

    if (periodic_interval)
        periodic_interval->updateTreeHashImpl(hash_state);

    hash_state.update(repeat);

    hash_state.update(on_update);

    hash_state.update(per_event);

    if (batch_interval)
        batch_interval->updateTreeHashImpl(hash_state);

    if (timeout_interval)
        timeout_interval->updateTreeHashImpl(hash_state);

    if (last_interval)
        last_interval->updateTreeHashImpl(hash_state);

    hash_state.update(proc_time);

    if (key_ts_col)
        key_ts_col->updateTreeHashImpl(hash_state);

    if (key_max_span_interval)
        key_max_span_interval->updateTreeHashImpl(hash_state);

    hash_state.update(only_max_span);

    IAST::updateTreeHashImpl(hash_state);
}

}
