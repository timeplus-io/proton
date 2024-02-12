#pragma once

#include <Parsers/IAST.h>
#include <Core/Streaming/Watermark.h>

namespace DB
{
struct ASTEmitQuery : public IAST
{
public:
    enum StreamMode {
        STREAM,
        CHANGELOG
    };
    StreamMode stream_mode = StreamMode::STREAM;

    /// [AFTER WATERMARK]
    bool after_watermark = false;

    /// [WITH DELAY INTERVAL 1 SECOND].
    ASTPtr delay_interval;

    /// [PERIODIC INTERVAL 1 SECOND]
    ASTPtr periodic_interval;

    /// [ON UPDATE]
    bool on_update = false;

    /// [TIMEOUT INTERVAL 5 SECOND]
    ASTPtr timeout_interval;
    /// [LAST <last-x> [ON PROCTIME]]]
    ASTPtr last_interval;
    bool proc_time = false; /// Proc time or event time processing.

    String getID(char) const override { return "Emit"; }

    ASTPtr clone() const override { return std::make_shared<ASTEmitQuery>(*this); }

    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;

    void updateTreeHashImpl(SipHash & hash_state) const override;
};
}
