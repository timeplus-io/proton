#pragma once

#include <Parsers/IAST.h>

namespace DB
{
struct ASTEmitQuery : public IAST
{
public:
    enum Mode
    {
        NONE,
        TAIL,
        PERIODIC,
        DELAY,
        WATERMARK,
        WATERMARK_WITH_DELAY,
    };

    bool streaming = false;
    Mode mode = NONE;
    ASTPtr interval;

    String getID(char) const override { return "Emit"; }

    ASTPtr clone() const override { return std::make_shared<ASTEmitQuery>(*this); }

    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;

    void updateTreeHashImpl(SipHash & hash_state) const override;
};
}
