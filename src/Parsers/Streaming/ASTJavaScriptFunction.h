#pragma once

#include <Parsers/IAST.h>

namespace DB
{
struct ASTJavaScriptFunction : public IAST
{
public:
    /// source code of javascript user defined function
    String source;

    String getID(char) const override { return "JavaScriptFunction"; }

    ASTPtr clone() const override { return std::make_shared<ASTJavaScriptFunction>(*this); }

    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;

    void updateTreeHashImpl(SipHash & hash_state) const override;
};
}
