#pragma once

#include <Parsers/ASTWithAlias.h>

namespace DB
{
struct ASTSessionRangeComparision : public ASTWithAlias
{
public:
    bool start_with_inclusion = true;
    bool end_with_inclusion = true;

    String getID(char) const override { return "SessionRangeComparision"; }

    ASTPtr clone() const override { return std::make_shared<ASTSessionRangeComparision>(*this); }

    void updateTreeHashImpl(SipHash & hash_state) const override;

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
    void appendColumnNameImpl(WriteBuffer & ostr) const override;
};
}
