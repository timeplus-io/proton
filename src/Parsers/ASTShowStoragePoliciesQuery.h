#pragma once


#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/IAST.h>

namespace DB
{


/** Query SHOW STORAGE POLICIES
  */
class ASTShowStoragePoliciesQuery : public ASTQueryWithOutput
{
public:
    String like;

    bool not_like{false};
    bool case_insensitive_like{false};

    ASTPtr where_expression;
    ASTPtr limit_length;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "ShowStoragePolicies"; }

    ASTPtr clone() const override;

protected:
    void formatLike(const FormatSettings & settings) const;
    void formatLimit(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const;
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
