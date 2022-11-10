#pragma once

#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{
namespace Streaming
{
class ASTRecoverQuery final : public ASTQueryWithOutput
{
public:

    String getID(char) const override { return "RecoverQuery"; }
    ASTPtr clone() const override;
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr query_id;
};
}
}
