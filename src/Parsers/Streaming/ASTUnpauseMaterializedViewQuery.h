#pragma once

#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{
namespace Streaming
{
class ASTUnpauseMaterializedViewQuery final : public ASTQueryWithOutput
{
public:
    String getID(char) const override { return "UnpauseMaterializedViewQuery"; }
    ASTPtr clone() const override;
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    ASTPtr mvs;
};
}
}
