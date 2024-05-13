#pragma once

#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{
namespace Streaming
{
enum class MaterializedViewCommandType
{
    Pause,  /// Stop the pipeline execution but keep the background thread running
    Resume,  /// Resume the pipeline execution

    Abort,  /// Abort the pipeline execution and shutdown background thread
    Recover, /// Recover the pipeline execution
};

class ASTMaterialiedViewCommandQuery final : public ASTQueryWithOutput
{
public:
    String getID(char) const override { return "MaterialiedViewCommandQuery"; }
    ASTPtr clone() const override;
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    MaterializedViewCommandType type;
    bool sync = false;
    ASTPtr mvs;
};
}
}
