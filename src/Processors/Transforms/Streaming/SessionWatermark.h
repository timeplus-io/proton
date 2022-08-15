#pragma once

#include "HopTumbleBaseWatermark.h"

#include <Columns/IColumn.h>
#include <Interpreters/Streaming/SessionMap.h>

namespace DB
{
namespace Streaming
{
class SessionWatermark : public HopTumbleBaseWatermark
{
public:
    explicit SessionWatermark(
        WatermarkSettings && watermark_settings_,
        bool proc_time_,
        ExpressionActionsPtr start_actions_,
        ExpressionActionsPtr end_actions_,
        Poco::Logger * log);
    ~SessionWatermark() override = default;

private:
    void doProcess(Block & block) override;

    void handleIdlenessWatermark(Block & block) override;

    SessionHashMap::Type chooseBlockCacheMethod();

    SessionHashMap session_map;
    SessionHashMap::Type method_chosen;

    ExpressionActionsPtr start_actions;
    ExpressionActionsPtr end_actions;

    std::vector<size_t> start_required_pos;
    std::vector<size_t> end_required_pos;

    Sizes key_sizes; /// sizes of key columns
};
}
}
