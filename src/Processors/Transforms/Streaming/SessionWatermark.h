#pragma once

#include "HopTumbleBaseWatermark.h"

#include <Columns/IColumn.h>

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
    SessionWatermark(const SessionWatermark &) = default;
    ~SessionWatermark() override = default;

    WatermarkPtr clone() const override { return std::make_unique<SessionWatermark>(*this); }

private:
    void doProcess(Block & block) override;

    void handleIdlenessWatermark(Block & block) override;

    ExpressionActionsPtr start_actions;
    ExpressionActionsPtr end_actions;

    std::vector<size_t> start_required_pos;
    std::vector<size_t> end_required_pos;
};
}
}
