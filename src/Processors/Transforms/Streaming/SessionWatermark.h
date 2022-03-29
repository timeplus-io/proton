#pragma once

#include "HopTumbleBaseWatermark.h"

#include <Columns/IColumn.h>
#include <Interpreters/Streaming/SessionMap.h>

namespace DB
{
class SessionWatermark : public HopTumbleBaseWatermark
{
public:
    explicit SessionWatermark(WatermarkSettings && watermark_settings_, bool proc_time_, Poco::Logger * log);
    ~SessionWatermark() override = default;

private:
    void doProcess(Block & block) override;

    void handleIdlenessWatermark(Block & block) override;

    SessionHashMap::Type chooseBlockCacheMethod();

    SessionHashMap session_map;
    SessionHashMap::Type method_chosen;
    Sizes key_sizes; /// sizes of key columns
};
}
