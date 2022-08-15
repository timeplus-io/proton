#pragma once

#include <Interpreters/Streaming/WindowCommon.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{
namespace Streaming
{
/// Implement process time filtering like WHERE _tp_time > now() - interval
class ProcessTimeFilter  : public ISimpleTransform
{
public:
    ProcessTimeFilter(
        const String & column_name_,
        BaseScaleInterval interval_bs_,
        const Block & header);

    ~ProcessTimeFilter() override = default;

    String getName() const override { return "ProcessTimeFilter"; }

private:
    void transform(Chunk & chunk) override;

private:
    String column_name;
    BaseScaleInterval interval_bs;

    DataTypePtr timestamp_col_data_type;
    UInt32 timestamp_col_pos = 0;
    bool is_datetime64 = false;

    String timezone;
    Int32 scale = 0;
    Int32 multiplier = 1;
};
}
}
