#pragma once

#include <Processors/ISimpleTransform.h>

namespace DB
{

/// Implement process time filtering like WHERE _tp_time > now() - interval
class ProcessTimeFilter  : public ISimpleTransform
{
public:
    ProcessTimeFilter(
        const String & column_name_,
        Int64 interval_seconds_,
        const Block & header);

    ~ProcessTimeFilter() override = default;

    String getName() const override { return "ProcessTimeFilter"; }

private:
    void transform(Chunk & chunk) override;

private:
    String column_name;
    Int64 interval_seconds;

    DataTypePtr timestamp_col_data_type;
    UInt32 timestamp_col_pos = 0;
    bool is_datetime64 = false;

    String timezone;
    Int32 scale = 0;
    Int32 multiplier = 1;
};
}
