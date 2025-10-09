#include <Common/ClickHouseCompatibleFlag.h>

#include <thread>

namespace DB
{

namespace
{
/// The SQL receiving / parsing thread can set this flag to true to enable Timeplus to run ClickHouse SQL
thread_local bool is_clickhouse_compatible_mode_ = false;
}

bool isClickHouseCompatibleMode()
{
    return is_clickhouse_compatible_mode_;
}

void setClickHouseCompatibleMode(bool is_clickhouse_compatible)
{
    is_clickhouse_compatible_mode_ = is_clickhouse_compatible;
}

}
