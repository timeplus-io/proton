#include <Processors/Streaming/ReplayStreamTransform.h>

#include <Processors/ISimpleTransform.h>
#include <Processors/ProcessorID.h>
#include <base/ClockUtils.h>
#include <base/types.h>
#include <Common/ProtonCommon.h>

#include <thread>


namespace DB
{
namespace Streaming
{

constexpr Int64 MAX_WAIT_INTERVAL_MS = 500;

ReplayStreamTransform::ReplayStreamTransform(const Block & header, Float32 replay_speed_, Int64 last_sn_)
    : ISimpleTransform(header, header, true, ProcessorID::ReplayStreamTransformID)
    , replay_speed(replay_speed_)
    , last_sn(last_sn_)
    , enable_replay(last_sn >= 0)
{
    append_time_index = header.getPositionByName(ProtonConsts::RESERVED_APPEND_TIME);
    sn_index = header.getPositionByName(ProtonConsts::RESERVED_EVENT_SEQUENCE_ID);
}

void ReplayStreamTransform::transform(Chunk & chunk)
{
    if (!enable_replay || !chunk.rows())
        return;

    /// get the time of this chunk.
    const auto & columns = chunk.getColumns();
    auto this_batch_time = columns[append_time_index]->getInt(0);
    auto this_batch_last_sn = columns[sn_index]->getInt(chunk.rows() - 1);

    /// mark the historical data replay end and begin stream query.
    if (this_batch_last_sn >= last_sn)
        enable_replay = false;

    wait_interval_ms
        = static_cast<Int64>(std::lround((last_batch_time.has_value() ? this_batch_time - last_batch_time.value() : 0) / replay_speed));
    last_batch_time = this_batch_time;

    while (wait_interval_ms > 0)
    {
        if (isCancelled())
            return;

        auto sleep_interval_ms = wait_interval_ms > MAX_WAIT_INTERVAL_MS ? MAX_WAIT_INTERVAL_MS : wait_interval_ms;
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_interval_ms));
        wait_interval_ms -= sleep_interval_ms;
    }
}
}
}
