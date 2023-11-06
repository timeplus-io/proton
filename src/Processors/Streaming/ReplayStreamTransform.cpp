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

ReplayStreamTransform::ReplayStreamTransform(const Block & header, Float32 replay_speed_, const Int64 & shard_last_sn_)
    : ISimpleTransform(header, header, true, ProcessorID::ReplayStreamTransformID)
    , replay_speed(replay_speed_)
    , shard_last_sn(shard_last_sn_)
{
    append_time_index = header.getPositionByName(ProtonConsts::RESERVED_APPEND_TIME);
    sn_index = header.getPositionByName(ProtonConsts::RESERVED_EVENT_SEQUENCE_ID);
    if (shard_last_sn < 0)
        enable_replay = false;
}

void ReplayStreamTransform::transform(Chunk & chunk)
{
    if (!chunk.rows() || !enable_replay)
        return;

    /// get the time of this chunk.
    const auto & this_batch_time = chunk.getColumns()[append_time_index]->get64(0);
    size_t size = chunk.getColumns()[sn_index]->size();
    Int64 this_batch_last_sn = chunk.getColumns()[sn_index]->getInt(size - 1);

    /// mark the historical data replay end and begin stream query.
    if (this_batch_last_sn >= shard_last_sn)
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
