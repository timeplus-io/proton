#include <thread>
#include <Processors/ISimpleTransform.h>
#include <Processors/ProcessorID.h>
#include <Processors/Streaming/ReplayStreamTransform.h>
#include <base/ClockUtils.h>
#include <Common/ProtonCommon.h>
#include "base/types.h"
namespace DB
{
namespace Streaming
{

constexpr Int64 max_wait_interval = 500;

ReplayStreamTransform::ReplayStreamTransform(const Block & header, Float32 replay_speed_)
    : ISimpleTransform(header, header, true, ProcessorID::ReplayStreamTransformID), replay_speed(replay_speed_)
{
    append_time_index = header.getPositionByName(ProtonConsts::RESERVED_APPEND_TIME);
}

void ReplayStreamTransform::transform(Chunk & chunk)
{
    if (chunk.rows())
    {
        /// get the time of this chunk.
        const auto & this_batch_time = chunk.getColumns()[append_time_index]->get64(0);
        wait_interval = static_cast<Int64>((last_batch_time.has_value() ? this_batch_time - last_batch_time.value() : 0) / replay_speed);
        last_batch_time = this_batch_time;
    }
    else 
        /// in case of the empty chunk.
        return;

    while (wait_interval > 0)
    {
        if (isCancelled())
            return;

        std::this_thread::sleep_for(std::chrono::milliseconds(wait_interval > max_wait_interval ? max_wait_interval : wait_interval));
        wait_interval -= max_wait_interval;
    }
}

}
}
