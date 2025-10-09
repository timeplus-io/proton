#include <Common/PackedTimeBucket.h>

#include <base/defines.h>

namespace DB
{
/// packedFixedBatch(...) in AggregatingCommon.h packed the continuous integral keys in the descending order of bit width
/// So the window key can be shuffled to different offset instead of offset-0
/// This function figure outs how many bytes will be encoded before the window key because of the shuffling
/// We assume the window key is the first element in the \key_sizes
size_t timeBucketPackedOffset(const std::vector<size_t> & key_sizes, size_t key_bytes)
{
    /// Logic from HashMethodKeysFixed::usePreparedKeys
    if (key_bytes > 16)
        /// Fallback to
        return 0;

    for (auto size : key_sizes)
        if (size != 1 && size != 2 && size != 4 && size != 8 && size != 16)
            return 0;

    chassert(!key_sizes.empty());

    /// ColumnsHashing packFixedBatch
    size_t window_key_bytes = key_sizes[0];

    size_t offset = 0;
    for (size_t i = 1; i < key_sizes.size(); ++i)
    {
        if (key_sizes[i] > window_key_bytes)
            offset += key_sizes[i];
    }

    return offset;
}
}
