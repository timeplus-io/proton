#pragma once

#include <Core/Names.h>

#include <algorithm>
#include <cstdint>

namespace DB
{

/// Describes how rows of a data stream are partitioned across pipeline streams.
struct ShuffleDescription
{
    /// Ordered by strength — Substream subsumes Light (it also gives same-key-same-stream, plus a
    /// substream id), so `isShuffledBy` compares with `>=`. Keep Light first.
    enum class Kind : uint8_t
    {
        Light,     /// Hash-mod shuffling (LightShufflingStep); used to align with GROUP BY keys.
        Substream, /// Substream-id shuffling (Streaming::SubstreamShufflingStep); a stronger Light.
    };

    Kind kind;
    Names keys;

    /// True iff every shuffle key is in `consumer_keys`, so same-consumer-key rows share a stream.
    bool keysCoveredBy(const Names & consumer_keys) const
    {
        return std::all_of(keys.begin(), keys.end(), [&](const auto & k) {
            return std::find(consumer_keys.begin(), consumer_keys.end(), k) != consumer_keys.end();
        });
    }
};

}
