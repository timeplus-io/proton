#pragma once

#include <Interpreters/Context_fwd.h>

namespace DB
{

namespace ExternalStream
{

/// Configurations for batching data.
struct BatchConfiguration
{
    /// Threshold for the batch size in terms of bytes.
    uint64_t bytes_threshold;
    /// Threshold for the batch size in terms of record count.
    uint64_t count_threshold;
    /// Timeout (in milliseconds) for batching.
    uint64_t timeout_ms;

public:
    void loadFromContext(const ContextPtr & context);

    BatchConfiguration copyAndUpdateFromContext(const ContextPtr & context) const;
};

}

}
