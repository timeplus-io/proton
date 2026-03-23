#pragma once

#include <base/types.h>

namespace DB
{
namespace StreamTypes
{
    inline constexpr String KAFKA = "kafka";
    inline constexpr String REDPANDA = "redpanda";
    inline constexpr String PULSAR = "pulsar";
    inline constexpr String TIMEPLUS = "timeplus";
    inline constexpr String LOG = "log";
    inline constexpr String ICEBERG = "iceberg";
    inline constexpr String HTTP = "http";
    inline constexpr String NATS_JETSTREAM = "nats_jetstream";
}

namespace ExternalStreamTypes
{
    inline constexpr String PYTHON = "python";
}
}
