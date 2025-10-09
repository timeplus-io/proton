#pragma once

#include <Core/UUID.h>

#include <filesystem>

namespace fs = std::filesystem;
namespace Poco
{
    class Logger;
}

namespace DB
{

class ServerMeta
{
    inline static uint64_t version = 1;
    inline static UUID server_uuid = UUIDHelpers::Nil;
    inline static uint64_t epoch = 0;
    inline static int64_t boot_timestamp = 0;

public:
    /// Returns persistent UUID of current clickhouse-server or clickhouse-keeper instance.
    static const UUID & getIdentity() noexcept { return server_uuid; }

    static uint64_t getEpoch() noexcept { return epoch; }

    /// Milliseconds since UTC
    static int64_t getBootTimestamp() noexcept { return boot_timestamp; }

    /// Loads server meta from file or creates new one. Should be called on daemon startup.
    static void load(const fs::path & server_meta_file, Poco::Logger * logger);
};

}
