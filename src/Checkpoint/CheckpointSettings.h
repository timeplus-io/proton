#pragma once

#include <Checkpoint/CheckpointStorageType.h>
#include <Checkpoint/CheckpointStrategy.h>
#include <Checkpoint/CheckpointType.h>

#include <base/types.h>

#include <memory>

namespace DB
{
class WriteBuffer;
class ReadBuffer;

struct CheckpointSettings;
using CheckpointSettingsPtr = std::shared_ptr<CheckpointSettings>;
using CheckpointSettingsConstPtr = std::shared_ptr<const CheckpointSettings>;
struct CheckpointSettings
{
    std::string raw_settings;

    /// Parsed settings
    CheckpointType type = CheckpointType::Auto;
    CheckpointStorageType storage_type = CheckpointStorageType::LocalFileSystem;
    CheckpointStrategy strategy{};

    UInt64 interval = 0;

    bool isAsync() const { return strategy.async; }
    bool isIncremental() const { return strategy.incremental; }

    void serialize(VersionType version, WriteBuffer & wb) const;
    void deserialize(VersionType version, ReadBuffer & rb);

    static CheckpointType parseCheckpointType(std::string_view ckpt_type_str);
    static CheckpointStorageType parseCheckpointStorageType(std::string_view ckpt_storage_type_str);
    static CheckpointSettingsPtr parse(const std::string & settings_str);

    static constexpr VersionType VERSION = 2;

private:
    void parseImpl();
};
}
