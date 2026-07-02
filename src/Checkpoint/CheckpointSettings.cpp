#include <Checkpoint/CheckpointSettings.h>

#include <Cluster/Common/serde.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <ranges>
#include <fmt/ranges.h>


namespace DB
{
namespace ErrorCodes
{
extern const int INVALID_SETTING_VALUE;
}

namespace
{
bool parseBoolTextWord(const std::string & str, std::string_view setting_name)
{
    if (str.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Invalid checkpoint setting '{}': empty value", setting_name);

    try
    {
        if (str[0] == '0')
            return false;
        else if (str[0] == '1')
            return true;

        bool x;
        ReadBufferFromString rb(str);
        readBoolTextWord(x, rb, /*support_upper_case=*/true);
        return x;
    }
    catch (const Exception & e)
    {
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Invalid setting '{}': {}", setting_name, e.what());
    }
}
}

void CheckpointSettings::serialize(VersionType, WriteBuffer & wb) const
{
    writeStringBinary(raw_settings, wb);
}

void CheckpointSettings::deserialize(VersionType version, ReadBuffer & rb)
{
    if (version >= 2)
    {
        readStringBinary(raw_settings, rb);
        parseImpl();
    }
    else if (version == 1)
    {
        [[maybe_unused]] auto type_value = cluster::deserializeEnum<CheckpointType>(rb);
        // Convert any old type values to File
        type = CheckpointType::File;

        replication_type = cluster::deserializeEnum<CheckpointReplicationType>(rb);
        readVarUInt(interval, rb);

        // Always use LocalFileSystem
        replication_type = CheckpointReplicationType::LocalFileSystem;

        /// Repair raw settings
        std::string replication_type_str = "local_file_system"; // Always local
        raw_settings = fmt::format(
            "type={};replication_type={};interval={};async={};incremental={}",
            "file",
            replication_type_str,
            interval,
            isAsync(),
            isIncremental());
    }
}

CheckpointType CheckpointSettings::parseCheckpointType(std::string_view ckpt_type_str)
{
    static const std::unordered_map<std::string_view, CheckpointType> map{
        {"auto", CheckpointType::Auto},
        {"file", CheckpointType::File},
        {"rocks", CheckpointType::Rocks},
    };

    auto iter = map.find(ckpt_type_str);
    if (iter == map.end())
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE, "Unknown checkpoint type '{}', expected one of 'auto', 'file', 'rocks'", ckpt_type_str);

    return iter->second;
}

CheckpointReplicationType CheckpointSettings::parseCheckpointReplicationType(std::string_view ckpt_replication_type_str)
{
    // Accept any valid input but always use LocalFileSystem
    static const std::unordered_set<std::string_view> valid_types{"auto", "local_file_system", "nativelog", "shared"};

    if (!valid_types.contains(ckpt_replication_type_str))
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "Unknown checkpoint replication type '{}', expected one of 'auto', 'local_file_system', 'nativelog', 'shared'",
            ckpt_replication_type_str);

    // Always return LocalFileSystem
    return CheckpointReplicationType::LocalFileSystem;
}

void CheckpointSettings::parseImpl()
{
    std::unordered_map<std::string, std::string> settings_map;
    
    /// Parse simple key=value;key=value format
    if (!raw_settings.empty())
    {
        size_t pos = 0;
        size_t end = 0;
        while (pos < raw_settings.length())
        {
            /// Find next semicolon or end of string
            end = raw_settings.find(';', pos);
            if (end == std::string::npos)
                end = raw_settings.length();
            
            /// Extract key=value pair
            std::string pair = raw_settings.substr(pos, end - pos);
            size_t eq_pos = pair.find('=');
            if (eq_pos != std::string::npos)
            {
                std::string key = pair.substr(0, eq_pos);
                std::string value = pair.substr(eq_pos + 1);
                
                /// Trim whitespace
                key.erase(0, key.find_first_not_of(" \t"));
                key.erase(key.find_last_not_of(" \t") + 1);
                value.erase(0, value.find_first_not_of(" \t"));
                value.erase(value.find_last_not_of(" \t") + 1);
                
                if (!key.empty())
                    settings_map[key] = value;
            }
            
            pos = end + 1;
        }
    }

    if (auto iter = settings_map.find("type"); iter != settings_map.end())
        type = parseCheckpointType(iter->second);

    /// storage_type is deprecated, use replication_type instead
    if (auto iter = settings_map.find("replication_type"); iter != settings_map.end())
        replication_type = parseCheckpointReplicationType(iter->second);
    else if (iter = settings_map.find("storage_type"); iter != settings_map.end())
        replication_type = parseCheckpointReplicationType(iter->second);

    if (auto iter = settings_map.find("interval"); iter != settings_map.end())
        interval = std::stoul(iter->second);

    if (auto iter = settings_map.find("async"); iter != settings_map.end())
        strategy.async = parseBoolTextWord(iter->second, "async");

    if (auto iter = settings_map.find("incremental"); iter != settings_map.end())
        strategy.incremental = parseBoolTextWord(iter->second, "incremental");

    if (auto iter = settings_map.find("offsets_only"); iter != settings_map.end())
        strategy.offsets_only = parseBoolTextWord(iter->second, "offsets_only");


    /// Other unknown settings are ignored
}

CheckpointSettingsPtr CheckpointSettings::parse(const std::string & settings_str)
{
    auto settings = std::make_shared<CheckpointSettings>();
    settings->raw_settings = settings_str;
    settings->parseImpl();
    return settings;
}
}
