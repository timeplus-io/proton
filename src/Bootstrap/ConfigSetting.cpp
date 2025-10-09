#include <Bootstrap/ConfigSetting.h>

#include <Common/Exception.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <fmt/format.h>
#include <magic_enum.hpp>

namespace DB
{
namespace ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
}

void parseConfigSettings(
    ConfigSettings & config_settings, const Poco::Util::AbstractConfiguration & config, const std::string & setting_prefix)
{
    for (const auto & config_setting : config_settings)
    {
        auto key = fmt::format("{}{}", setting_prefix, config_setting.postfix);
        if (!config.has(key))
            continue;

        try
        {
            switch (config_setting.type)
            {
                case ConfigSetting::DataType::Bool:
                    *static_cast<bool *>(config_setting.setting) = config.getBool(key);
                    break;
                case ConfigSetting::DataType::Int:
                    *static_cast<int32_t *>(config_setting.setting) = config.getInt(key);
                    break;
                case ConfigSetting::DataType::UInt:
                    *static_cast<uint32_t *>(config_setting.setting) = config.getUInt(key);
                    break;
                case ConfigSetting::DataType::Int64:
                    *static_cast<int64_t *>(config_setting.setting) = config.getInt64(key);
                    break;
                case ConfigSetting::DataType::UInt64:
                    *static_cast<uint64_t *>(config_setting.setting) = config.getUInt64(key);
                    break;
                case ConfigSetting::DataType::String:
                    *static_cast<std::string *>(config_setting.setting) = config.getString(key);
                    break;
            }
        }
        catch (const std::exception & e)
        {
            throw DB::Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "Failed to parse '{}' setting, raw_value={} expected_type={} error={}",
                key,
                config.getString(key, ""),
                magic_enum::enum_name(config_setting.type),
                e.what());
        }
        catch (...)
        {
            throw DB::Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "Failed to parse '{}' setting, raw_value={} expected_type={}",
                key,
                config.getString(key, ""),
                magic_enum::enum_name(config_setting.type));
        }
    }
}
}
