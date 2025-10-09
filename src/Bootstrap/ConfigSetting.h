#pragma once

#include <cassert>
#include <limits>
#include <string>
#include <vector>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{
struct ConfigSetting
{
    enum class DataType : std::uint8_t
    {
        Bool,
        Int,
        UInt,
        Int64,
        UInt64,
        String,
    };

    template <typename T>
    ConfigSetting(std::string_view postfix_, T & setting_) : postfix(postfix_)
    {
        static_assert(
            std::is_same_v<T, bool> || std::is_same_v<T, int> || std::is_same_v<T, int32_t> || std::is_same_v<T, unsigned int>
                || std::is_same_v<T, uint32_t> || std::is_same_v<T, int64_t> || std::is_same_v<T, uint64_t>
                || std::is_same_v<T, std::string>,
            "Unsupported data type");

        if constexpr (std::is_same_v<T, bool>)
        {
            type = DataType::Bool;
        }
        else if constexpr (std::is_same_v<T, int> || std::is_same_v<T, int32_t>)
        {
            type = DataType::Int;
        }
        else if constexpr (std::is_same_v<T, unsigned int> || std::is_same_v<T, uint32_t>)
        {
            type = DataType::UInt;
        }
        else if constexpr (std::is_same_v<T, int64_t>)
        {
            type = DataType::Int64;
        }
        else if constexpr (std::is_same_v<T, uint64_t>)
        {
            type = DataType::UInt64;
        }
        else if constexpr (std::is_same_v<T, std::string>)
        {
            type = DataType::String;
        }

        setting = &setting_;
    }

    std::string postfix;
    void * setting;

    DataType type;
};

using ConfigSettings = std::vector<ConfigSetting>;

void parseConfigSettings(
    ConfigSettings & config_settings, const Poco::Util::AbstractConfiguration & config, const std::string & setting_prefix);
}
