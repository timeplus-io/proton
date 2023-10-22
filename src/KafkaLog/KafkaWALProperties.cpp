#include <KafkaLog/KafkaWALProperties.h>

#include <base/defines.h>
#include <Common/Exception.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

namespace DB::ErrorCodes
{
extern const int INVALID_SETTING_VALUE;
}

namespace klog
{
KConfParams parseProperties(const String & properties)
{
    KConfParams result;

    if (properties.empty())
        return result;

    std::vector<String> parts;
    boost::split(parts, properties, boost::is_any_of(";"));
    result.reserve(parts.size());

    for (const auto & part : parts)
    {
        /// skip empty part, this happens when there are redundant / trailing ';'
        if (unlikely(std::all_of(part.begin(), part.end(), [](char ch) { return isspace(static_cast<unsigned char>(ch)); })))
            continue;

        auto equal_pos = part.find('=');
        if (unlikely(equal_pos == std::string::npos || equal_pos == 0 || equal_pos == part.size() - 1))
            throw DB::Exception(DB::ErrorCodes::INVALID_SETTING_VALUE, "Invalid property `{}`, expected format: <key>=<value>.", part);

        auto key = part.substr(0, equal_pos);
        auto value = part.substr(equal_pos + 1);

        /// no spaces are supposed be around `=`, thus only need to
        /// remove the leading spaces of keys and trailing spaces of values
        boost::trim_left(key);
        boost::trim_right(value);
        result.emplace_back(std::move(key), std::move(value));
    }

    return result;
}
}
