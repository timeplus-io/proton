#include "KafkaWALProperties.h"

#include <base/defines.h>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

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
            throw std::invalid_argument("Invalid property `" + part + "`, expected format: <key>=<value>.");

        auto key = part.substr(0, equal_pos);
        auto value = part.substr(equal_pos + 1);

        /// no spaces are supposed be around `=`, thus only need to
        /// remove the leading spaces of keys and trailing spaces of values
        boost::trim_left(key);
        boost::trim_right(value);
        result.push_back(std::make_pair(key, value));
    }

    return result;
}
}
