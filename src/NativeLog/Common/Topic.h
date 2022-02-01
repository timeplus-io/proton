#pragma once

#include <string>
#include <functional>

#include <Poco/UUID.h>

namespace nlog
{
using TopicID = Poco::UUID;

struct Topic
{
    Topic(const std::string & name_, const TopicID & id_) : name(name_), id(id_) { }

    bool operator==(const Topic & rhs) const
    {
        return name == rhs.name;
    }

    std::string name;
    TopicID id;
};
}

template<>
struct std::hash<nlog::Topic>
{
    std::size_t operator()(const nlog::Topic & topic) const noexcept
    {
        return std::hash<std::string>{}(topic.name);
    }
};
