#pragma once

#include <Core/Field.h>
#include <Parsers/ASTSetQuery.h>

namespace DB
{

inline bool isTargetStreamSettingKey(std::string_view name)
{
    return name.find("target_stream") != std::string_view::npos;
}

inline std::vector<String> extractTargetStreamValues(const ASTSetQuery * settings_ast)
{
    std::vector<String> targets;
    if (!settings_ast)
        return targets;

    for (const auto & change : settings_ast->changes)
    {
        if (!isTargetStreamSettingKey(change.name))
            continue;
        chassert(change.value.getType() == Field::Types::String);

        const auto & value = change.value.safeGet<String>();
        if (!value.empty())
            targets.push_back(value);
    }

    std::sort(targets.begin(), targets.end());
    targets.erase(std::unique(targets.begin(), targets.end()), targets.end());
    return targets;
}

}
