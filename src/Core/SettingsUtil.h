#pragma once
#include <Core/BaseSettings.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
/// Get the corresponding settings of `ConfigurableTraits_` from the configuration
template <class ConfigurableTraits_>
SettingsChanges loadSettingChangesFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(config_elem))
        return {};

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    const BaseSettings<ConfigurableTraits_> config_settings;
    SettingsChanges changes;
    changes.reserve(config_keys.size());
    for (const String & key : config_keys)
        if (config_settings.has(key))
            changes.emplace_back(key, config.getString(config_elem + "." + key));
        else
            BaseSettingsHelpers::warningSettingNotFound(config_elem + "." + key);

    return changes;
}
}
