#include <Storages/ExternalStream/ExternalStreamSettings.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Common/Exception.h>

#include <Poco/FileStream.h>
#include <Poco/Util/PropertyFileConfiguration.h>

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_OPEN_FILE;
extern const int UNKNOWN_SETTING;
}

IMPLEMENT_SETTINGS_TRAITS(ExternalStreamSettingsTraits, LIST_OF_EXTERNAL_STREAM_SETTINGS)

void ExternalStreamSettings::loadFromQuery(ASTStorage & storage_def, bool throw_on_unknown)
{
    if (storage_def.settings != nullptr)
    {
        for (const auto & change : storage_def.settings->changes)
        {
            if (has(change.name))
            {
                set(change.name, change.value);
            }
            else
            {
                if (throw_on_unknown)
                    throw Exception(ErrorCodes::UNKNOWN_SETTING, "Unknown setting {}: for storage ExternalStream", change.name);
            }
        }
    }
    else
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }
}

/// Load settings from a Java-style properties file.
/// Each line denotes a setting assignment in the form <key> = <valuie>.
/// For example,
///
/// brokers = kafka-1:12345
/// username = user1
/// password = password1234
/// poll_waittime_ms = 1000
///
/// It only updates the unchanged settings so will not overwrite settings from DDL.
void ExternalStreamSettings::loadFromConfigFile()
{
    String file_path;
    if (tryGetString("config_file", file_path) && !file_path.empty())
    {
        Poco::FileInputStream istr(file_path);
        if (!istr.good())
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Cannot open the configure file: {}", file_path);

        Poco::AutoPtr<Poco::Util::PropertyFileConfiguration> config(new Poco::Util::PropertyFileConfiguration(istr));

        auto iter = allUnchanged().begin();
        auto end = allUnchanged().end();
        for (; iter != end; ++iter)
        {
            const auto & field_name = (*iter).getName();
            if (config->has(field_name))
                setString(field_name, config->getString(field_name));
        }
    }
}
}
