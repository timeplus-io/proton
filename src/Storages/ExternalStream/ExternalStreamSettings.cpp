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
            else if (change.name.starts_with("http_header_")) /// dynamic HTTP headers
            {
                continue;
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
}
