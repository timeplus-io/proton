#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/ExternalTable/ExternalTableSettings.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

IMPLEMENT_SETTINGS_TRAITS(ExternalTableSettingsTraits, LIST_OF_EXTERNAL_TABLE_SETTINGS)

void ExternalTableSettings::loadFromQuery(ASTStorage & storage)
{
    if (storage.settings)
    {
        try
        {
            applyChanges(storage.settings->changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for storage " + storage.engine->name);
            throw;
        }
    }
    else
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage.set(storage.settings, settings_ast);
    }
}

}
