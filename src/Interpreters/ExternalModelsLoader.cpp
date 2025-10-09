#include <Common/logger_useful.h>
#include <Interpreters/ExternalModelsLoader.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}


ExternalModelsLoader::ExternalModelsLoader(ContextPtr context_)
    : ExternalLoader("external model", getLogger("ExternalModelsLoader")), WithContext(context_)
{
    setConfigSettings({"model", "name", {}, {}});
    enablePeriodicUpdates(true);
}

std::shared_ptr<IExternalLoadable> ExternalModelsLoader::createObject(
    const std::string & name, const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix, const std::string & /* repository_name */) const
{
    String type = config.getString(config_prefix + ".type");
    ExternalLoadableLifetime lifetime(config, config_prefix + ".lifetime");

    /// TODO: add models factory.
    if (type == "catboost")
    {
        return std::make_unique<CatBoostModel>(
                name, config.getString(config_prefix + ".path"),
                getContext()->getConfigRef().getString("catboost_dynamic_library_path"),
                lifetime
        );
    }
    else
    {
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Unknown model type: {}", type);
    }
}
}
