#include <Storages/NamedCollectionsHelpers.h>
#include <Access/ContextAccess.h>
#include <Core/Settings.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>

/// proton: starts
#include <Storages/ExternalStream/ExternalStreamSettings.h>
#include <Storages/ExternalTable/ExternalTableSettings.h>
/// proton: ends

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace
{
    std::optional<std::string> getCollectionName(ASTs asts)
    {
        if (asts.empty())
            return std::nullopt;

        const auto * identifier = asts[0]->as<ASTIdentifier>();
        if (!identifier)
            return std::nullopt;

        return identifier->name();
    }

    std::optional<std::pair<std::string, std::variant<Field, ASTPtr>>>
    getKeyValueFromASTImpl(ASTPtr ast, [[maybe_unused]] bool fallback_to_ast_value, ContextPtr context)
    {
        const auto * function = ast->as<ASTFunction>();
        if (!function || function->name != "equals")
            return std::nullopt;

        const auto * function_args_expr = assert_cast<const ASTExpressionList *>(function->arguments.get());
        const auto & function_args = function_args_expr->children;

        if (function_args.size() != 2)
            return std::nullopt;

        auto literal_key = evaluateConstantExpressionOrIdentifierAsLiteral(function_args[0], context);
        auto key = checkAndGetLiteralArgument<String>(literal_key, "key");

        auto literal_value = evaluateConstantExpressionOrIdentifierAsLiteral(function_args[1], context);
        auto value = literal_value->as<ASTLiteral>()->value;

        return std::pair{key, value};
    }
}

MutableNamedCollectionPtr tryGetNamedCollectionWithOverrides(
    ASTs asts, ContextPtr context, bool throw_unknown_collection, std::vector<std::pair<std::string, ASTPtr>> * complex_args)
{
    if (asts.empty())
        return nullptr;

    NamedCollectionFactory::instance().loadIfNot();

    auto collection_name = getCollectionName(asts);
    if (!collection_name.has_value())
        return nullptr;

    context->checkAccess(AccessType::NAMED_COLLECTION, *collection_name);

    NamedCollectionPtr collection;
    if (throw_unknown_collection)
        collection = NamedCollectionFactory::instance().get(*collection_name);
    else
        collection = NamedCollectionFactory::instance().tryGet(*collection_name);

    if (!collection)
        return nullptr;

    auto collection_copy = collection->duplicate();

    if (asts.size() == 1)
        return collection_copy;

    const auto allow_override_by_default = context->getSettingsRef().allow_named_collection_override_by_default;

    for (auto it = std::next(asts.begin()); it != asts.end(); ++it)
    {
        auto value_override = getKeyValueFromASTImpl(*it, /* fallback_to_ast_value */ complex_args != nullptr, context);

        if (!value_override)
    {
            if (!(*it)->as<ASTFunction>())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected key-value argument or function");
            if (allow_override_by_default)
                continue;
            // if allow_override_by_default is false we don't allow extra arguments
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Override not allowed because setting allow_override_by_default is disabled");
        }
        if (!collection_copy->isOverridable(value_override->first, allow_override_by_default))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Override not allowed for '{}'", value_override->first);

        if (const ASTPtr * value = std::get_if<ASTPtr>(&value_override->second))
        {
            complex_args->emplace_back(value_override->first, *value);
            continue;
        }

        const auto & [key, value] = *value_override;
        collection_copy->setOrUpdate<String>(key, toString(std::get<Field>(value)), {});
    }

    return collection_copy;
}

MutableNamedCollectionPtr tryGetNamedCollectionWithOverrides(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, ContextPtr context)
{
    auto collection_name = config.getString(config_prefix + ".name", "");
    if (collection_name.empty())
        return nullptr;

    context->checkAccess(AccessType::NAMED_COLLECTION, collection_name);

    const auto & collection = NamedCollectionFactory::instance().get(collection_name);
    auto collection_copy = collection->duplicate();

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);
    const auto allow_override_by_default = context->getSettingsRef().allow_named_collection_override_by_default;
    for (const auto & key : keys)
    {
        if (collection_copy->isOverridable(key, allow_override_by_default))
            collection_copy->setOrUpdate<String>(key, config.getString(config_prefix + '.' + key), {});
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Override not allowed for '{}'", key);
    }

    return collection_copy;
}

/// proton: starts
template <typename Settings>
void updateSettingsByNamedCollection(Settings & settings, ContextPtr context)
{
    auto collection_name = settings.named_collection.value;
    if (collection_name.empty())
        return;

    const auto collection = NamedCollectionFactory::instance().get(collection_name);
    chassert(collection);

    const auto allow_override_by_default = context->getSettingsRef().allow_named_collection_override_by_default;
    for (const auto & key : *collection)
    {
        if (settings.isChanged(key))
        {
            if (!collection->isOverridable(key, allow_override_by_default))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Override not allowed for '{}'", key);
        }
        else
        {
            settings.setString(key, collection->template get<String>(key));
        }
    }
}

template void updateSettingsByNamedCollection<ExternalStreamSettings>(ExternalStreamSettings & settings, ContextPtr context);
template void updateSettingsByNamedCollection<ExternalTableSettings>(ExternalTableSettings & settings, ContextPtr context);


void updateConfigurationByNamedCollection(Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    /// Read named_collection value from config
    auto collection_name = config.getString(config_prefix + ".named_collection", "");
    if (collection_name.empty())
        return;

    /// Load named collection
    const auto collection = NamedCollectionFactory::instance().get(collection_name);
    chassert(collection);

    /// Read named collection values and update config with its key values
    for (const auto & key : *collection)
    {
        /// Check if config already has the key defined in named collection
        auto config_key = config_prefix + "." + key;
        if (config.has(config_key))
        {
            /// Key already exists in config, check if it's overridable
            if (!collection->isOverridable(key, true))
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Override not allowed for key '{}' from named collection '{}'", key, collection_name);
            }
            /// Overridable and config already has the key, do nothing
        }
        else
        {
            /// Key doesn't exist in config, add it from named collection
            config.setString(config_key, collection->get<String>(key));
        }
    }
}
/// proton: ends

HTTPHeaderEntries getHeadersFromNamedCollection(const NamedCollection & collection)
{
    HTTPHeaderEntries headers;
    auto keys = collection.getKeys(0, "headers");
    for (const auto & key : keys)
        headers.emplace_back(collection.get<String>(key + ".name"), collection.get<String>(key + ".value"));
    return headers;
}

}
