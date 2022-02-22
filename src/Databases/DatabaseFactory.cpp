#include <Databases/DatabaseFactory.h>

#include <filesystem>
#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseDictionary.h>
#include <Databases/DatabaseLazy.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabaseOrdinary.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/queryToString.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Common/Macros.h>

#include "config_core.h"

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_AST;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_DATABASE_ENGINE;
    extern const int CANNOT_CREATE_DATABASE;
    extern const int NOT_IMPLEMENTED;
}

DatabasePtr DatabaseFactory::get(const ASTCreateQuery & create, const String & metadata_path, ContextPtr context)
{
    bool created = false;

    try
    {
        /// Creates store/xxx/ for Atomic
        fs::create_directories(fs::path(metadata_path).parent_path());

        /// Before 20.7 it's possible that .sql metadata file does not exist for some old database.
        /// In this case Ordinary database is created on server startup if the corresponding metadata directory exists.
        /// So we should remove metadata directory if database creation failed.
        /// TODO remove this code
        created = fs::create_directory(metadata_path);

        DatabasePtr impl = getImpl(create, metadata_path, context);

        if (impl && context->hasQueryContext() && context->getSettingsRef().log_queries)
            context->getQueryContext()->addQueryFactoriesInfo(Context::QueryLogFactories::Database, impl->getEngineName());

        // Attach database metadata
        if (impl && create.comment)
            impl->setDatabaseComment(create.comment->as<ASTLiteral>()->value.safeGet<String>());

        return impl;
    }
    catch (...)
    {
        if (created && fs::exists(metadata_path))
            fs::remove_all(metadata_path);
        throw;
    }
}

template <typename ValueType>
static inline ValueType safeGetLiteralValue(const ASTPtr &ast, const String &engine_name)
{
    if (!ast || !ast->as<ASTLiteral>())
        throw Exception("Database engine " + engine_name + " requested literal argument.", ErrorCodes::BAD_ARGUMENTS);

    return ast->as<ASTLiteral>()->value.safeGet<ValueType>();
}

DatabasePtr DatabaseFactory::getImpl(const ASTCreateQuery & create, const String & metadata_path, ContextPtr context)
{
    auto * engine_define = create.storage;
    const String & database_name = create.getDatabase();
    const String & engine_name = engine_define->engine->name;
    const UUID & uuid = create.uuid;

    static const std::unordered_set<std::string_view> database_engines{"Ordinary", "Atomic", "Memory",
        "Dictionary", "Lazy", "Replicated", "MySQL", "MaterializeMySQL", "MaterializedMySQL",
        "PostgreSQL", "MaterializedPostgreSQL", "SQLite"};

    if (!database_engines.contains(engine_name))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database engine name `{}` does not exist", engine_name);

    static const std::unordered_set<std::string_view> engines_with_arguments{"MySQL", "MaterializeMySQL", "MaterializedMySQL",
        "Lazy", "Replicated", "PostgreSQL", "MaterializedPostgreSQL", "SQLite"};

    static const std::unordered_set<std::string_view> engines_with_table_overrides{"MaterializeMySQL", "MaterializedMySQL", "MaterializedPostgreSQL"};
    bool engine_may_have_arguments = engines_with_arguments.contains(engine_name);

    if (engine_define->engine->arguments && !engine_may_have_arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database engine `{}` cannot have arguments", engine_name);

    bool has_unexpected_element = engine_define->engine->parameters || engine_define->partition_by ||
                                  engine_define->primary_key || engine_define->order_by ||
                                  engine_define->sample_by;
    bool may_have_settings = endsWith(engine_name, "MySQL") || engine_name == "Replicated" || engine_name == "MaterializedPostgreSQL";

    if (has_unexpected_element || (!may_have_settings && engine_define->settings))
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_AST,
                        "Database engine `{}` cannot have parameters, primary_key, order_by, sample_by, settings", engine_name);

    if (create.table_overrides && !engines_with_table_overrides.contains(engine_name))
        /// proton: starts
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database engine `{}` cannot have stream overrides", engine_name);
        /// proton: ends

    if (engine_name == "Ordinary")
        return std::make_shared<DatabaseOrdinary>(database_name, metadata_path, context);
    else if (engine_name == "Atomic")
        return std::make_shared<DatabaseAtomic>(database_name, metadata_path, uuid, context);
    else if (engine_name == "Memory")
        return std::make_shared<DatabaseMemory>(database_name, context);
    else if (engine_name == "Dictionary")
        return std::make_shared<DatabaseDictionary>(database_name, context);
    else if (engine_name == "Lazy")
    {
        const ASTFunction * engine = engine_define->engine;

        if (!engine->arguments || engine->arguments->children.size() != 1)
            throw Exception("Lazy database require cache_expiration_time_seconds argument", ErrorCodes::BAD_ARGUMENTS);

        const auto & arguments = engine->arguments->children;

        const auto cache_expiration_time_seconds = safeGetLiteralValue<UInt64>(arguments[0], "Lazy");
        return std::make_shared<DatabaseLazy>(database_name, metadata_path, cache_expiration_time_seconds, context);
    }

    throw Exception("Unknown database engine: " + engine_name, ErrorCodes::UNKNOWN_DATABASE_ENGINE);
}

}
