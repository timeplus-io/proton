#include <Dictionaries/ClickHouseDictionarySource.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/ExternalQueryBuilder.h>
#include <Dictionaries/readInvalidateQuery.h>

#include <ClickHouse/Client.h>
#include <ClickHouse/Source.h>
#include <IO/ConnectionTimeouts.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Session.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Sources/RemoteSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Common/logger_useful.h>

#include <Poco/Logger.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NO_FREE_CONNECTION;
}

static const std::unordered_set<std::string_view> dictionary_allowed_keys
    = {"host",
       "port",
       "user",
       "password",
       "quota_key",
       "db",
       "database",
       "table",
       "update_field",
       "update_lag",
       "invalidate_query",
       "query",
       "where",
       "name",
       "secure",
       "final"};

namespace
{
constexpr size_t MAX_CONNECTIONS = 16;

inline UInt16 getPortFromContext(ContextPtr /*context*/, bool secure)
{
    return secure ? DBMS_DEFAULT_SECURE_PORT : DBMS_DEFAULT_PORT;
}

ClickHouse::ConnectionPoolPtr createPool(const ClickHouseDictionarySource::Configuration & configuration)
{
    return ClickHouse::ConnectionPoolFactory::instance().get(
        /*max_connections=*/MAX_CONNECTIONS,
        configuration.host,
        configuration.port,
        configuration.db,
        configuration.user,
        configuration.password,
        configuration.quota_key,
        /*cluster=*/"",
        /*cluster_secret=*/"",
        "ClickHouseDictionarySource",
        Protocol::Compression::Enable,
        configuration.secure ? Protocol::Secure::Enable : Protocol::Secure::Disable,
        /*priority=*/0);
}

}

ClickHouseDictionarySource::ClickHouseDictionarySource(
    const DictionaryStructure & dict_struct_, const Configuration & configuration_, const Block & sample_block_, ContextMutablePtr context_)
    : update_time{std::chrono::system_clock::from_time_t(0)}
    , dict_struct{dict_struct_}
    , configuration{configuration_}
    , query_builder{dict_struct, configuration.db, "", configuration.table, configuration.query, configuration.where, IdentifierQuotingStyle::Backticks}
    , sample_block{sample_block_}
    , context(context_)
    , pool{createPool(configuration)}
    , load_all_query{query_builder.composeLoadAllQuery()}
    , logger(getLogger("ClickHouseDictionarySource"))
{
}

ClickHouseDictionarySource::ClickHouseDictionarySource(const ClickHouseDictionarySource & other)
    : update_time{other.update_time}
    , dict_struct{other.dict_struct}
    , configuration{other.configuration}
    , invalidate_query_response{other.invalidate_query_response}
    , query_builder{dict_struct, configuration.db, "", configuration.table, configuration.query, configuration.where, IdentifierQuotingStyle::Backticks}
    , sample_block{other.sample_block}
    , context(Context::createCopy(other.context))
    , pool{createPool(configuration)}
    , load_all_query{other.load_all_query}
    , logger(getLogger("ClickHouseDictionarySource"))
{
}

std::string ClickHouseDictionarySource::getUpdateFieldAndDate()
{
    if (update_time != std::chrono::system_clock::from_time_t(0))
    {
        time_t hr_time = std::chrono::system_clock::to_time_t(update_time) - configuration.update_lag;
        std::string str_time = DateLUT::instance().timeToString(hr_time);
        update_time = std::chrono::system_clock::now();
        return query_builder.composeUpdateQuery(configuration.update_field, str_time);
    }
    else
    {
        update_time = std::chrono::system_clock::now();
        return query_builder.composeLoadAllQuery();
    }
}

QueryPipeline ClickHouseDictionarySource::loadAllWithSizeHint(std::atomic<size_t> * result_size_hint)
{
    return createStreamForQuery(load_all_query, result_size_hint);
}

QueryPipeline ClickHouseDictionarySource::loadAll()
{
    return createStreamForQuery(load_all_query);
}

QueryPipeline ClickHouseDictionarySource::loadUpdatedAll()
{
    String load_update_query = getUpdateFieldAndDate();
    return createStreamForQuery(load_update_query);
}

QueryPipeline ClickHouseDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    return createStreamForQuery(query_builder.composeLoadIdsQuery(ids));
}

QueryPipeline ClickHouseDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    String query = query_builder.composeLoadKeysQuery(
        key_columns, requested_rows, ExternalQueryBuilder::IN_WITH_TUPLES, /*partition_key_prefix=*/0, configuration.final);
    return createStreamForQuery(query);
}

bool ClickHouseDictionarySource::isModified() const
{
    if (!configuration.invalidate_query.empty())
    {
        auto response = doInvalidateQuery(configuration.invalidate_query);
        LOG_TRACE(logger, "Invalidate query has returned: {}, previous value: {}", response, invalidate_query_response);
        if (invalidate_query_response == response)
            return false;
        invalidate_query_response = response;
    }
    return true;
}

bool ClickHouseDictionarySource::hasUpdateField() const
{
    return !configuration.update_field.empty();
}

std::string ClickHouseDictionarySource::toString() const
{
    const std::string & where = configuration.where;
    return fmt::format("clickhouse: {}.{}{}", configuration.db, configuration.table, (where.empty() ? "" : ", where: " + where));
}

QueryPipeline ClickHouseDictionarySource::createStreamForQuery(const String & query, std::atomic<size_t> * result_size_hint)
{
    /// LOG_INFO(logger, "Issuing ClickHouse query=`{}`", query);

    const Settings & current_settings = context->getSettingsRef();
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithoutFailover(current_settings);

    auto connection = pool->get(timeouts, &current_settings, true);
    if (!connection)
        throw Exception(ErrorCodes::NO_FREE_CONNECTION, "No free connection to ClickHouse for dictionary query");

    auto client = std::make_unique<DB::ClickHouse::Client>(shared_from_this(), *connection, timeouts, logger);

    auto source = std::make_shared<DB::ClickHouse::Source>(query, sample_block.cloneEmpty(), std::move(client));

    QueryPipeline pipeline{std::move(source)};

    if (result_size_hint)
        pipeline.setProgressCallback([result_size_hint](const Progress & progress) { *result_size_hint += progress.total_rows_to_read; });

    return pipeline;
}

std::string ClickHouseDictionarySource::doInvalidateQuery(const std::string & request) const
{
    LOG_INFO(logger, "Performing invalidate query");

    (void)request;

    /// FIXME
    return "";
}

void registerDictionarySourceClickHouse(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                   const Poco::Util::AbstractConfiguration & config,
                                   const std::string & config_prefix,
                                   Block & sample_block,
                                   ContextPtr global_context,
                                   const std::string & default_database [[maybe_unused]],
                                   bool created_from_ddl) -> DictionarySourcePtr {
        bool secure = config.getBool(config_prefix + ".secure", false);

        UInt16 default_port = getPortFromContext(global_context, secure);

        std::string settings_config_prefix = config_prefix + ".clickhouse";

        std::string host = config.getString(settings_config_prefix + ".host", "localhost");
        std::string user = config.getString(settings_config_prefix + ".user", "default");
        std::string password = config.getString(settings_config_prefix + ".password", "");
        std::string quota_key = config.getString(settings_config_prefix + ".quota_key", "");
        std::string db = config.getString(settings_config_prefix + ".db", default_database);
        std::string table = config.getString(settings_config_prefix + ".table", "");
        UInt16 port = static_cast<UInt16>(config.getUInt(settings_config_prefix + ".port", default_port));
        auto has_config_key = [](const String & key) { return dictionary_allowed_keys.contains(key); };

        auto named_collection = created_from_ddl
            ? getExternalDataSourceConfiguration(config, settings_config_prefix, global_context, has_config_key)
            : std::nullopt;

        if (named_collection)
        {
            const auto & configuration = named_collection->configuration;
            host = configuration.host;
            user = configuration.username;
            password = configuration.password;
            quota_key = configuration.quota_key;
            db = configuration.database;
            table = configuration.table;
            port = configuration.port;
        }

        ClickHouseDictionarySource::Configuration configuration{
            .host = host,
            .user = user,
            .password = password,
            .quota_key = quota_key,
            .db = db,
            .table = table,
            .query = config.getString(settings_config_prefix + ".query", ""),
            .where = config.getString(settings_config_prefix + ".where", ""),
            .invalidate_query = config.getString(settings_config_prefix + ".invalidate_query", ""),
            .update_field = config.getString(settings_config_prefix + ".update_field", ""),
            .update_lag = config.getUInt64(settings_config_prefix + ".update_lag", 1),
            .port = port,
            .is_local = false, /// ClickHouse is always remote
            .secure = config.getBool(settings_config_prefix + ".secure", false),
            .final = config.getBool(settings_config_prefix + ".final", false)};


        ContextMutablePtr context = Context::createCopy(global_context);

        context->applySettingsChanges(readSettingsFromDictionaryConfig(config, config_prefix));

        String dictionary_name = config.getString(".dictionary.name", "");
        String dictionary_database = config.getString(".dictionary.database", "");

        if (dictionary_name == configuration.table && dictionary_database == configuration.db)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "ClickHouseDictionarySource stream cannot be dictionary");

        return std::make_unique<ClickHouseDictionarySource>(dict_struct, configuration, sample_block, context);
    };

    factory.registerSource("clickhouse", create_table_source);
}

}
