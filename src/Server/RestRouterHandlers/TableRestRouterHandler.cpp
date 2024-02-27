#include "TableRestRouterHandler.h"
#include "SchemaValidator.h"

#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Streaming/ASTToJSONUtils.h>
#include <Interpreters/Streaming/DDLHelper.h>
#include <Interpreters/executeQuery.h>
#include <KafkaLog/KafkaWALPool.h>
#include <NativeLog/Server/NativeLog.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/Streaming/StorageMaterializedView.h>
#include <Storages/Streaming/StorageStream.h>
#include <Storages/Streaming/parseHostShards.h>
#include <Common/ProtonCommon.h>
#include <Common/queryStreams.h>

#include <boost/algorithm/string/join.hpp>

#include <regex>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
extern const int STREAM_ALREADY_EXISTS;
extern const int UNKNOWN_DATABASE;
extern const int UNKNOWN_STREAM;
}

namespace
{
enum class DeleteMode
{
    TRUNCATE,
    DROP,
    INVALID,
};

inline DeleteMode toDeleteMode(const std::string & mode)
{
    if (mode == "truncate")
        return DeleteMode::TRUNCATE;
    else if (mode == "drop")
        return DeleteMode::DROP;
    else
        return DeleteMode::INVALID;
}

std::regex PARSE_HOST_SHARDS_REGEX{R"(host_shards\s*=\s*'([,|\s|\d]*)')"};
std::regex PARSE_SHARDS_REGEX{R"(Stream\(\s*(\d+),\s*\d+\s*,)"};
std::regex PARSE_REPLICATION_REGEX{R"(Stream\(\s*\d+,\s*(\d+)\s*,)"};

Int32 searchIntValueByRegex(const std::regex & pattern, const String & str)
{
    std::smatch pattern_match;

    auto m = std::regex_search(str, pattern_match, pattern);
    /// assert(m);
    /// (void)m;
    /// FIXME, without ddl/placement service (nativelog case), we will not assign shard number
    if (!m)
        return 0;

    return std::stoi(pattern_match.str(1));
}
}

TableRestRouterHandler::Table::Table(const String & node_identity_, const String & host_, const Block & block, size_t row)
    : node_identity(node_identity_), host(host_)
{
    std::unordered_map<String, void *> kvp = {
        {"database", &database},
        {"name", &name},
        {"engine", &engine},
        {"mode", &mode},
        {"uuid", &uuid},
        {"dependencies_table", &dependencies_table},
        {"create_table_query", &create_table_query},
        {"engine_full", &engine_full},
        {"partition_key", &partition_key},
        {"sorting_key", &sorting_key},
        {"primary_key", &primary_key},
        {"sampling_key", &sampling_key},
        {"storage_policy", &storage_policy},
    };

    for (const auto & col : block)
    {
        auto it = kvp.find(col.name);
        if (it != kvp.end())
        {
            if (col.name == "dependencies_table")
            {
                /// String array
                WriteBufferFromOwnString buffer;
                col.type->getDefaultSerialization()->serializeText(*col.column, row, buffer, FormatSettings{});
                *static_cast<String *>(it->second) = buffer.str();
            }
            else if (col.name == "uuid")
            {
                auto type_id = col.type->getTypeId();
                if (type_id == TypeIndex::UUID)
                    *static_cast<UUID *>(it->second) = static_cast<const ColumnUUID *>(col.column.get())->getElement(row);
                else if (type_id == TypeIndex::UInt128)
                {
                    static_assert(sizeof(UInt128) == sizeof(UUID));
                    *static_cast<UUID *>(it->second) = static_cast<const ColumnUInt128 *>(col.column.get())->getElement(row);
                }
                else
                    assert(false && "Invalid type of uuid column.");
            }
            else
            {
                /// String
                *static_cast<String *>(it->second) = col.column->getDataAt(row).toString();
            }
        }
    }

    if (engine == "Stream")
    {
        auto shards = searchIntValueByRegex(PARSE_SHARDS_REGEX, engine_full);

        std::smatch pattern_match;
        auto m = std::regex_search(engine_full, pattern_match, PARSE_HOST_SHARDS_REGEX);
        if (!m)
        {
            for (Int32 i = 0; i < shards; ++i)
                host_shards.push_back(i);
        }
        else
        {
            host_shards = parseHostShards(pattern_match.str(1), shards);
        }
    }
}

std::map<String, std::map<String, String>> TableRestRouterHandler::update_schema = {
    {"required", {}}, {"optional", {{"ttl_expression", "string"}, {"logstore_retention_bytes", "int"}, {"logstore_retention_ms", "int"}}}};

std::map<String, String> TableRestRouterHandler::granularity_func_mapping
    = {{"M", "to_YYYYMM(`" + ProtonConsts::RESERVED_EVENT_TIME + "`)"},
       {"D", "to_YYYYMMDD(`" + ProtonConsts::RESERVED_EVENT_TIME + "`)"},
       {"H", "to_start_of_hour(`" + ProtonConsts::RESERVED_EVENT_TIME + "`)"},
       {"m", "to_start_of_minute(`" + ProtonConsts::RESERVED_EVENT_TIME + "`)"}};

bool TableRestRouterHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    if (payload->has("partition_by_granularity"))
    {
        if (!granularity_func_mapping.contains(payload->get("partition_by_granularity").toString()))
        {
            error_msg = "Invalid partition_by_granularity, only `m, H, D, M` are supported";
            return false;
        }
    }

    if (payload->has("order_by_granularity"))
    {
        if (!granularity_func_mapping.contains(payload->get("order_by_granularity").toString()))
        {
            error_msg = "Invalid order_by_granularity, only `m, H, D, M` are supported";
            return false;
        }
    }

    /// For non-distributed env or user force to create a `local` MergeTree table
    if (!query_context->isDistributedEnv() || getQueryParameterBool("distributed", false))
    {
        int shards = payload->has("shards") ? payload->get("shards").convert<Int32>() : 1;
        int replication_factor = payload->has("replication_factor") ? payload->get("replication_factor").convert<Int32>() : 1;

        if (shards != 1 || replication_factor != 1)
        {
            error_msg = "Invalid shards / replication factor, local stream shall have only 1 shard and 1 replica";
            return false;
        }
    }

    return true;
}

bool TableRestRouterHandler::validatePatch(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    return validateSchema(update_schema, payload, error_msg);
}

std::pair<String, Int32> TableRestRouterHandler::executeGet(const Poco::JSON::Object::Ptr & /* payload */) const
{
    String requested_database, requested_name;
    requested_database = getPathParameter("database");
    requested_name = getPathParameter("stream");

    /// For the case GET '/proton/v1/ddl/streams'
    if (requested_database.empty())
        requested_database = database;

    if (!DatabaseCatalog::instance().tryGetDatabase(requested_database))
        return {
            jsonErrorResponse(fmt::format("Databases {} does not exist.", requested_database), ErrorCodes::UNKNOWN_DATABASE),
            HTTPResponse::HTTP_NOT_FOUND};

    TablePtrs streams;
    auto node_identity{query_context->getNodeIdentity()};
    auto this_host{query_context->getHostFQDN()};

    if (requested_name.empty())
    {
        queryStreamsByDatabasse(query_context, requested_database, [&](Block && block) {
            streams.reserve(block.rows());
            for (size_t row = 0; row < block.rows(); ++row)
                streams.push_back(std::make_shared<Table>(node_identity, this_host, block, row));
        });
    }
    else
    {
        queryOneStream(query_context, requested_database, requested_name, [&](Block && block) {
            streams.reserve(block.rows());
            for (size_t row = 0; row < block.rows(); ++row)
                streams.push_back(std::make_shared<Table>(node_identity, this_host, block, row));
        });
        if (streams.empty())
            return {
                jsonErrorResponse(fmt::format("No stream named '{}' in database '{}'", requested_name, requested_database), ErrorCodes::UNKNOWN_STREAM),
                HTTPResponse::HTTP_NOT_FOUND};
    }

    Poco::JSON::Object resp;
    buildTablesJSON(resp, streams);

    resp.set("request_id", query_context->getCurrentQueryId());
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);
    String resp_str = resp_str_stream.str();

    return {resp_str, HTTPResponse::HTTP_OK};
}

std::pair<String, Int32> TableRestRouterHandler::executePost(const Poco::JSON::Object::Ptr & payload) const
{
    const auto & table = payload->get("name").toString();
    if (DatabaseCatalog::instance().tryGetTable({database, table}, query_context))
        return {
            jsonErrorResponse(fmt::format("Stream '{}.{}' already exists.", database, table), ErrorCodes::STREAM_ALREADY_EXISTS),
            HTTPResponse::HTTP_BAD_REQUEST};

    const auto & host_shards = getQueryParameter("host_shards");
    const auto & uuid = getQueryParameter("uuid");
    const auto & synchronous_ddl = getQueryParameter("synchronous_ddl", "1");
    const auto & suspend = getQueryParameter("_suspend", "false");
    const auto & query = getCreationSQL(payload, host_shards, uuid);

    if (synchronous_ddl == "1")
        query_context->setSetting("synchronous_ddl", true);
    else
        query_context->setSetting("synchronous_ddl", false);

    if (query.empty())
        return {"", HTTPResponse::HTTP_BAD_REQUEST};

    if (suspend == "true")
        /// suspend the stream after creation
        setupDistributedQueryParameters({{"_suspend", suspend}}, payload);
    else
        setupDistributedQueryParameters({}, payload);

    return {processQuery(query), HTTPResponse::HTTP_OK};
}

std::pair<String, Int32> TableRestRouterHandler::executePatch(const Poco::JSON::Object::Ptr & payload) const
{
    const String & table = getPathParameter("stream");

    if (!DatabaseCatalog::instance().tryGetTable({database, table}, query_context))
    {
        return {
            jsonErrorResponse(fmt::format("Stream {}.{} doesn't exist", database, table), ErrorCodes::UNKNOWN_STREAM),
            HTTPResponse::HTTP_BAD_REQUEST};
    }

    LOG_INFO(log, "Updating stream {}.{}", database, table);

    setupDistributedQueryParameters({}, payload);
    String resp;

    /// TTL
    if (payload->has("ttl_expression"))
    {
        String query;
        const auto & ttl_expression = payload->get("ttl_expression").toString();
        if (ttl_expression.empty())
            query = fmt::format("ALTER STREAM {}.`{}` REMOVE TTL", database, table);
        else
            query = fmt::format("ALTER STREAM {}.`{}` MODIFY TTL {}", database, table, ttl_expression);
        resp = processQuery(query);
    }

    /// stream store TTL
    std::vector<String> settings;
    for (const auto & [k, v] : ProtonConsts::LOG_STORE_SETTING_NAME_TO_KAFKA)
        if (payload->has(k))
            settings.push_back(fmt::format("{}={}", k, payload->get(k).toString()));

    if (!settings.empty())
    {
        const auto query = fmt::format("ALTER STREAM {}.`{}` MODIFY SETTING {}", database, table, boost::algorithm::join(settings, ","));
        resp = processQuery(query);
    }

    return {resp, HTTPResponse::HTTP_OK};
}

std::pair<String, Int32> TableRestRouterHandler::executeDelete(const Poco::JSON::Object::Ptr & /* payload */) const
{
    const String & table = getPathParameter("stream");

    String query = fmt::format("DROP STREAM {}.`{}`", database, table);
    if (hasQueryParameter("mode"))
    {
        const auto & mode = getQueryParameter("mode");
        auto delete_mode = toDeleteMode(mode);
        if (delete_mode == DeleteMode::INVALID)
            return {
                jsonErrorResponse("No support delete mode: " + mode, ErrorCodes::BAD_REQUEST_PARAMETER), HTTPResponse::HTTP_BAD_REQUEST};
        else if (delete_mode == DeleteMode::TRUNCATE)
            query = "TRUNCATE STREAM " + database + ".`" + table + "`";
    }

    if (!DatabaseCatalog::instance().tryGetTable({database, table}, query_context))
    {
        return {
            jsonErrorResponse(fmt::format("Stream {}.{} doesn't exist", database, table), ErrorCodes::UNKNOWN_STREAM),
            HTTPResponse::HTTP_BAD_REQUEST};
    }

    setupDistributedQueryParameters({});

    return {processQuery(query), HTTPResponse::HTTP_OK};
}

void TableRestRouterHandler::buildRetentionSettings(Poco::JSON::Object & resp_table, const String & database_, const String & table) const
{
    auto table_id = query_context->resolveStorageID(StorageID(database_, table), Context::ResolveOrdinary);
    if (nlog::NativeLog::instance(query_context).enabled())
    {
        auto storage = DatabaseCatalog::instance().tryGetTable(StorageID(database_, table), query_context);
        if (storage)
        {
            MergeTreeSettingsPtr settings;
            if (auto * stream = storage->as<StorageStream>())
                settings = stream->getSettings();
            else if (auto * mv = storage->as<StorageMaterializedView>())
            {
                settings = mv->getSettings();

                /// set ttl, only work for single instance environment
                auto target = mv->getTargetTable();
                if (target)
                {
                    TTLTableDescription ttl = target->getInMemoryMetadataPtr()->getTableTTLs();
                    if (ttl.definition_ast)
                        resp_table.set("ttl", queryToString(ttl.definition_ast, true));
                }
            }

            if (settings)
            {
                resp_table.set("logstore_flush_messages", static_cast<Int64>(settings->logstore_flush_messages));
                resp_table.set("logstore_flush_ms", static_cast<Int64>(settings->logstore_flush_ms));
                resp_table.set("logstore_retention_bytes", static_cast<Int64>(settings->logstore_retention_bytes));
                resp_table.set("logstore_retention_ms", static_cast<Int64>(settings->logstore_retention_ms));
            }
        }
    }
    else
    {
        UUID table_uuid = table_id.uuid;
        auto storage = DatabaseCatalog::instance().tryGetTable(StorageID(database, table), query_context);
        if (storage)
        {
            if (auto * mv = storage->as<StorageMaterializedView>())
            {
                if (auto target = mv->getTargetTable())
                    table_uuid = target->getStorageID().uuid;
            }
        }

        /// Kafka log store
        if (table_uuid != UUIDHelpers::Nil)
        {
            auto klog = klog::KafkaWALPool::instance(query_context).getMeta();
            const auto & params = klog->get(toString(table_uuid));
            for (const auto & [k, v] : ProtonConsts::LOG_STORE_SETTING_NAME_TO_KAFKA)
                if (params.contains(v))
                    resp_table.set(k, std::stoll(params.at(v)));
        }
    }
}

void TableRestRouterHandler::buildColumnsJSON(Poco::JSON::Object & resp_table, const ASTColumns * columns_list) const
{
    Poco::JSON::Array columns_mapping_json;
    if (columns_list && columns_list->columns)
    {
        const auto & columns_ast = columns_list->columns;
        for (auto & ast_it : columns_ast->children)
        {
            Poco::JSON::Object column_mapping_json;
            const auto & col_decl = ast_it->as<ASTColumnDeclaration &>();

            Streaming::columnDeclarationToJSON(column_mapping_json, col_decl);
            columns_mapping_json.add(column_mapping_json);
        }
    }
    resp_table.set("columns", columns_mapping_json);
}

String TableRestRouterHandler::getEngineExpr(
    const Poco::JSON::Object::Ptr & payload) const /// NOLINT(readability-convert-member-functions-to-static)
{
    const auto & shards = getStringValueFrom(payload, "shards", "1");
    const auto & replication_factor = getStringValueFrom(payload, "replication_factor", "1");
    const auto & shard_by_expression = getStringValueFrom(payload, "shard_by_expression", "rand()");

    return fmt::format("Stream({}, {}, {})", shards, replication_factor, shard_by_expression);
}

String TableRestRouterHandler::getPartitionExpr(const Poco::JSON::Object::Ptr & payload, const String & default_granularity)
{
    /// If `partition by` is explicitly specified, honor it
    const auto & partition_by_expression = getStringValueFrom(payload, "partition_by_expression", String());
    if (!partition_by_expression.empty())
        return partition_by_expression;

    const auto & partition_by_granularity = getStringValueFrom(payload, "partition_by_granularity", default_granularity);
    return granularity_func_mapping[partition_by_granularity];
}

String TableRestRouterHandler::getStringValueFrom(const Poco::JSON::Object::Ptr & payload, const String & key, const String & default_value)
{
    return payload->has(key) ? payload->get(key).toString() : default_value;
}

String
TableRestRouterHandler::getCreationSQL(const Poco::JSON::Object::Ptr & payload, const String & host_shards, const String & uuid) const
{
    const auto & time_col = getStringValueFrom(payload, ProtonConsts::RESERVED_EVENT_TIME_API_NAME, ProtonConsts::RESERVED_EVENT_TIME);
    std::vector<String> create_segments;

    if (isExternal())
    {
        return fmt::format(
            "CREATE EXTERNAL STREAM `{}`.`{}` ({}) SETTINGS {}",
            database,
            payload->get("name").toString(),
            getColumnsDefinition(payload),
            getSettings(payload));
    }

    const auto & order_by = getOrderByExpr(payload, time_col, getDefaultOrderByGranularity());
    const auto & primary_key = payload->has("primary_key") ? payload->get("primary_key").toString() : order_by;
    const auto & partition_by = getPartitionExpr(payload, getDefaultPartitionGranularity());

    if (uuid.empty())
    {
        create_segments.push_back(fmt::format(
            "CREATE STREAM `{}`.`{}` ({}) ENGINE = {} PARTITION BY {} PRIMARY KEY ({}) ORDER BY ({})",
            database,
            payload->get("name").toString(),
            getColumnsDefinition(payload),
            getEngineExpr(payload),
            partition_by,
            primary_key,
            order_by));
    }
    else
    {
        create_segments.push_back(fmt::format(
            "CREATE STREAM `{}`.`{}` UUID '{}' ({}) ENGINE = {} PARTITION BY {} PRIMARY KEY ({}) ORDER BY ({})",
            database,
            payload->get("name").toString(),
            uuid,
            getColumnsDefinition(payload),
            getEngineExpr(payload),
            partition_by,
            primary_key,
            order_by));
    }

    if (payload->has("ttl_expression"))
        /// FIXME  Enforce time based TTL only
        create_segments.push_back(fmt::format("TTL {}", payload->get("ttl_expression").toString()));

    create_segments.push_back(fmt::format("SETTINGS subtype='{}'", subtype()));

    if (payload->has("mode"))
        create_segments.push_back(fmt::format(", mode='{}'", payload->get("mode").toString()));

    if (!host_shards.empty())
        create_segments.push_back(fmt::format(", host_shards='{}'", host_shards));

    Streaming::getAndValidateStorageSetting(
        [this](const auto & key) -> String {
            if (hasQueryParameter(key))
                return getQueryParameter(key);

            return "";
        },
        [&](const auto & key, const auto & value) {
            if (key != "subtype" && key != "mode")
                create_segments.push_back(fmt::format(", {}='{}'", key, value));
        });

    return boost::algorithm::join(create_segments, " ");
}

void TableRestRouterHandler::buildTablesJSON(Poco::JSON::Object & resp, const TablePtrs & tables) const
{
    Poco::JSON::Array tables_mapping_json;
    std::unordered_set<String> table_names;

    bool include_internal_streams
        = getQueryParameterBool("include_internal_streams", query_context->getSettingsRef().include_internal_streams.value);
    for (const auto & table : tables)
    {
        /// If include_internal_streams = false, ignore internal streams
        if (table->name.starts_with(".inner.") && !include_internal_streams)
            continue;

        if (table_names.contains(table->name))
            continue;

        if (table->engine_full.find("subtype = 'rawstore'") == String::npos)
            continue;

        const auto & query_ptr = parseQuery(table->create_table_query, query_context);
        const auto & create = query_ptr->as<const ASTCreateQuery &>();

        Poco::JSON::Object table_mapping_json;
        table_mapping_json.set("name", table->name);
        table_mapping_json.set("engine", table->engine);
        table_mapping_json.set("order_by_expression", table->sorting_key);
        table_mapping_json.set("partition_by_expression", table->partition_key);

        if (create.storage->ttl_table)
            table_mapping_json.set("ttl", queryToString(*create.storage->ttl_table));

        tables_mapping_json.add(table_mapping_json);
        table_names.insert(table->name);
    }

    resp.set("data", tables_mapping_json);
}
}
