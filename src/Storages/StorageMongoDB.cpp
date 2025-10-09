#include "config.h"

#if USE_MONGODB

#include <Processors/Sources/MongoDBSource.h>

#include <Core/Joins.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/FieldToDataType.h>
#include <Formats/BSONTypes.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMongoDB.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Common/BSONCXXHelper.h>
#include <Common/ErrorCodes.h>
#include <Common/FieldVisitorToString.h>
#include <Common/logger_useful.h>
#include <Common/parseAddress.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <bsoncxx/json.hpp>

#include <memory>
#include <optional>

using bsoncxx::to_json;
using bsoncxx::builder::basic::document;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_array;
using bsoncxx::builder::basic::make_document;

namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int NOT_IMPLEMENTED;
}

void MongoDBConfiguration::checkHosts(const ContextPtr & context) const
{
    // Because domain records will be resolved inside the driver, we can't check resolved IPs for our restrictions.
    for (const auto & host : uri->hosts())
        context->getRemoteHostFilter().checkHostAndPort(host.name, toString(host.port));
}

StorageMongoDB::StorageMongoDB(
    const StorageID & table_id_,
    MongoDBConfiguration configuration_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment)
    : IStorage{table_id_}
    , configuration{std::move(configuration_)}
    , log(getLogger("StorageMongoDB (" + table_id_.getFullTableName() + ")"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageMongoDB::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);

    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        sample_block.insert({column_data.type, column_data.name});
    }

    auto options = mongocxx::options::find{};

    return Pipe(std::make_shared<MongoDBSource>(
        *configuration.uri,
        configuration.collection,
        buildMongoDBQuery(context, options, storage_snapshot, query_info, sample_block),
        std::move(options),
        sample_block,
        max_block_size));
}

MongoDBConfiguration StorageMongoDB::getConfiguration(ASTs engine_args, ContextPtr context)
{
    MongoDBConfiguration configuration;
    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, context))
    {
        if (named_collection->has("uri"))
        {
            validateNamedCollection(*named_collection, {"uri", "collection"}, {"oid_columns"});
            configuration.uri = std::make_unique<mongocxx::uri>(named_collection->get<String>("uri"));
        }
        else
        {
            validateNamedCollection(
                *named_collection, {"host", "port", "user", "password", "database", "collection"}, {"options", "oid_columns"});
            String user = named_collection->get<String>("user");
            String auth_string;
            String escaped_password;
            Poco::URI::encode(named_collection->get<String>("password"), "!?#/'\",;:$&()[]*+=@", escaped_password);
            if (!user.empty())
                auth_string = fmt::format("{}:{}@", user, escaped_password);
            configuration.uri = std::make_unique<mongocxx::uri>(fmt::format(
                "mongodb://{}{}:{}/{}?{}",
                auth_string,
                named_collection->get<String>("host"),
                named_collection->get<String>("port"),
                named_collection->get<String>("database"),
                named_collection->getOrDefault<String>("options", "")));
        }
        configuration.collection = named_collection->get<String>("collection");
        if (named_collection->has("oid_columns"))
            boost::split(configuration.oid_fields, named_collection->get<String>("oid_columns"), boost::is_any_of(","));
    }
    else
    {
        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context);

        if (engine_args.size() >= 5 && engine_args.size() <= 7)
        {
            configuration.collection = checkAndGetLiteralArgument<String>(engine_args[2], "collection");

            String options;
            if (engine_args.size() >= 6)
                options = checkAndGetLiteralArgument<String>(engine_args[5], "options");

            String user = checkAndGetLiteralArgument<String>(engine_args[3], "user");
            String auth_string;
            String escaped_password;
            Poco::URI::encode(checkAndGetLiteralArgument<String>(engine_args[4], "password"), "!?#/'\",;:$&()[]*+=@", escaped_password);
            if (!user.empty())
                auth_string = fmt::format("{}:{}@", user, escaped_password);
            auto parsed_host_port = parseAddress(checkAndGetLiteralArgument<String>(engine_args[0], "host:port"), 27017);
            configuration.uri = std::make_unique<mongocxx::uri>(fmt::format(
                "mongodb://{}{}:{}/{}?{}",
                auth_string,
                parsed_host_port.first,
                parsed_host_port.second,
                checkAndGetLiteralArgument<String>(engine_args[1], "database"),
                options));
            if (engine_args.size() == 7)
                boost::split(
                    configuration.oid_fields, checkAndGetLiteralArgument<String>(engine_args[6], "oid_columns"), boost::is_any_of(","));
        }
        else if (engine_args.size() == 2 || engine_args.size() == 3)
        {
            configuration.collection = checkAndGetLiteralArgument<String>(engine_args[1], "database");
            configuration.uri = std::make_unique<mongocxx::uri>(checkAndGetLiteralArgument<String>(engine_args[0], "host"));
            if (engine_args.size() == 3)
                boost::split(
                    configuration.oid_fields, checkAndGetLiteralArgument<String>(engine_args[2], "oid_columns"), boost::is_any_of(","));
        }
        else
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Incorrect number of arguments. Example usage: "
                "MongoDB('host:port', 'database', 'collection', 'user', 'password'[, options[, oid_columns]]) or MongoDB('uri', "
                "'collection'[, oid columns]).");
    }

    configuration.checkHosts(context);

    return configuration;
}

static std::string mongoFuncName(const std::string & func)
{
    if (func == "equals")
        return "$eq";
    if (func == "not_equals")
        return "$ne";
    if (func == "greater_than" || func == "greater")
        return "$gt";
    if (func == "less_than" || func == "less")
        return "$lt";
    if (func == "greater_or_equals")
        return "$gte";
    if (func == "less_or_equals")
        return "$lte";
    if (func == "in")
        return "$in";
    if (func == "not_in")
        return "$nin";
    if (func == "and")
        return "$and";
    if (func == "or")
        return "$or";

    return "";
}

static std::string invertMongoComparison(const std::string & func)
{
    if (func == "$gt")
        return "$lt";
    if (func == "$gte")
        return "$lte";
    if (func == "$lt")
        return "$gt";
    if (func == "$lte")
        return "$gte";

    return func;
}

/// proton: starts. Rewrite without Analyzer
const ColumnDescription * StorageMongoDB::getColumn(const ASTPtr & node, const StorageSnapshotPtr & storage_snapshot)
{
    return storage_snapshot->metadata->getColumns().tryGet(node->getColumnName());
}

std::optional<bsoncxx::document::value> StorageMongoDB::visitWhereFunctionArguments(
    const ColumnDescription * column_node, const ASTLiteral * const_node, const ASTFunction * func, bool invert_comparison)
{
    auto func_name = mongoFuncName(func->name);

    if (func_name.empty())
    {
        LOG_DEBUG(log, "MongoDB function name not found for '{}'", func->name);
        return {};
    }

    if (invert_comparison)
    {
        func_name = invertMongoComparison(func_name);
    }

    auto const_type = applyVisitor(FieldToDataType(), const_node->value);
    auto column_type = column_node->type;
    auto const_value = const_node->value;

    bool is_const_number = WhichDataType(const_type).isNumber();
    bool is_column_number = WhichDataType(column_type).isNumber();

    if (func_name == "$in" || func_name == "$nin")
    {
        if (const_value.getType() == Field::Types::Array)
        {
            column_type = std::make_shared<DataTypeArray>(column_type);
        }
        else if (const_value.getType() == Field::Types::Tuple)
        {
            auto & value_tuple = const_value.safeGet<Tuple>();
            const_value = Array(value_tuple.begin(), value_tuple.end());
            column_type = std::make_shared<DataTypeArray>(column_type);
        }
    }

    /// Conversion is required because MongoDB cannot perform implicit cast and the result of WHERE clause may be incorrect.
    /// But implicit conversion between numbers works well and doesn't affect the result of WHERE clause.
    if (!const_type->equals(*column_type) && (!is_const_number || !is_column_number))
    {
        auto converted_value = convertFieldToType(const_value, *column_type, const_type.get());

        if (converted_value.isNull())
        {
            auto value_string = applyVisitor(FieldVisitorToString(), const_value);
            LOG_DEBUG(log, "Cannot convert constant value {} to column type {}", value_string, column_type->getName());
            return {};
        }

        const_type = column_type;
        const_value = std::move(converted_value);
    }

    auto func_value = BSONCXXHelper::fieldAsBSONValue(const_value, const_type, configuration.isOidColumn(column_node->name));

    if (func_name == "$in" && func_value.view().type() != bsoncxx::v_noabi::type::k_array)
        func_name = "$eq";
    if (func_name == "$nin" && func_value.view().type() != bsoncxx::v_noabi::type::k_array)
        func_name = "$ne";

    return make_document(kvp(column_node->name, make_document(kvp(func_name, std::move(func_value)))));
}

std::optional<bsoncxx::document::value>
StorageMongoDB::visitWhereFunction(const ContextPtr & context, const ASTFunction * func, const StorageSnapshotPtr & storage_snapshot)
{
    const auto & argument_nodes = func->arguments->children;
    if (argument_nodes.empty())
        return {};

    if (argument_nodes.size() == 1)
    {
        const auto * column = getColumn(argument_nodes.at(0), storage_snapshot);
        if (!column)
            return {};

        // Only these function can have exactly one argument and be passed to MongoDB.
        if (func->name == "is_null")
            return make_document(kvp(column->name, make_document(kvp("$eq", bsoncxx::types::b_null{}))));
        if (func->name == "is_not_null")
            return make_document(kvp(column->name, make_document(kvp("$ne", bsoncxx::types::b_null{}))));
        if (func->name == "empty")
            return make_document(kvp(column->name, make_document(kvp("$in", make_array(bsoncxx::types::b_null{}, "")))));
        if (func->name == "not_empty")
            return make_document(kvp(column->name, make_document(kvp("$nin", make_array(bsoncxx::types::b_null{}, "")))));

        return {};
    }

    if (argument_nodes.size() == 2)
    {
        const auto * left_column = getColumn(argument_nodes.at(0), storage_snapshot);
        const auto * right_const = argument_nodes.at(1)->as<ASTLiteral>();

        if (left_column && right_const)
            return visitWhereFunctionArguments(left_column, right_const, func, false);

        /// We cannot invert "in" and "not_in" functions.
        if (func->name == "in" || func->name == "not_in")
            return {};

        const auto * left_const = argument_nodes.at(0)->as<ASTLiteral>();
        const auto * right_column = getColumn(argument_nodes.at(1), storage_snapshot);

        if (left_const && right_column)
            return visitWhereFunctionArguments(right_column, left_const, func, true);
    }

    auto func_name = mongoFuncName(func->name);
    if (func_name.empty())
        return {};

    auto arr = bsoncxx::builder::basic::array{};

    for (const auto & elem : func->arguments->children)
    {
        auto res_value = visitWhereNode(context, elem, storage_snapshot);
        if (!res_value)
            return {};

        arr.append(*res_value);
    }

    return make_document(kvp(func_name, arr));
}

std::optional<bsoncxx::document::value>
StorageMongoDB::visitWhereNode(const ContextPtr & context, const ASTPtr & where_node, const StorageSnapshotPtr & storage_snapshot)
{
    if (const auto * func = where_node->as<ASTFunction>())
        return visitWhereFunction(context, func, storage_snapshot);

    return {};
}
/// proton: ends

bsoncxx::document::value StorageMongoDB::buildMongoDBQuery(
    const ContextPtr & context,
    mongocxx::options::find & options,
    const StorageSnapshotPtr & storage_snapshot,
    const SelectQueryInfo & query_info,
    const Block & sample_block)
{
    document projection{};
    for (const auto & column : sample_block)
        projection.append(kvp(column.name, 1));

    LOG_DEBUG(log, "MongoDB projection has built: '{}'", bsoncxx::to_json(projection));
    options.projection(projection.extract());

    const auto & query = query_info.query->as<ASTSelectQuery &>();

    bool throw_on_error = context->getSettingsRef().mongodb_throw_on_unsupported_query;
    if (throw_on_error)
    {
        if (query.having())
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "HAVING section is not supported. You can disable this error with 'SET mongodb_throw_on_unsupported_query=0', but this may "
                "cause poor performance, and is highly not recommended");
        if (query.groupBy())
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "GROUP BY section is not supported. You can disable this error with 'SET mongodb_throw_on_unsupported_query=0', but this "
                "may cause poor performance, and is highly not recommended");
        if (query.window())
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "WINDOW section is not supported. You can disable this error with 'SET mongodb_throw_on_unsupported_query=0', but this may "
                "cause poor performance, and is highly not recommended");
        if (query.prewhere())
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "PREWHERE section is not supported. You can disable this error with 'SET mongodb_throw_on_unsupported_query=0', but this "
                "may cause poor performance, and is highly not recommended");
        if (query.limitBy())
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "LIMIT BY section is not supported. You can disable this error with 'SET mongodb_throw_on_unsupported_query=0', but this "
                "may cause poor performance, and is highly not recommended");
    }

    auto on_error = [&](const IAST * node) {
        /// Reset limit, because if we omit ORDER BY, it should not be applied
        options.limit(0);

        if (throw_on_error)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Only simple queries are supported, failed to convert expression '{}' to MongoDB query. "
                "You can disable this restriction with 'SET mongodb_throw_on_unsupported_query=0', to read the full table and process on "
                "Timeplus side (this may cause poor performance)",
                node->formatForErrorMessage());
        LOG_WARNING(log, "Failed to build MongoDB query for '{}'", node ? node->formatForErrorMessage() : "<unknown>");
    };

    /// proton: starts
    auto [limit_value, offset_value] = InterpreterSelectQuery::getLimitLengthAndOffset(query, context);
    if (limit_value > 0)
    {
        if (!query.groupBy() && !query.limitBy())
            options.limit(limit_value + offset_value);
    }

    if (auto order_by = query.orderBy())
    {
        document sort{};
        for (const auto & child : order_by->children)
        {
            auto ss = child->dumpTree(0);
            LOG_INFO(log, "{}", ss);
            if (const auto * sort_node = child->as<ASTOrderByElement>())
            {
                if (sort_node->getFillFrom() || sort_node->getFillTo() || sort_node->getFillStep())
                    on_error(sort_node);

                if (sort_node->children.size() == 1 && !sort_node->children[0]->getColumnName().empty())
                    sort.append(kvp(sort_node->children[0]->getColumnName(), sort_node->direction));
                else
                    on_error(sort_node);
            }
            else
                on_error(sort_node);
        }

        if (!sort.view().empty())
        {
            LOG_DEBUG(log, "MongoDB sort has built: '{}'", bsoncxx::to_json(sort));
            options.sort(sort.extract());
        }
    }

    if (auto where_node = query.where())
    {
        auto filter = visitWhereNode(context, where_node, storage_snapshot);
        if (!filter)
        {
            on_error(where_node.get());
            return make_document();
        }

        LOG_DEBUG(log, "MongoDB query has built: '{}'.", bsoncxx::to_json(*filter));
        return std::move(*filter);
    }
    /// proton: ends

    return make_document();
}

void registerStorageMongoDB(StorageFactory & factory)
{
    factory.registerStorage(
        "MongoDB",
        [](const StorageFactory::Arguments & args) {
            return std::make_shared<StorageMongoDB>(
                args.table_id,
                StorageMongoDB::getConfiguration(args.engine_args, args.getLocalContext()),
                args.columns,
                args.constraints,
                args.comment);
        },
        {
            .source_access_type = AccessType::MONGO,
        });
}

/// proton: starts
class MongoDBSink : public SinkToStorage
{
public:
    MongoDBSink(
        const MongoDBConfiguration & configuration, const StorageMetadataPtr & metadata_snapshot_, std::optional<Block> sample_block)
        : SinkToStorage(sample_block.has_value() ? sample_block.value() : metadata_snapshot_->getSampleBlock(), ProcessorID::MongoDBSinkID)
        , metadata_snapshot(metadata_snapshot_)
        , client{*configuration.uri}
        , database{client.database(configuration.uri->database())}
        , collection{database.collection(configuration.collection)}
        , oid_fields(configuration.oid_fields)
    {
    }

    String getName() const override { return "StorageMongoDBSink"; }

    void consume(Chunk chunk) override
    {
        if (!chunk.hasRows())
            return;

        const auto & columns_type_and_name = getHeader().getColumnsWithTypeAndName();
        const auto rows = chunk.getNumRows();
        const auto cols = chunk.getNumColumns();

        std::vector<bsoncxx::document::value> documents;
        documents.reserve(rows);

        for (size_t r = 0; r < rows; ++r)
        {
            document builder;
            for (size_t c = 0; c < cols; ++c)
            {
                const auto & column = *(chunk.getColumns()[c]);
                const auto & column_type = columns_type_and_name[c].type;
                const auto & column_name = columns_type_and_name[c].name;
                const auto is_oid = oid_fields.contains(column_name);
                builder.append(kvp(column_name, BSONCXXHelper::fieldAsBSONValue(column[r], column_type, is_oid)));
            }
            documents.push_back(builder.extract());
        }

        collection.insert_many(documents);
    }

private:
    StorageMetadataPtr metadata_snapshot;

    mongocxx::client client;
    mongocxx::database database;
    mongocxx::collection collection;
    std::unordered_set<String> oid_fields;
};

SinkToStoragePtr StorageMongoDB::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    auto maybe_sample_block = getWriteSampleBlock(query, metadata_snapshot, local_context);
    return std::make_shared<MongoDBSink>(configuration, metadata_snapshot, std::move(maybe_sample_block));
}
/// proton: ends

}
#endif
