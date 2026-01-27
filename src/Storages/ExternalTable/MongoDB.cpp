#include <Storages/ExternalTable/MongoDB.h>

#if USE_MONGODB

#include <Storages/ExternalTable/ExternalTableFactory.h>
#include <Storages/ExternalTable/ExternalTableSettings.h>
#include <Storages/StorageMongoDB.h>
#include <Common/ThreadPool.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

namespace DB
{

void registerMongoDBExternalTable(ExternalTableFactory & factory)
{
    factory.registerExternalTable(
        "mongodb",
        [](const StorageID & table_id,
           const StorageInMemoryMetadata & storage_metadata,
           const ASTCreateQuery &,
           std::unique_ptr<ExternalTableSettings> settings,
           bool attach,
           ContextPtr context) -> StoragePtr {
            return std::make_shared<ExternalTable::MongoDB>(table_id, storage_metadata, std::move(settings), attach, context);
        });
}

namespace ExternalTable
{
MongoDB::MongoDB(
    const StorageID & storage_id,
    const StorageInMemoryMetadata & storage_metadata,
    std::unique_ptr<ExternalTableSettings> settings_,
    bool attach_,
    ContextPtr context_)
    : StorageExternalTable(storage_id, storage_metadata, std::move(settings_), attach_, context_)
{
    MongoDBConfiguration configuration;

    String uri = settings->uri.value;
    if (uri.empty())
    {
        String address = settings->address.value;
        String database = settings->database.value;
        if (address.empty() || database.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Neither MongoDB uri nor address/database is specified.");

        String user = settings->user.value;
        String auth_string;
        String escaped_password;
        Poco::URI::encode(settings->password.value, "!?#/'\",;:$&()[]*+=@", escaped_password);
        if (!user.empty())
            auth_string = fmt::format("{}:{}@", user, escaped_password);

        uri = fmt::format("mongodb://{}{}/{}?{}", auth_string, address, database, settings->connection_options.value);
    }

    configuration.uri = std::make_unique<mongocxx::uri>(uri);

    configuration.collection = settings->collection.value;
    if (configuration.collection.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MongoDB collection is not specified.");

    if (!settings->oid_columns.value.empty())
        boost::split(configuration.oid_fields, settings->oid_columns.value, boost::is_any_of(","));

    db = std::make_shared<StorageMongoDB>(
        storage_id, std::move(configuration), storage_metadata.columns, storage_metadata.constraints, storage_metadata.comment);
}
}
}

#else
namespace DB
{
class ExternalTableFactory;

void registerMongoDBExternalTable(ExternalTableFactory &)
{
}
}
#endif
