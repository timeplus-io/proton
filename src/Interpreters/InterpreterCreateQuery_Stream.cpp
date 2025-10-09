#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/queryToString.h>

#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/CreateDatabaseRequest.h>

namespace DB
{
BlockIO InterpreterCreateQuery::createDatabaseOnCluster(ASTCreateQuery & create)
{
    if (create.uuid == UUIDHelpers::Nil)
        create.uuid = UUIDHelpers::generateV4();

    auto database_desc = std::make_shared<cluster::protocol::DatabaseDescriptor>(create.getDatabase(), queryToString(create));
    if (create.comment != nullptr)
        database_desc->comment = queryToString(*create.comment);
    database_desc->uuid = create.uuid;
    database_desc->create_timestamp_ms = DB::UTCMilliseconds::now();
    database_desc->last_modify_timestamp_ms = database_desc->create_timestamp_ms;
    database_desc->created_by = getContext()->getUserName();
    database_desc->last_modified_by = database_desc->created_by;

    auto timeout_ms = getContext()->getSettingsRef().query_timeout_sec * 1000;
    auto & metastore = Globals::getMetaStore();
    auto req
        = std::make_shared<cluster::CreateDatabaseRequest>(std::move(database_desc), metastore.nodeID(), timeout_ms, /*request_version=*/2);
    auto resp = metastore.createDatabase(std::move(req));
    if (resp->hasError())
        throw DB::Exception::createRuntime(resp->error().error_code, resp->error().error_message);

    return {};
}

}
