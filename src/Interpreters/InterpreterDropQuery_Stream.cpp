#include <Interpreters/InterpreterDropQuery.h>

#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/DeleteDatabaseRequest.h>
#include <Parsers/queryToString.h>

namespace DB
{

BlockIO InterpreterDropQuery::executeToDatabaseOnCluster(const ASTDropQuery & query)
{
    auto & metastore = Globals::getMetaStore();
    auto req = std::make_shared<cluster::DeleteDatabaseRequest>(
        query.getDatabase(), queryToString(query), metastore.nodeID(), /*timeout_ms_=*/10'000, /*data_version_=*/2);
    auto resp = metastore.deleteDatabase(std::move(req));
    if (resp->hasError())
        throw DB::Exception::createRuntime(resp->error().error_code, resp->error().error_message);

    return {};
}

}
