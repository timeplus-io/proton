#include <Interpreters/Apply/MetadataUpdater.h>

#include <Access/AccessControl.h>
#include <Access/AccessEntityIO.h>
#include <Access/MetaStoreAccessStorage.h>
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Protocol/CreateDiskRequestData.h>
#include <Cluster/Protocol/DeleteDiskRequestData.h>
#include <Cluster/Requests/CreateDiskRequest.h>
#include <Cluster/Requests/DeleteDiskRequest.h>
#include <Disks/getOrCreateDiskFromAST.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>

namespace DB
{

namespace ErrorCodes
{
extern const int OK;
extern const int BAD_REQUEST_PARAMETER;
extern const int CANNOT_LOAD_CONFIG;
}

namespace
{

}

void MetadataUpdater::handleCreateDisk(
    const cluster::CreateDiskRequest & request,
    const cluster::RequestHeader & request_header,
    uint64_t sn) const
{
    try
    {
        const auto & request_data = request.data();
        /// TODO: add to DiskSelector in-memory disks

        auto disk_desc = request_data.disk;
        ASTPtr disk_func;
        ParserFunction disk_func_p;
        disk_func = parseQuery(disk_func_p, disk_desc->config, 0, 0);

        /// Parse config to disk function (AST)
        if (!disk_func || disk_func->as<ASTFunction>()->name != "disk")
            throw Exception(ErrorCodes::CANNOT_LOAD_CONFIG, "disk {} with invalid config: [{}]", disk_desc->name, disk_desc->config);

        const auto & ast_function = assert_cast<const ASTFunction &>(*disk_func);
        auto query_context = Context::createCopy(Globals::getGlobalContext().getGlobalContext());
        getOrCreateDiskFromDiskAST(ast_function, query_context, disk_desc->name);

        /// Save/replace disk in MetaDB.
        auto & metastore = Globals::getMetaStore();
        auto & db = metastore.getMetaDB();
        auto res = db.saveDisk(*disk_desc, cluster::AppliedSequence(sn));
        if (res.hasError())
            LOG_ERROR(logger, "Failed to create disk: {}", res.error_message);

        meta_store->ackProposal(request_header.correlationID(), sn, res.error_code, std::move(res.error_message));
    }
    catch (const Exception & ex)
    {
        LOG_ERROR(logger, "Failed to create disk: error={}", ex.message());
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(request_header.correlationID(), sn, ex.code(), std::string{ex.message()});
    }
}

void MetadataUpdater::handleDeleteDisk(
    const cluster::DeleteDiskRequest & request,
    const cluster::RequestHeader & request_header,
    uint64_t sn) const
{
    try
    {
        const auto & request_data = request.data();
        const auto & name = request_data.name;
        /// Drop from memory
        global_context->deleteDisk(name);

        /// Drop from metastore
        Globals::getGlobalContext().deleteDisk(name);
        meta_store->ackProposal(request_header.correlationID(), sn, ErrorCodes::OK, /*error_message=*/"");
        LOG_INFO(logger, "Delete disk: {} successfully", name);
    }
    catch (const Exception & ex)
    {
        LOG_ERROR(logger, "Failed to delete disk, error={}", ex.message());
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(request_header.correlationID(), sn, ex.code(), std::string{ex.message()});
    }
}
}
