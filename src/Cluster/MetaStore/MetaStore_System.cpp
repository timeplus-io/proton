#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/MetaStore/LocalMetaQueue.h>
#include <Cluster/MetaStore/MetaDB.h>
#include <Cluster/MetaStore/ProposalRegistry.h>
#include <Cluster/Requests/ChangeLogLevelRequest.h>
#include <Cluster/Requests/ChangeLogLevelResponse.h>
#include <Cluster/Requests/PipPythonPackageRequest.h>
#include <Cluster/Requests/PipPythonPackageResponse.h>
#include <Common/logger_useful.h>

namespace DB::ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace cluster::meta
{

ChangeLogLevelResponsePtr MetaStore::changeLogLevel(ChangeLogLevelRequestPtr request)
{
    /// Always supported

    if (!local_meta_queue || !proposal_registry)
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "LocalMetaQueue not initialized");
    }

    /// Submit to LocalMetaQueue for processing by MetadataUpdater
    auto correlation_id = nextCorrelationID();
    auto future = proposal_registry->registerProposal(correlation_id);
    cluster::RequestHeader header;
    header.data() = protocol::RequestHeaderData(request->opCode(), request->version(), correlation_id);
    auto serialized_bytes = request->serializeWithHeader(header);
    String serialized_data(reinterpret_cast<const char*>(serialized_bytes.data()), serialized_bytes.size());
    
    LOG_INFO(logger, "Submitting ChangeLogLevel request to LocalMetaQueue: correlation_id=0x{:x}", 
             correlation_id);
    
    auto result = local_meta_queue->append(serialized_data, correlation_id);
    if (result.hasError())
    {
        proposal_registry->cancelProposal(correlation_id, Error(result.err.error_code, result.err.error_message));
        return std::make_shared<ChangeLogLevelResponse>(
            Error(result.err.error_code, result.err.error_message),
            /*data_version_=*/1);
    }

    /// Wait for MetadataUpdater to process and get result
    LOG_INFO(logger, "Waiting for ChangeLogLevel response: correlation_id=0x{:x} sn={}", correlation_id, result.result);
    auto error = future.get();
    LOG_INFO(logger, "ChangeLogLevel response received: correlation_id=0x{:x} error_code={}", 
             correlation_id, error.hasError() ? error.error_code : 0);
    
    if (error.hasError())
    {
        return std::make_shared<ChangeLogLevelResponse>(
            std::move(error),
            /*data_version_=*/1);
    }

    /// Return success
    return std::make_shared<ChangeLogLevelResponse>(
        /*replica_leader_=*/0,
        /*sn_=*/result.result,
        /*data_version_=*/1);
}

PipPythonPackageResponsePtr MetaStore::pythonPackageOperation(PipPythonPackageRequestPtr request)
{
    /// Always supported

    if (!local_meta_queue || !proposal_registry)
    {
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "LocalMetaQueue not initialized");
    }

    /// Submit to LocalMetaQueue for processing by MetadataUpdater
    auto correlation_id = nextCorrelationID();
    auto future = proposal_registry->registerProposal(correlation_id);
    cluster::RequestHeader header;
    header.data() = protocol::RequestHeaderData(request->opCode(), request->version(), correlation_id);
    auto serialized_bytes = request->serializeWithHeader(header);
    String serialized_data(reinterpret_cast<const char*>(serialized_bytes.data()), serialized_bytes.size());
    
    LOG_INFO(logger, "Submitting PipPythonPackage request to LocalMetaQueue: correlation_id=0x{:x}", 
             correlation_id);
    
    auto result = local_meta_queue->append(serialized_data, correlation_id);
    if (result.hasError())
    {
        proposal_registry->cancelProposal(correlation_id, Error(result.err.error_code, result.err.error_message));
        return std::make_shared<PipPythonPackageResponse>(
            Error(result.err.error_code, result.err.error_message),
            /*data_version_=*/1);
    }

    /// Wait for MetadataUpdater to process and get result
    LOG_INFO(logger, "Waiting for PipPythonPackage response: correlation_id=0x{:x} sn={}", correlation_id, result.result);
    auto error = future.get();
    LOG_INFO(logger, "PipPythonPackage response received: correlation_id=0x{:x} error_code={}", 
             correlation_id, error.hasError() ? error.error_code : 0);
    
    if (error.hasError())
    {
        return std::make_shared<PipPythonPackageResponse>(
            std::move(error),
            /*data_version_=*/1);
    }

    /// Return success
    return std::make_shared<PipPythonPackageResponse>(
        /*replica_leader_=*/0,
        /*sn_=*/result.result,
        /*data_version_=*/1);
}

}
