#include <Interpreters/Apply/MetadataUpdater.h>

#include <Cluster/MetaStore/MetaStore.h>

#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int OK;
extern const int UNKNOWN_EXCEPTION;
extern const int UNKNOWN_FUNCTION;
extern const int UNKNOWN_AGGREGATE_FUNCTION;
extern const int UDF_INVALID_NAME;
extern const int UDF_INTERNAL_ERROR;
extern const int FUNCTION_ALREADY_EXISTS;
extern const int CANNOT_DROP_FUNCTION;
extern const int RESOURCE_NOT_FOUND;
extern const int METADATA_VERSION_CHANGED;
}
}

namespace DB
{

void MetadataUpdater::handleCreateUserDefinedFunction(
    const cluster::CreateUserDefinedFunctionRequest & request,
    const cluster::RequestHeader & request_header,
    uint64_t sn) const
{
    const auto & request_data = request.data();
    const auto & request_desc = request_data.desc;

    LOG_INFO(logger, "Start CreateUserDefinedFunction with func_name={}", request_desc.name);
    Stopwatch stopwatch;
    SCOPE_EXIT({
        LOG_INFO(logger, "End CreateUserDefinedFunction with func_name={}, took={}ms", request_desc.name, stopwatch.elapsedMilliseconds());
    });

    uint32_t current_version{cluster::Nulls::NullVersion};
    uint32_t next_version{1};

    cluster::CallResultV<cluster::protocol::UserDefinedFunctionDescriptorPtr> udf_desc;
    auto load_res = executeWithRetry([this, &udf_desc, &request_desc] {
        udf_desc = meta_store->getMetaDB().getUserDefinedFunction(request_desc.name);
        return udf_desc.err.error_code;
    });

    if (load_res != ErrorCodes::OK)
    {
        if (!udf_desc.err.isNotFound())
        {
            LOG_ERROR(
                logger,
                "Failed to load user defined function descriptor when creating user defined function {} error_code={} error={}",
                request_desc.name,
                udf_desc.err.error_code,
                udf_desc.errorString());

            meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
            meta_store->ackProposal(
                request_header.correlationID(), sn, udf_desc.err.error_code, std::string{udf_desc.err.error_message});
            return;
        }
    }
    else
    {
        current_version = udf_desc.result->data_version;
    }

    if (current_version == cluster::Nulls::NullVersion)
    {
        if (request_desc.data_version != 1)
        {
            auto error = fmt::format(
                "Failed to create user defined function due to invalid data version: name={} expected_version=1 actual_version={}",
                request_desc.name,
                request_desc.data_version);
            LOG_ERROR(logger, "{}", error);

            meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
            meta_store->ackProposal(
                request_header.correlationID(), sn, DB::ErrorCodes::METADATA_VERSION_CHANGED, std::move(error));

            return;
        }
    }
    else if (request_data.exists_op == cluster::protocol::ExistsOperation::Throw)
    {
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(
            request_header.correlationID(),
            sn,
            DB::ErrorCodes::FUNCTION_ALREADY_EXISTS,
            fmt::format("User defined function {} already exists, version={}", request_desc.name, current_version));

        return;
    }
    else if (request_data.exists_op == cluster::protocol::ExistsOperation::Ignore)
    {
        meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");
    }
    else
    {
        if (current_version != request_desc.data_version)
        {
            auto error = fmt::format(
                "Failed to create user defined function {} due to version conflict: expected_version={} current_version={}",
                request_desc.name,
                request_desc.data_version,
                current_version);
            LOG_ERROR(logger, "{}", error);

            meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
            meta_store->ackProposal(
                request_header.correlationID(), sn, DB::ErrorCodes::METADATA_VERSION_CHANGED, std::move(error));

            return;
        }

        next_version = current_version + 1;
    }

    auto updated_desc{request_data.desc};
    updated_desc.data_version = next_version;

    /// Try to commit create udf to metadb until success or failed with unretriable error
    auto res = executeWithRetry([this, &updated_desc, &sn] {
        return meta_store->getMetaDB().saveUserDefinedFunction(updated_desc, cluster::AppliedSequence(sn)).error_code;
    });

    if (res != ErrorCodes::OK)
    {
        handleFailedRequest(
            request_header.correlationID(), sn, res, fmt::format("commit create user defined function {}", updated_desc.name));
        return;
    }

    meta_store->notifyUserDefinedFunctionCreated(updated_desc.name);
    meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");
}

void MetadataUpdater::handleDeleteUserDefinedFunction(
    const cluster::DeleteUserDefinedFunctionRequest & request,
    const cluster::RequestHeader & request_header,
    uint64_t sn) const
{
    const auto & request_data = request.data();

    LOG_INFO(logger, "Start DeleteUserDefinedFunction with func_name={}", request_data.func_name);
    Stopwatch stopwatch;
    SCOPE_EXIT({
        LOG_INFO(
            logger, "End DeleteUserDefinedFunction with func_name={}, took={}ms", request_data.func_name, stopwatch.elapsedMilliseconds());
    });

    /// Try to commit delete udf to metadb until success or failed with unretriable error
    auto res = executeWithRetry([this, &request_data, &sn] {
        return meta_store->getMetaDB().deleteUserDefinedFunction(request_data.func_name, cluster::AppliedSequence(sn)).error_code;
    });

    if (res != ErrorCodes::OK)
    {
        handleFailedRequest(
            request_header.correlationID(),
            sn,
            res,
            fmt::format("Failed to commit delete user defined function {}", request_data.func_name));
        return;
    }

    meta_store->notifyUserDefinedFunctionDeleted(request_data.func_name);
    meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");
}
}
