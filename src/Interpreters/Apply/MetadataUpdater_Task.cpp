#include <Interpreters/Apply/MetadataUpdater.h>

#include <Cluster/MetaStore/MetaStore.h>
#include <Core/UUID.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/executeSelectQuery.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Task/TaskScheduler.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int OK;
extern const int UNKNOWN_DATABASE;
extern const int UNKNOWN_EXCEPTION;
extern const int INVALID_DATA;
extern const int METADATA_VERSION_CHANGED;
extern const int RESOURCE_ALREADY_EXISTS;
}

cluster::CallResultV<cluster::protocol::TaskDescriptorPtr>
MetadataUpdater::loadTaskDescriptor(const std::string & ns, const std::string & name) const
{
    cluster::CallResultV<cluster::protocol::TaskDescriptorPtr> task_desc;

    /// Ignore the result as it will be handled by caller
    auto _ = executeWithRetry([this, &task_desc, &ns, &name] {
        task_desc = meta_store->getMetaDB().getTask(ns, name);
        return task_desc.err.error_code;
    });

    return task_desc;
}

void MetadataUpdater::handleCreateTask(
    const cluster::CreateTaskRequest & request,
    const cluster::RequestHeader & request_header,
    uint64_t sn) const
{
    const auto & request_data = request.data();
    const auto & desc = request_data.desc;

    LOG_INFO(logger, "Start CreateTask, name={}", desc.name);
    Stopwatch stopwatch;
    SCOPE_EXIT({ LOG_INFO(logger, "End CreateTask, name={}, took={}ms", desc.name, stopwatch.elapsedMilliseconds()); });

    uint32_t next_version{1};

    auto task_desc = loadTaskDescriptor(desc.ns, desc.name);
    if (task_desc.hasError())
    {
        if (!task_desc.err.isNotFound())
        {
            LOG_ERROR(
                logger,
                "Failed to load task metadata for creating task {}, error_code={} error={}",
                desc.name,
                task_desc.err.error_code,
                task_desc.errorString());

            meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
            meta_store->ackProposal(
                request_header.correlationID(), sn, task_desc.err.error_code, std::move(task_desc.err.error_message));
            return;
        }

        /// If the alert does not exists, data_version should be 1
        if (desc.data_version != 1)
        {
            auto error
                = fmt::format("Failed to create task {} due to invalid data version: expected=1 got={}", desc.name, desc.data_version);

            LOG_ERROR(logger, "{}", error);

            meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
            meta_store->ackProposal(
                request_header.correlationID(), sn, ErrorCodes::METADATA_VERSION_CHANGED, std::move(error));

            return;
        }
    }
    else
    {
        if (request_data.exists_op == cluster::protocol::ExistsOperation::Throw)
        {
            meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
            meta_store->ackProposal(
                request_header.correlationID(),
                sn,
                ErrorCodes::RESOURCE_ALREADY_EXISTS,
                fmt::format("Failed to create task {}, already exists", desc.name));

            return;
        }

        if (request_data.exists_op == cluster::protocol::ExistsOperation::Ignore)
        {
            meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");
            return;
        }

        if (task_desc.result->id != desc.id)
        {
            auto error = fmt::format(
                "Failed to replace task {} due to ID conflict: expected={} actual={}",
                desc.name,
                toString(desc.id),
                toString(task_desc.result->id));

            LOG_ERROR(logger, "{}", error);

            meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
            meta_store->ackProposal(
                request_header.correlationID(), sn, ErrorCodes::METADATA_VERSION_CHANGED, std::move(error));

            return;
        }

        auto current_version = task_desc.result->data_version;
        if (current_version != desc.data_version)
        {
            auto error = fmt::format(
                "Failed to replace task {} due to version conflict: expected={} got={}", desc.name, desc.data_version, current_version);

            LOG_ERROR(logger, "{}", error);

            meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
            meta_store->ackProposal(
                request_header.correlationID(), sn, ErrorCodes::METADATA_VERSION_CHANGED, std::move(error));

            return;
        }

        next_version = current_version + 1;
    }


    auto updated_desc{desc};
    updated_desc.data_version = next_version;
    updated_desc.last_modify_timestamp_ms = request_data.requested_ts;
    updated_desc.last_modified_by = request_data.requested_by;

    if (updated_desc.data_version == 1)
    {
        updated_desc.create_timestamp_ms = request_data.requested_ts;
        updated_desc.created_by = request_data.requested_by;
    }
    else
    {
        updated_desc.create_timestamp_ms = task_desc.result->create_timestamp_ms;
        updated_desc.created_by = task_desc.result->created_by;
    }

    auto res = executeWithRetry(
        [this, &updated_desc, &sn] { return meta_store->getMetaDB().saveTask(updated_desc, cluster::AppliedSequence(sn)).error_code; });

    if (res != ErrorCodes::OK)
    {
        handleFailedRequest(request_header.correlationID(), sn, res, fmt::format("commit create task {}", updated_desc.name));
        return;
    }

    meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");

    if (auto ts = global_context->getTaskScheduler(); ts)
        ts->onTaskCreated(updated_desc);
}

void MetadataUpdater::handleDeleteTask(
    const cluster::DeleteTaskRequest & request,
    const cluster::RequestHeader & request_header,
    uint64_t sn) const
{
    const auto & request_data = request.data();

    LOG_INFO(logger, "Start DeleteTask, ns={} name={} id={}", request_data.ns, request_data.name, toString(request_data.id));

    Stopwatch stopwatch;
    SCOPE_EXIT({
        LOG_INFO(
            logger,
            "End DeleteTask, ns={} name={} id={} took={}ms",
            request_data.ns,
            request_data.name,
            toString(request_data.id),
            stopwatch.elapsedMilliseconds());
    });

    auto res = executeWithRetry([this, &request_data, &sn] {
        return meta_store->getMetaDB().deleteTask(request_data.ns, request_data.name, cluster::AppliedSequence(sn)).error_code;
    });

    if (res != ErrorCodes::OK)
    {
        handleFailedRequest(
            request_header.correlationID(),
            sn,
            res,
            fmt::format("Failed to commit delete task {}.{}", request_data.ns, request_data.name));
        return;
    }

    meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");

    if (auto ts = global_context->getTaskScheduler(); ts)
        ts->onTaskDeleted(request_data.id);
}

void MetadataUpdater::handleAlterTask(
    const cluster::AlterTaskRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const
{
    const auto & request_data = request.data();

    LOG_INFO(logger, "Start AlterTask: ns={} name={}", request_data.ns, request_data.name);
    Stopwatch stopwatch;
    SCOPE_EXIT({
        LOG_INFO(logger, "End AlterTask: ns={} name={} took={}ms", request_data.ns, request_data.name, stopwatch.elapsedMilliseconds());
    });

    auto maybe_task_desc = loadTaskDescriptor(request_data.ns, request_data.name);
    if (maybe_task_desc.hasError())
    {
        handleFailedRequest(
            request_header.correlationID(), sn, maybe_task_desc.err.error_code, std::move(maybe_task_desc.err.error_message));
        return;
    }

    const auto & task_desc = *maybe_task_desc.result;

    if (task_desc.id != request_data.id || task_desc.data_version != request_data.data_version)
    {
        handleFailedRequest(
            request_header.correlationID(), sn, ErrorCodes::METADATA_VERSION_CHANGED, "Task was changed before applying alter.");
        return;
    }

    auto updated_desc{task_desc};
    bool should_update = false;
    switch (request_data.type)
    {
        case cluster::protocol::AlterTaskRequestData::AlterType::STATUS:
            if (request_data.status != updated_desc.status)
            {
                updated_desc.status = request_data.status;
                should_update = true;
            }
            break;
    }

    if (should_update)
    {
        updated_desc.data_version = updated_desc.data_version + 1;
        updated_desc.last_modify_timestamp_ms = request_data.requested_ts;
        updated_desc.last_modified_by = request_data.requested_by;

        auto res = executeWithRetry(
            [this, &updated_desc, &sn] { return meta_store->getMetaDB().saveTask(updated_desc, cluster::AppliedSequence(sn)).error_code; });

        if (res != ErrorCodes::OK)
        {
            handleFailedRequest(request_header.correlationID(), sn, res, fmt::format("commit create task {}", updated_desc.name));
            return;
        }

        if (auto ts = global_context->getTaskScheduler(); ts)
            ts->onTaskCreated(updated_desc);
    }

    meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");
}
}
