#include <Interpreters/Apply/MetadataUpdater.h>

#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Protocol/ExistsOperation.h>
#include <Core/UUID.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/executeSelectQuery.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Storages/Alert/StorageAlert.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <base/sleep.h>

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

namespace
{
void deleteCheckpoint(const DB::UUID & id, ContextMutablePtr context, LoggerPtr logger)
{
    auto uuid = toString(id);
    executeNonInsertQuery(
        fmt::format("DELETE FROM system.alert_ckpt_log WHERE uuid='{}'", uuid), std::move(context), [logger, &uuid](Block &&) {
            LOG_INFO(logger, "Checkpoint deleted for alert {}", uuid);
        });
}

std::optional<std::pair</*error_code*/ const int, /*msg*/ std::string>>
shutdownAlert(const std::string & ns, const std::string & name, const DB::UUID & id, ContextMutablePtr context, LoggerPtr logger)
{
    try
    {
        auto db = DatabaseCatalog::instance().tryGetDatabase(ns);
        if (!db)
            return {{ErrorCodes::UNKNOWN_DATABASE, fmt::format("Database {} does not exist", ns)}};

        auto alert = db->tryGetTable(name, context);
        if (alert)
        {
            if (alert->getStorageID().uuid != id)
                return {
                    {ErrorCodes::UNKNOWN_EXCEPTION,
                     fmt::format("Alert UUID did not match, expected={} actual={}", toString(id), toString(alert->getStorageID().uuid))}};

            LOG_INFO(logger, "Shutting down alert {}.{}", ns, name);
            alert->shutdown(true);
            db->detachTable(context, name);
        }
        else
        {
            /// if the alert was not found, it's fine, we want to shut it down anyways.
            LOG_INFO(logger, "Skipped shutdown, alert {}.{} it did not exist", ns, name);
        }

        deleteCheckpoint(id, context, logger);
    }
    catch (const Exception & e)
    {
        return {{e.code(), e.message()}};
    }
    catch (...)
    {
        auto ex_msg = getCurrentExceptionMessage(true);
        return {{DB::ErrorCodes::UNKNOWN_EXCEPTION, fmt::format("Failed to shut down alert {}.{}, error={}", ns, name, ex_msg)}};
    }

    return std::nullopt;
}
}

void MetadataUpdater::handleCreateAlert(
    const cluster::CreateAlertRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const
{
    const auto & request_data = request.data();
    const auto & desc = request_data.desc;

    LOG_INFO(logger, "Start CreateAlert, ns={} name={}", desc.ns, desc.name);
    Stopwatch stopwatch;
    SCOPE_EXIT({ LOG_INFO(logger, "End CreateAlert, ns={} name={}, took={}ms", desc.ns, desc.name, stopwatch.elapsedMilliseconds()); });

    auto db = DatabaseCatalog::instance().tryGetDatabase(desc.ns);
    /// Even this has already been checked in the Interpreter, but we want to double confirm if the DB is still there
    if (!db)
    {
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(
            request_header.correlationID(), sn, ErrorCodes::UNKNOWN_DATABASE, fmt::format("Database {} does not exist", desc.ns));
        return;
    }

    uint32_t next_version{1};

    auto alert_desc = loadAlertDescriptor(desc.ns, desc.name);
    if (alert_desc.hasError())
    {
        if (!alert_desc.err.isNotFound())
        {
            LOG_ERROR(
                logger,
                "Failed to load alert metadata for creating alert {}.{}, error_code={} error={}",
                desc.ns,
                desc.name,
                alert_desc.err.error_code,
                alert_desc.errorString());

            meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
            meta_store->ackProposal(request_header.correlationID(), sn, alert_desc.err.error_code, std::move(alert_desc.err.error_message));
            return;
        }

        /// If the alert does not exists, data_version should be 1
        if (desc.data_version != 1)
        {
            auto error = fmt::format(
                "Failed to create alert {}.{} due to invalid data version: expected=1 got={}", desc.ns, desc.name, desc.data_version);

            LOG_ERROR(logger, "{}", error);

            meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
            meta_store->ackProposal(request_header.correlationID(), sn, ErrorCodes::METADATA_VERSION_CHANGED, std::move(error));

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
                fmt::format("Failed to create alert {}.{}, already exists", desc.ns, desc.name));

            return;
        }

        if (request_data.exists_op == cluster::protocol::ExistsOperation::Ignore)
        {
            meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");
            return;
        }

        if (alert_desc.result->id != desc.id)
        {
            auto error = fmt::format(
                "Failed to replace alert {}.{} due to ID conflict: expected={} actual={}",
                desc.ns,
                desc.name,
                toString(desc.id),
                toString(alert_desc.result->id));

            LOG_ERROR(logger, "{}", error);

            meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
            meta_store->ackProposal(request_header.correlationID(), sn, ErrorCodes::METADATA_VERSION_CHANGED, std::move(error));

            return;
        }

        auto current_version = alert_desc.result->data_version;
        if (current_version != desc.data_version)
        {
            auto error = fmt::format(
                "Failed to replace alert {}.{} due to version conflict: expected={} got={}",
                desc.ns,
                desc.name,
                desc.data_version,
                current_version);

            LOG_ERROR(logger, "{}", error);

            meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
            meta_store->ackProposal(request_header.correlationID(), sn, ErrorCodes::METADATA_VERSION_CHANGED, std::move(error));

            return;
        }

        next_version = current_version + 1;
    }


    auto updated_desc{desc};
    updated_desc.data_version = next_version;
    updated_desc.last_modify_timestamp_ms = request_data.requested_ts;

    if (updated_desc.data_version == 1)
    {
        updated_desc.create_timestamp_ms = request_data.requested_ts;
        updated_desc.created_by = request_data.requested_by;
    }
    else
    {
        updated_desc.create_timestamp_ms = alert_desc.result->create_timestamp_ms;
        updated_desc.created_by = alert_desc.result->created_by;
        updated_desc.last_modified_by = request_data.requested_by;
    }

    /// std::function<std::pair<int, String>(std::function<void()> fn)>
    auto captureException = [&](String operation, std::function<void()> fn) {
        try
        {
            fn();
        }
        catch (const Exception & e)
        {
            LOG_ERROR(logger, "Failed to {}, error_code={}, error={}", operation, e.code(), e.message());
            meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
            meta_store->ackProposal(request_header.correlationID(), sn, e.code(), std::string{e.message()});
            return e.code();
        }
        catch (...)
        {
            auto err_msg = getCurrentExceptionMessageAndPattern(true).text;
            LOG_ERROR(logger, "Failed to {}, error={}", operation, err_msg);
            meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
            meta_store->ackProposal(
                request_header.correlationID(),
                sn,
                ErrorCodes::UNKNOWN_EXCEPTION,
                /*error_message=*/std::move(err_msg));
            return ErrorCodes::UNKNOWN_EXCEPTION;
        }

        return ErrorCodes::OK;
    };

    auto local_context = Context::createCopy(global_context);

    StoragePtr storage;
    auto err_code = captureException("create alert", [&updated_desc, &storage, &local_context]() {
        StorageID storage_id{updated_desc.ns, updated_desc.name, updated_desc.id};
        /// Create the storage before calling saveAlert to make sure updated_desc is valid.
        /// As InterpreterCreateAlert has already done a FULL validation, we just do a quick one
        /// here to avoid metadata operation timeout.
        storage = std::make_shared<StorageAlert>(
            std::move(storage_id),
            std::make_shared<cluster::protocol::AlertDescriptor>(updated_desc),
            StorageAlert::ValidationLevel::Quick,
            local_context);
    });

    if (err_code != ErrorCodes::OK)
        return;

    /// This is a replace operation, need to shutdown the existing alert first
    if (updated_desc.data_version > 1)
    {
        {
            auto err = shutdownAlert(updated_desc.ns, updated_desc.name, updated_desc.id, local_context, logger);
            if (err)
            {
                meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
                meta_store->ackProposal(request_header.correlationID(), sn, err->first, std::string{err->second});
                return;
            }
        }
    }

    auto res = executeWithRetry(
        [this, &updated_desc, &sn]() { return meta_store->getMetaDB().saveAlert(updated_desc, cluster::AppliedSequence(sn)).error_code; });
    if (res != DB::ErrorCodes::OK)
    {
        handleFailedRequest(
            request_header.correlationID(),
            sn,
            res,
            fmt::format("Failed to commit created alert definition: {{{}}}", updated_desc.string()));
        return;
    }

    {
        err_code = captureException("start alert", [&]() {
            LOG_INFO(logger, "Starting alert {} in database {}", updated_desc.name, updated_desc.ns);
            storage->startup();
            db->attachTable(local_context, updated_desc.name, storage, "alerts");
        });

        if (err_code != ErrorCodes::OK)
        {
            try
            {
                if (auto err = meta_store->getMetaDB().deleteAlert(updated_desc.ns, updated_desc.name, cluster::AppliedSequence(sn));
                    err.hasError())
                    LOG_ERROR(
                        logger,
                        "Failed to rollback alert {} in database {}, error={}",
                        updated_desc.name,
                        updated_desc.ns,
                        err.error_message);
            }
            catch (...)
            {
                tryLogCurrentException(
                    logger, fmt::format("Failed to rollback alert {} in database {}", updated_desc.name, updated_desc.ns));
            }
            return;
        }
    }

    meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");
}

void MetadataUpdater::handleDeleteAlert(
    const cluster::DeleteAlertRequest & request, const cluster::RequestHeader & request_header, uint64_t sn) const
{
    const auto & request_data = request.data();

    LOG_INFO(logger, "Start DeleteAlert, ns={} name={} id={}", request_data.ns, request_data.name, request_data.id);

    Stopwatch stopwatch;
    SCOPE_EXIT({
        LOG_INFO(
            logger,
            "End DeleteAlert, ns={} name={} id={} took={}ms",
            request_data.ns,
            request_data.name,
            request_data.id,
            stopwatch.elapsedMilliseconds());
    });

    {
        auto local_context = Context::createCopy(global_context);
        {
            auto err = shutdownAlert(request_data.ns, request_data.name, request_data.id, local_context, logger);
            if (err)
            {
                LOG_ERROR(logger, "Failed to delete alert ns={} name={}, error={}", request_data.ns, request_data.name, err->second);

                meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
                meta_store->ackProposal(request_header.correlationID(), sn, err->first, std::move(err->second));
                return;
            }
        }
    }

    int retries = 0;
    while (true)
    {
        try
        {
            /// Commit to meta db, we will need rollback, no way ? retry
            if (auto err = meta_store->getMetaDB().deleteAlert(request_data.ns, request_data.name, cluster::AppliedSequence(sn));
                !err.hasError())
            {
                break;
            }
            else
            {
                LOG_WARNING(
                    logger,
                    "Failed to commit delete alert, ns={} name={}, retried {} times, {}",
                    request_data.ns,
                    request_data.name,
                    retries++,
                    err.string());
                sleepForMilliseconds(500);
            }
        }
        catch (...)
        {
            LOG_WARNING(
                logger, "Failed to commit delete alert, ns={} name={}, retried {} times", request_data.ns, request_data.name, retries++);
            sleepForMilliseconds(500);
        }
    }

    meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");
}

cluster::CallResultV<cluster::protocol::AlertDescriptorPtr>
MetadataUpdater::loadAlertDescriptor(const std::string & ns, const std::string & name) const
{
    int retries = 0;
    while (true)
    {
        try
        {
            /// moving a temporary object prevents copy elision, use copy assignment here.
            auto alert_desc = meta_store->getMetaDB().getAlert(ns, name);

            if (!alert_desc.hasError() || isPermanentError(alert_desc.err.error_code))
            {
                return alert_desc;
            }
            else
            {
                LOG_ERROR(
                    logger,
                    "Failed to load descriptor for alert {} in {}, error={} retried {} times",
                    name,
                    ns,
                    alert_desc.err.error_message,
                    retries++);

                sleepForMilliseconds(500);
            }
        }
        catch (const Exception & e)
        {
            if (isPermanentError(e.code()))
            {
                return cluster::CallResultV<cluster::protocol::AlertDescriptorPtr>{e.code(), std::string{e.message()}};
            }
            else
            {
                LOG_ERROR(
                    logger, "Failed to load descriptor for alert {} in {}, error={} retried {} times", name, ns, e.message(), retries++);

                sleepForMilliseconds(500);
            }
        }
        catch (...)
        {
            LOG_ERROR(logger, "Failed to load alert description when updating alert {} of type {} retried {} times", name, name, retries++);

            sleepForMilliseconds(500);
        }
    }

    UNREACHABLE();
}
}
