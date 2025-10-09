#include <Interpreters/Apply/MetadataUpdater.h>

#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/PipPythonPackageRequest.h>
#include <Interpreters/Context.h>
#include <Common/Stopwatch.h>

#if USE_PYTHON_UDF
#include <CPython/AsyncPythonPackageManager.h>
#include <CPython/PythonPackage.h>
#include <Interpreters/EnsurePython.h>
#endif

namespace DB
{

namespace ErrorCodes
{
extern const int OK;
extern const int BAD_ARGUMENTS;
extern const int UNKNOWN_EXCEPTION;
extern const int UNSUPPORTED;
}

void MetadataUpdater::handlePipPythonPackage(
    const cluster::PipPythonPackageRequest & request,
    const cluster::RequestHeader & request_header,
    uint64_t sn) const
{
    const auto & data = request.data();

    LOG_INFO(logger, "Start PipPythonPackage, {}", data.doString());
    Stopwatch stopwatch;
    SCOPE_EXIT({ LOG_INFO(logger, "End PipPythonPackage, {} took={}ms", data.doString(), stopwatch.elapsedMilliseconds()); });

    try
    {
#if USE_PYTHON_UDF
        switch (data.operation)
        {
            case cluster::protocol::PipPythonPackageRequestData::Install:
            {
                if (data.package_names.empty())
                {
                    std::string error_msg = "No package specified for installation";
                    LOG_ERROR(logger, "Failed to install Python package {}: {}", data.doString(), error_msg);
                    meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
                    meta_store->ackProposal(
                        request_header.correlationID(), sn, DB::ErrorCodes::BAD_ARGUMENTS, std::move(error_msg));
                    return;
                }

                /// Handle single package installation
                const std::string & package_name = data.package_names[0];
                std::string package_version = data.package_versions.empty() ? "" : data.package_versions[0];

                try
                {
                    std::string package_with_version = package_name;
                    if (!package_version.empty())
                    {
                        /// Check if version already contains operators like >, <, =, ~, !
                        if (package_version.find_first_of("><!=~") != std::string::npos)
                            package_with_version += package_version; /// Version spec like ">2.0", ">=1.0", etc.
                        else
                            package_with_version += "==" + package_version; /// Simple version like "1.0.0"
                    }

                    auto package = cpython::PythonPackage(package_with_version);

                    LOG_DEBUG(
                        logger,
                        "Validating Python package for async installation: {}{}",
                        package_name,
                        package_version.empty() ? "" : fmt::format("=={}", package_version));

                    /// Fast validation: check if package exists and can be installed (dry-run)
                    /// This is quick and catches most issues immediately
                    package.validateInstall(logger);

                    LOG_DEBUG(
                        logger,
                        "Package validation successful, scheduling async installation: {}{}",
                        package_name,
                        package_version.empty() ? "" : fmt::format("=={}", package_version));

                    /// Acknowledge raft proposal immediately after validation
                    meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
                    meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");

                    /// Build proper version display string for logging
                    std::string version_display;
                    if (!package_version.empty())
                    {
                        if (package_version.find_first_of("><!=~") != std::string::npos)
                            version_display = package_version; /// Already has operators like ">2.0"
                        else
                            version_display = "==" + package_version; /// Simple version like "1.0.0"
                    }

                    /// Schedule actual installation asynchronously
                    /// This won't block other DDL operations
                    auto async_manager = global_context->getAsyncPythonPackageManager();
                    if (async_manager)
                    {
                        auto result_ = async_manager->scheduleInstall(
                            data.task_id,
                            package_name,
                            package_version,
                            [package_name, version_display, this](const cpython::AsyncTaskResult & result) {
                                if (result.isSuccess())
                                {
                                    LOG_INFO(
                                        logger,
                                        "Async Python package installation completed successfully: {}{} (task_id={})",
                                        package_name,
                                        version_display,
                                        result.task_id);
                                }
                                else
                                {
                                    LOG_ERROR(
                                        logger,
                                        "Async Python package installation failed: {}{} (task_id={}, error={})",
                                        package_name,
                                        version_display,
                                        result.task_id,
                                        result.error.string());
                                }
                            });

                        if (result_.hasError())
                        {
                            LOG_ERROR(logger, "Failed to schedule Python package installation: {}", result_.errorString());
                        }
                        else
                        {
                            LOG_INFO(
                                logger,
                                "Scheduled async Python package installation: {}{} (task_id={})",
                                package_name,
                                version_display,
                                result_.result);
                        }
                    }
                    else
                    {
                        LOG_WARNING(logger, "AsyncPythonPackageManager not available, falling back to sync installation");
                        /// Fallback to synchronous installation if async manager is not available
                        package.install(logger);
                    }
                }
                catch (const DB::Exception & e)
                {
                    std::string error_msg
                        = fmt::format("Failed to validate Python package for installation {}: {}", package_name, e.message());
                    meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
                    meta_store->ackProposal(request_header.correlationID(), sn, e.code(), std::move(error_msg));
                }
                catch (...)
                {
                    std::string error_msg
                        = fmt::format("Failed to validate Python package for installation {}: Unknown exception", package_name);
                    meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
                    meta_store->ackProposal(
                        request_header.correlationID(), sn, ErrorCodes::UNKNOWN_EXCEPTION, std::move(error_msg));
                }

                break;
            }
            case cluster::protocol::PipPythonPackageRequestData::Uninstall:
            {
                if (data.package_names.empty())
                {
                    std::string error_msg = "No package specified for uninstallation";
                    LOG_ERROR(logger, "Failed to uninstall Python package {}: {}", data.doString(), error_msg);
                    meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
                    meta_store->ackProposal(
                        request_header.correlationID(), sn, DB::ErrorCodes::BAD_ARGUMENTS, std::move(error_msg));
                    return;
                }

                /// Handle single package uninstallation
                const std::string & package_name = data.package_names[0];

                try
                {
                    auto package = cpython::PythonPackage(package_name);

                    LOG_DEBUG(logger, "Validating Python package for async uninstallation: {}", package_name);

                    /// Quick validation: check if package is currently installed (this is fast)
                    /// For uninstall, we only need to check current installation status
                    package.validateUninstall(logger);

                    LOG_DEBUG(logger, "Package validation successful, scheduling async uninstallation: {}", package_name);

                    /// Acknowledge raft proposal immediately after validation
                    meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
                    meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");

                    /// Schedule actual uninstallation asynchronously
                    /// This won't block other DDL operations
                    auto async_manager = global_context->getAsyncPythonPackageManager();
                    if (async_manager)
                    {
                        auto result_ = async_manager->scheduleUninstall(
                            data.task_id, package_name, [package_name, this](const cpython::AsyncTaskResult & result) {
                                if (result.isSuccess())
                                {
                                    LOG_INFO(
                                        logger,
                                        "Async Python package uninstallation completed successfully: {} (task_id={})",
                                        package_name,
                                        result.task_id);
                                }
                                else
                                {
                                    LOG_ERROR(
                                        logger,
                                        "Async Python package uninstallation failed: {} (task_id={}, error={})",
                                        package_name,
                                        result.task_id,
                                        result.error.string());
                                }
                            });

                        if (result_.hasError())
                        {
                            LOG_ERROR(logger, "Failed to schedule Python package uninstallation: {}", result_.errorString());
                        }
                        else
                        {
                            LOG_INFO(
                                logger, "Scheduled async Python package uninstallation: {} (task_id={})", package_name, result_.result);
                        }
                    }
                    else
                    {
                        LOG_WARNING(logger, "AsyncPythonPackageManager not available, falling back to sync uninstallation");
                        /// Fallback to synchronous uninstallation if async manager is not available
                        package.uninstall(logger);
                    }
                }
                catch (const DB::Exception & e)
                {
                    std::string error_msg
                        = fmt::format("Failed to validate Python package uninstallation status for {}: {}", package_name, e.message());
                    meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
                    meta_store->ackProposal(request_header.correlationID(), sn, e.code(), std::move(error_msg));
                }
                catch (...)
                {
                    std::string error_msg
                        = fmt::format("Failed to validate Python package uninstallation status for {}: Unknown exception", package_name);
                    meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
                    meta_store->ackProposal(
                        request_header.correlationID(), sn, ErrorCodes::UNKNOWN_EXCEPTION, std::move(error_msg));
                }

                break;
            }
            case cluster::protocol::PipPythonPackageRequestData::List:
            {
                /// LIST operation is handled by the REST API directly via executeSelectQuery
                /// This shouldn't normally reach here
                LOG_INFO(logger, "LIST operation handled in Python package management");
                meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
                meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");
                break;
            }
        }
#else
        std::string error_msg = "Python UDF support is not enabled";
        LOG_ERROR(logger, "Failed to process Python package operation {}: {}", data.doString(), error_msg);
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::UNSUPPORTED, std::move(error_msg));
#endif
    }
    catch (const DB::Exception & e)
    {
        std::string error_msg = fmt::format("Database exception while processing Python package operation: {}", e.message());
        LOG_ERROR(logger, "Failed to process Python package operation {}: {}", data.doString(), error_msg);
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(request_header.correlationID(), sn, e.code(), std::move(error_msg));
    }
    catch (...)
    {
        std::string error_msg = "Unknown exception occurred while processing Python package operation";
        tryLogCurrentException(logger, fmt::format("Failed to process Python package operation {}", data.doString()));
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::UNKNOWN_EXCEPTION, std::move(error_msg));
    }
}

}
