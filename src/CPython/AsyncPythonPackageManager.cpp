#include <CPython/AsyncPythonPackageManager.h>
#include <CPython/PythonPackage.h>

#include <Interpreters/Context.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include <Common/logger_useful.h>

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
}

namespace ProfileEvents
{
extern const Event PythonPackagesInstalled;
}

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int RESOURCE_NOT_INITED;
}


namespace cpython
{

AsyncPythonPackageManager::AsyncPythonPackageManager(ContextPtr context)
    : global_context(context)
    , logger(getLogger("AsyncPythonPackageManager"))
    , thread_pool(std::make_unique<ThreadPool>(
          CurrentMetrics::LocalThread,
          CurrentMetrics::LocalThreadActive,
          /*max_threads=*/1,
          /*max_free_threads=*/1,
          /*queue_size=*/1024))
{
}

AsyncPythonPackageManager::~AsyncPythonPackageManager()
{
    shutdown();
}

void AsyncPythonPackageManager::shutdown()
{
    if (shutdown_requested.exchange(true))
        return;

    LOG_INFO(logger, "Stopping");

    if (thread_pool)
    {
        thread_pool->wait();
        thread_pool.reset();
    }

    LOG_INFO(logger, "Stopped");
}

cluster::CallResultV<String> AsyncPythonPackageManager::scheduleInstall(
    const String & task_id,
    const String & package_name,
    const String & package_version,
    const PipInstallOptions & install_options,
    AsyncTaskCallback callback)
{
    if (shutdown_requested.load())
        return cluster::CallResultV<String>(DB::ErrorCodes::RESOURCE_NOT_INITED, "AsyncPythonPackageManager is shutting down");

    /// Build original package specification for installation
    std::string original_spec = package_name;
    if (!package_version.empty())
    {
        /// Check if version already contains operators like >, <, =, ~, !
        if (package_version.find_first_of("><!=~") != std::string::npos)
            original_spec += package_version; /// Version spec like ">2.0", ">=1.0", etc.
        else
            original_spec += "==" + package_version; /// Simple version like "1.0.0"
    }

    auto task_result = std::make_shared<AsyncTaskResult>(task_id, package_name, "install", original_spec);
    task_result->_install_options = install_options;

    {
        std::unique_lock lock(results_mutex);
        if (task_results.find(task_id) != task_results.end())
            return cluster::CallResultV<String>(DB::ErrorCodes::LOGICAL_ERROR, fmt::format("Task ID {} already exists", task_id));

        task_results[task_id] = task_result;
    }

    try
    {
        thread_pool->scheduleOrThrowOnError([this, task_result, callback]() { install(task_result, callback); });

        LOG_DEBUG(
            logger,
            "Scheduled Python package install task: {} for package '{}'{}",
            task_id,
            package_name,
            package_version.empty() ? "" : ("==" + package_version));
    }
    catch (...)
    {
        /// Remove task from tracking if scheduling failed
        {
            std::unique_lock lock(results_mutex);
            task_results.erase(task_id);
        }
        return cluster::CallResultV<String>(DB::ErrorCodes::LOGICAL_ERROR, "Failed to schedule task");
    }

    return cluster::CallResultV<String>(task_id);
}

cluster::CallResultV<String> AsyncPythonPackageManager::scheduleInstallRequirements(
    const String & task_id, const String & requirements_text, const PipInstallOptions & install_options, AsyncTaskCallback callback)
{
    if (shutdown_requested.load())
        return cluster::CallResultV<String>(DB::ErrorCodes::RESOURCE_NOT_INITED, "AsyncPythonPackageManager is shutting down");

    auto task_result = std::make_shared<AsyncTaskResult>(task_id, "requirements.txt", "install", "requirements.txt");
    task_result->_requirements_text = requirements_text;
    task_result->_install_options = install_options;
    task_result->_install_from_requirements = true;

    {
        std::unique_lock lock(results_mutex);
        if (task_results.find(task_id) != task_results.end())
            return cluster::CallResultV<String>(DB::ErrorCodes::LOGICAL_ERROR, fmt::format("Task ID {} already exists", task_id));

        task_results[task_id] = task_result;
    }

    try
    {
        thread_pool->scheduleOrThrowOnError([this, task_result, callback]() { install(task_result, callback); });
        LOG_DEBUG(logger, "Scheduled Python requirements install task: {}", task_id);
    }
    catch (...)
    {
        {
            std::unique_lock lock(results_mutex);
            task_results.erase(task_id);
        }
        return cluster::CallResultV<String>(DB::ErrorCodes::LOGICAL_ERROR, "Failed to schedule task");
    }

    return cluster::CallResultV<String>(task_id);
}

bool AsyncPythonPackageManager::removeCompletedTask(const String & task_id)
{
    std::unique_lock lock(results_mutex);
    auto iter = task_results.find(task_id);
    if (iter == task_results.end() || !iter->second->isCompleted())
        return false;

    task_results.erase(iter);
    return true;
}

cluster::CallResultV<String>
AsyncPythonPackageManager::scheduleUninstall(const String & task_id, const String & package_name, AsyncTaskCallback callback)
{
    if (shutdown_requested.load())
        return cluster::CallResultV<String>(DB::ErrorCodes::RESOURCE_NOT_INITED, "AsyncPythonPackageManager is shutting down");

    auto task_result = std::make_shared<AsyncTaskResult>(task_id, package_name, "uninstall");

    {
        std::unique_lock lock(results_mutex);
        /// Check if task_id already exists
        if (task_results.find(task_id) != task_results.end())
            return cluster::CallResultV<String>(DB::ErrorCodes::LOGICAL_ERROR, fmt::format("Task ID {} already exists", task_id));

        task_results[task_id] = task_result;
    }

    try
    {
        thread_pool->scheduleOrThrowOnError([this, task_result, callback]() { uninstall(task_result, callback); });

        LOG_DEBUG(logger, "Scheduled Python package uninstall task: {} for package '{}'", task_id, package_name);
    }
    catch (...)
    {
        /// Remove task from tracking if scheduling failed
        {
            std::unique_lock lock(results_mutex);
            task_results.erase(task_id);
        }
        return cluster::CallResultV<String>(DB::ErrorCodes::LOGICAL_ERROR, "Failed to schedule task");
    }

    return cluster::CallResultV<String>(task_id);
}

void AsyncPythonPackageManager::install(AsyncTaskResultPtr task_result, AsyncTaskCallback callback)
{
    LOG_INFO(logger, "Starting Python package install task: {} for package '{}'", task_result->task_id, task_result->package_name);

    update(task_result, AsyncTaskStatus::Installing);

    try
    {
        PythonPackage package = task_result->_install_from_requirements
            ? PythonPackage::fromRequirementsText(task_result->_requirements_text, task_result->_install_options)
            : PythonPackage(task_result->_original_package_spec, task_result->_install_options);
        package.install(logger);

        /// Extract the actual installed version for metrics tracking
        if (!task_result->_install_from_requirements)
        {
            try
            {
                std::string actual_version = package.getActualInstalledVersion(logger);
                if (!actual_version.empty())
                {
                    task_result->installed_version = actual_version;
                    LOG_DEBUG(logger, "Captured installed version '{}' for package '{}'", actual_version, task_result->package_name);
                }
                else
                {
                    LOG_DEBUG(
                        logger,
                        "Could not determine actual installed version for package '{}' after successful install",
                        task_result->package_name);
                }
            }
            catch (...)
            {
                LOG_DEBUG(
                    logger,
                    "Exception while extracting actual version for package '{}' after successful install",
                    task_result->package_name);
            }
        }

        ProfileEvents::increment(ProfileEvents::PythonPackagesInstalled);
        update(task_result, AsyncTaskStatus::Completed, cluster::Error(), callback);
    }
    catch (const Exception & e)
    {
        update(task_result, AsyncTaskStatus::Failed, cluster::Error(e.code(), e.message()), callback);

        LOG_ERROR(
            logger,
            "Failed Python package install task: {} for package '{}': {} (error_code={})",
            task_result->task_id,
            task_result->package_name,
            e.message(),
            e.code());
    }
    catch (...)
    {
        update(task_result, AsyncTaskStatus::Failed, cluster::Error(ErrorCodes::LOGICAL_ERROR, "Unknown error"), callback);

        LOG_ERROR(
            logger,
            "Failed Python package install task: {} for package '{}': Unknown error",
            task_result->task_id,
            task_result->package_name);
    }
}

void AsyncPythonPackageManager::uninstall(AsyncTaskResultPtr task_result, AsyncTaskCallback callback)
{
    update(task_result, AsyncTaskStatus::Installing);

    try
    {
        auto package = PythonPackage(task_result->package_name);
        package.uninstall(logger);

        update(task_result, AsyncTaskStatus::Completed, cluster::Error(), callback);
    }
    catch (const Exception & e)
    {
        update(task_result, AsyncTaskStatus::Failed, cluster::Error(e.code(), e.message()), callback);

        LOG_ERROR(
            logger,
            "Failed Python package uninstall task: {} for package '{}': {} (error_code={})",
            task_result->task_id,
            task_result->package_name,
            e.message(),
            e.code());
    }
    catch (...)
    {
        update(task_result, AsyncTaskStatus::Failed, cluster::Error(ErrorCodes::LOGICAL_ERROR, "Unknown error"), callback);

        LOG_ERROR(
            logger,
            "Failed Python package uninstall task: {} for package '{}': Unknown error",
            task_result->task_id,
            task_result->package_name);
    }
}

void AsyncPythonPackageManager::update(
    AsyncTaskResultPtr task_result, AsyncTaskStatus status, cluster::Error error, AsyncTaskCallback callback)
{
    task_result->status = status;
    task_result->error = error;

    if (status == AsyncTaskStatus::Installing)
        task_result->started_at = std::chrono::system_clock::now();
    else if (status == AsyncTaskStatus::Completed || status == AsyncTaskStatus::Failed)
        task_result->completed_at = std::chrono::system_clock::now();

    /// Notify callback if provided
    if (callback)
    {
        try
        {
            callback(*task_result);
        }
        catch (...)
        {
            LOG_ERROR(logger, "Callback execution failed for task {}: {}", task_result->task_id, getCurrentExceptionMessage(false));
        }
    }
}

std::vector<AsyncTaskResultPtr> AsyncPythonPackageManager::getAllTaskResults() const
{
    std::shared_lock lock(results_mutex);
    std::vector<AsyncTaskResultPtr> results;
    results.reserve(task_results.size());

    for (const auto & [task_id, result] : task_results)
        results.push_back(result);

    return results;
}

std::unordered_map<String, AsyncPythonPackageManager::PackageMetrics> AsyncPythonPackageManager::getPackageMetrics() const
{
    std::shared_lock lock(results_mutex);
    std::unordered_map<String, PackageMetrics> package_metrics;

    for (const auto & [task_id, result] : task_results)
    {
        const auto & package_name = result->package_name;
        auto & metrics = package_metrics[package_name];

        switch (result->status)
        {
            case AsyncTaskStatus::Scheduled:
                ++metrics.scheduled_tasks;
                break;
            case AsyncTaskStatus::Installing:
                ++metrics.installing_tasks;
                break;
            case AsyncTaskStatus::Completed:
                if (result->operation == "install")
                {
                    ++metrics.successful_installs;

                    /// Track latest installed version
                    if (result->completed_at.time_since_epoch().count() > 0 && !result->installed_version.empty())
                    {
                        auto install_ts
                            = std::chrono::duration_cast<std::chrono::milliseconds>(result->completed_at.time_since_epoch()).count();
                        if (install_ts > metrics.latest_install_ts)
                        {
                            metrics.latest_installed_version = result->installed_version;
                            metrics.latest_install_ts = install_ts;
                        }
                    }
                }
                else if (result->operation == "uninstall")
                {
                    ++metrics.successful_uninstalls;
                }
                break;
            case AsyncTaskStatus::Failed:
                if (result->operation == "install")
                    ++metrics.failed_installs;
                else if (result->operation == "uninstall")
                    ++metrics.failed_uninstalls;

                if (result->error.hasError() && result->completed_at.time_since_epoch().count() > 0)
                {
                    auto error_ts = std::chrono::duration_cast<std::chrono::milliseconds>(result->completed_at.time_since_epoch()).count();
                    if (error_ts > metrics.last_error_ts)
                    {
                        metrics.last_error = result->error.error_message;
                        metrics.last_error_ts = error_ts;
                    }
                }
                break;
        }
    }

    return package_metrics;
}

}
}
