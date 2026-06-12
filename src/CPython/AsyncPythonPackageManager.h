#pragma once

#include <CPython/PythonPackage.h>

#include <Cluster/Common/CallResult.h>
#include <Interpreters/Context_fwd.h>
#include <Common/ThreadPool_fwd.h>

#include <magic_enum.hpp>

namespace DB::cpython
{

enum class AsyncTaskStatus : uint8_t
{
    Scheduled = 0, /// Task has been queued but not started
    Installing = 1, /// Task is currently executing
    Completed = 2, /// Task completed successfully
    Failed = 3 /// Task failed with error
};

struct AsyncTaskResult
{
    String task_id;
    AsyncTaskStatus status = AsyncTaskStatus::Scheduled;
    String package_name;
    String installed_version; /// Actual version installed by pip (e.g., "2.31.0"), empty if install failed
    String operation; /// "install" or "uninstall"
    cluster::Error error;
    std::chrono::system_clock::time_point created_at;
    std::chrono::system_clock::time_point started_at;
    std::chrono::system_clock::time_point completed_at;

    /// Internal fields for processing (not exposed in system table)
    String _original_package_spec; /// Original specification used for installation (e.g., "requests>2.0")
    String _requirements_text;
    PipInstallOptions _install_options;
    bool _install_from_requirements = false;

    AsyncTaskResult(const String & task_id_, const String & package_name_, const String & operation_, const String & original_spec_ = "")
        : task_id(task_id_)
        , status(AsyncTaskStatus::Scheduled)
        , package_name(package_name_)
        , operation(operation_)
        , created_at(std::chrono::system_clock::now())
        , _original_package_spec(original_spec_)
    {
    }

    bool isCompleted() const noexcept { return status == AsyncTaskStatus::Completed || status == AsyncTaskStatus::Failed; }
    bool isSuccess() const noexcept { return status == AsyncTaskStatus::Completed && !error.hasError(); }

    std::string statusString() const noexcept { return fmt::format("{}", magic_enum::enum_name(status)); }
};

using AsyncTaskResultPtr = std::shared_ptr<AsyncTaskResult>;

/// Callback function type for task completion
using AsyncTaskCallback = std::function<void(const AsyncTaskResult &)>;

class AsyncPythonPackageManager
{
public:
    AsyncPythonPackageManager(ContextPtr context);
    ~AsyncPythonPackageManager();

    cluster::CallResultV<String> scheduleInstall(
        const String & task_id,
        const String & package_name,
        const String & package_version = "",
        const PipInstallOptions & install_options = {},
        AsyncTaskCallback callback = nullptr);

    cluster::CallResultV<String> scheduleInstallRequirements(
        const String & task_id,
        const String & requirements_text,
        const PipInstallOptions & install_options = {},
        AsyncTaskCallback callback = nullptr);

    cluster::CallResultV<String>
    scheduleUninstall(const String & task_id, const String & package_name, AsyncTaskCallback callback = nullptr);

    std::vector<AsyncTaskResultPtr> getAllTaskResults() const;

    /// Remove a finished task from tracking; returns false if the task is unknown or still running.
    /// Periodic schedulers (e.g. the S3 requirements reconciler) use this to keep retried attempts
    /// from accumulating in the results map for the lifetime of the process.
    bool removeCompletedTask(const String & task_id);

    /// Get task metrics for IntrospectionStateLog collection
    struct PackageMetrics
    {
        size_t scheduled_tasks = 0;
        size_t installing_tasks = 0;
        size_t successful_installs = 0;
        size_t failed_installs = 0;
        size_t successful_uninstalls = 0;
        size_t failed_uninstalls = 0;
        String last_error;
        UInt64 last_error_ts = 0;
        String latest_installed_version; /// Latest successfully installed version
        UInt64 latest_install_ts = 0; /// Timestamp of latest successful install
    };
    std::unordered_map<String, PackageMetrics> getPackageMetrics() const;

    /// Shutdown the manager and wait for all tasks to complete
    void shutdown();

private:
    void install(AsyncTaskResultPtr task_result, AsyncTaskCallback callback);
    void uninstall(AsyncTaskResultPtr task_result, AsyncTaskCallback callback);

    /// Update task status and notify callback
    void update(
        AsyncTaskResultPtr task_result,
        AsyncTaskStatus status,
        cluster::Error error = cluster::Error(),
        AsyncTaskCallback callback = nullptr);

    std::atomic<bool> shutdown_requested{false};

    std::unique_ptr<ThreadPool> thread_pool;

    /// Task results storage (task_id -> result)
    mutable DB::SharedMutex results_mutex;
    std::unordered_map<String, AsyncTaskResultPtr> task_results;

    ContextPtr global_context;

    LoggerPtr logger;
};

using AsyncPythonPackageManagerPtr = std::shared_ptr<AsyncPythonPackageManager>;

}
