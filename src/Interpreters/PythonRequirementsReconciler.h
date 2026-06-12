#pragma once

#include "config.h"

#if USE_PYTHON_UDF && USE_AWS_S3

#include <Interpreters/Context_fwd.h>
#include <Common/Logger.h>
#include <Common/ThreadPool.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <condition_variable>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace DB
{

namespace S3
{
class Client;
}

/// Reconciles the locally installed Python UDF packages against a requirements.txt hosted on S3.
///
/// When the `python_requirements` config section is present, each node fetches the requirements
/// file on startup — and then every `poll_interval_sec` to pick up updates — and schedules
/// `pip install -r` through AsyncPythonPackageManager unless the exact same content was already
/// applied (tracked via a marker file in PYTHONUSERBASE holding the content digest).
///
/// Runs on every node type. On ephemeral compute nodes (no persistent volume) it restores the
/// packages lost on each pod reschedule; on data nodes with persistent volumes both
/// site-packages and the marker survive restarts, so the startup pass is a cheap digest check
/// and polling is what picks up file updates. The reconcile is additive-only: packages removed
/// from the file are never uninstalled, which on persistent nodes means they linger until
/// removed manually (SYSTEM UNINSTALL PYTHON PACKAGE).
///
/// Fail-open: fetch/install failures never block server startup; they are retried in a
/// background thread with capped exponential backoff. The reconciler is node-local on purpose —
/// every node converges against the same declarative source, so no cluster coordination is needed.
class PythonRequirementsReconciler
{
public:
    struct Config
    {
        std::string url; /// S3 object URL of the requirements file; presence enables the reconciler
        std::string index_url;
        std::vector<std::string> extra_index_urls;
        /// How often to re-fetch the file and pick up changes after a successful reconcile.
        /// 0 disables polling (reconcile on startup only).
        UInt64 poll_interval_sec = DEFAULT_POLL_INTERVAL_SEC;
    };

    static constexpr auto CONFIG_SECTION = "python_requirements";
    static constexpr auto MARKER_FILE_NAME = ".s3_requirements_applied";
    static constexpr size_t MAX_REQUIREMENTS_BYTES = 1 << 20;
    static constexpr UInt64 DEFAULT_POLL_INTERVAL_SEC = 300;
    static constexpr UInt64 MIN_POLL_INTERVAL_SEC = 5;

    /// Returns nullopt when the `python_requirements` section is absent or has an empty url.
    static std::optional<Config> loadConfig(const Poco::Util::AbstractConfiguration & config);

    static std::string contentDigest(const std::string & requirements_text);
    static std::string markerFilePath(const std::string & python_user_base);
    /// Returns the digest recorded by the last successful reconcile, or empty if none.
    static std::string readMarker(const std::string & python_user_base);
    static void writeMarker(const std::string & python_user_base, const std::string & digest);

    /// Source of the requirements content; the default fetches from S3 using `config.url`.
    /// Injectable so the reconcile logic can be unit-tested without an S3 endpoint.
    using Fetcher = std::function<std::string()>;

    PythonRequirementsReconciler(Config config_, ContextPtr global_context_, Fetcher fetcher_ = {});
    ~PythonRequirementsReconciler();

    PythonRequirementsReconciler(const PythonRequirementsReconciler &) = delete;
    PythonRequirementsReconciler & operator=(const PythonRequirementsReconciler &) = delete;

    /// Spawns the background reconcile thread; never blocks startup.
    void start();
    void stop();

    /// One reconcile attempt. Returns true when reconciliation is finished (requirements applied
    /// or already up to date), false when it should be retried. Throws on fetch/validation errors.
    /// Public for unit tests; production code drives it via start().
    bool reconcileOnce(const std::string & python_user_base, size_t attempt);

private:
    void run();
    std::string fetchRequirementsText();
    bool
    installAndWait(const std::string & requirements_text, const std::string & digest, const std::string & python_user_base, size_t attempt);

    bool isStopRequested();
    /// Returns true if stop was requested while waiting.
    bool waitForStopOrTimeout(UInt64 seconds);

    const Config config;
    ContextPtr global_context;
    Fetcher fetcher;
    LoggerPtr logger;

    /// Reused across poll cycles: ClientFactory::create() builds a fresh client and credentials
    /// provider chain on every call (no caching in the factory), and re-resolving credentials
    /// each cycle is wasteful (e.g. STS round-trips under IRSA). The AWS providers refresh
    /// expiring credentials internally, so a long-lived client is the intended usage.
    /// Reset on fetch failure to recover from a wedged client. Reconcile-thread only.
    std::shared_ptr<const S3::Client> s3_client;

    /// Task id of the previous install attempt; removed from the package manager before the next
    /// attempt is scheduled so retries do not accumulate in its results map. Reconcile-thread only.
    std::string last_install_task_id;

    std::mutex mutex;
    std::condition_variable stop_cv;
    bool stop_requested = false;
    std::optional<ThreadFromGlobalPool> thread;
};

}

#endif
