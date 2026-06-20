#include <Interpreters/PythonRequirementsReconciler.h>

#if USE_PYTHON_UDF && USE_AWS_S3

#include <CPython/AsyncPythonPackageManager.h>
#include <CPython/PythonPackage.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadHelpers.h>
#include <IO/S3/Client.h>
#include <IO/S3/URI.h>
#include <IO/S3Common.h>
#include <Interpreters/Context.h>
#include <Storages/StorageS3Settings.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

#include <Python.h>

#include <aws/core/client/DefaultRetryStrategy.h>

#include <filesystem>
#include <fstream>
#include <future>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CANNOT_OPEN_FILE;
extern const int RESOURCE_NOT_INITED;
}

namespace
{
constexpr UInt64 RETRY_INITIAL_BACKOFF_SEC = 30;
constexpr UInt64 RETRY_MAX_BACKOFF_SEC = 300;
}

std::optional<PythonRequirementsReconciler::Config>
PythonRequirementsReconciler::loadConfig(const Poco::Util::AbstractConfiguration & config)
{
    const auto url_key = fmt::format("{}.url", CONFIG_SECTION);
    if (!config.has(url_key))
        return std::nullopt;

    Config result;
    result.url = config.getString(url_key);
    if (result.url.empty())
        return std::nullopt;

    result.index_url = config.getString(fmt::format("{}.index_url", CONFIG_SECTION), "");

    result.poll_interval_sec = config.getUInt64(fmt::format("{}.poll_interval_sec", CONFIG_SECTION), DEFAULT_POLL_INTERVAL_SEC);
    if (result.poll_interval_sec > 0)
        result.poll_interval_sec = std::max(result.poll_interval_sec, MIN_POLL_INTERVAL_SEC);

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(CONFIG_SECTION, keys);
    for (const auto & key : keys)
    {
        /// Repeated elements are enumerated as `extra_index_url`, `extra_index_url[1]`, ...
        if (key == "extra_index_url" || key.starts_with("extra_index_url["))
            result.extra_index_urls.emplace_back(config.getString(fmt::format("{}.{}", CONFIG_SECTION, key)));
    }

    return result;
}

std::string PythonRequirementsReconciler::contentDigest(const std::string & requirements_text)
{
    return sipHash128String(requirements_text);
}

std::string PythonRequirementsReconciler::markerFilePath(const std::string & python_user_base)
{
    return (std::filesystem::path(python_user_base) / MARKER_FILE_NAME).string();
}

std::string PythonRequirementsReconciler::readMarker(const std::string & python_user_base)
{
    std::ifstream in(markerFilePath(python_user_base));
    if (!in)
        return {};

    std::string digest;
    std::getline(in, digest);
    return digest;
}

void PythonRequirementsReconciler::writeMarker(const std::string & python_user_base, const std::string & digest)
{
    namespace fs = std::filesystem;

    fs::create_directories(python_user_base);

    const auto marker_path = markerFilePath(python_user_base);
    const auto tmp_path = marker_path + ".tmp";
    {
        std::ofstream out(tmp_path, std::ios::out | std::ios::trunc);
        out << digest << '\n';
        if (!out)
            throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Cannot write Python requirements marker file '{}'", tmp_path);
    }
    fs::rename(tmp_path, marker_path);
}

PythonRequirementsReconciler::PythonRequirementsReconciler(Config config_, ContextPtr global_context_, Fetcher fetcher_)
    : config(std::move(config_))
    , global_context(std::move(global_context_))
    , fetcher(std::move(fetcher_))
    , logger(getLogger("PythonRequirementsReconciler"))
{
}

PythonRequirementsReconciler::~PythonRequirementsReconciler()
{
    stop();
}

void PythonRequirementsReconciler::start()
{
    try
    {
        thread.emplace([this] { run(); });
        LOG_INFO(logger, "Started Python requirements reconciler for '{}'", config.url);
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Failed to start Python requirements reconciler");
    }
}

void PythonRequirementsReconciler::stop()
{
    {
        std::lock_guard lock(mutex);
        if (stop_requested)
            return;
        stop_requested = true;
    }
    stop_cv.notify_all();

    if (thread && thread->joinable())
        thread->join();
}

bool PythonRequirementsReconciler::isStopRequested()
{
    std::lock_guard lock(mutex);
    return stop_requested;
}

bool PythonRequirementsReconciler::waitForStopOrTimeout(UInt64 seconds)
{
    std::unique_lock lock(mutex);
    return stop_cv.wait_for(lock, std::chrono::seconds(seconds), [this] { return stop_requested; });
}

void PythonRequirementsReconciler::run()
{
    setThreadName("PyReqReconcile");

    if (!Py_IsInitialized())
    {
        LOG_WARNING(
            logger,
            "Skipping Python requirements reconciliation from '{}': the embedded Python interpreter is not initialized",
            config.url);
        return;
    }

    const char * user_base_env = getenv("PYTHONUSERBASE"); /// set by Server::initPythonInterpreter before the reconciler starts
    if (user_base_env == nullptr || *user_base_env == '\0')
    {
        LOG_WARNING(logger, "Skipping Python requirements reconciliation from '{}': PYTHONUSERBASE is not set", config.url);
        return;
    }
    const std::string python_user_base = user_base_env;

    UInt64 backoff_sec = RETRY_INITIAL_BACKOFF_SEC;
    for (size_t attempt = 1; !isStopRequested(); ++attempt)
    {
        bool reconciled = false;
        try
        {
            reconciled = reconcileOnce(python_user_base, attempt);
        }
        catch (...)
        {
            tryLogCurrentException(
                logger, fmt::format("Failed to reconcile Python requirements from '{}' (attempt {})", config.url, attempt));
        }

        if (reconciled)
        {
            backoff_sec = RETRY_INITIAL_BACKOFF_SEC;
            /// Stay alive and keep polling so updates to the requirements file are picked up
            /// without a server restart; poll_interval_sec = 0 opts out (startup-only).
            if (config.poll_interval_sec == 0)
                return;
            if (waitForStopOrTimeout(config.poll_interval_sec))
                return;
        }
        else
        {
            LOG_WARNING(logger, "Python requirements reconciliation attempt {} did not complete, retrying in {}s", attempt, backoff_sec);
            if (waitForStopOrTimeout(backoff_sec))
                return;
            backoff_sec = std::min(backoff_sec * 2, RETRY_MAX_BACKOFF_SEC);
        }
    }
}

bool PythonRequirementsReconciler::reconcileOnce(const std::string & python_user_base, size_t attempt)
{
    if (!config.index_url.empty())
        cpython::PythonPackage::validatePipIndexUrl(config.index_url, "--index-url");
    for (const auto & extra_index_url : config.extra_index_urls)
        cpython::PythonPackage::validatePipIndexUrl(extra_index_url, "--extra-index-url");

    const auto requirements_text = fetcher ? fetcher() : fetchRequirementsText();
    if (requirements_text.size() > MAX_REQUIREMENTS_BYTES)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Python requirements file '{}' is too large: {} bytes (max {})",
            config.url,
            requirements_text.size(),
            MAX_REQUIREMENTS_BYTES);

    /// An intentionally emptied file (blank lines / comments only) means "nothing to install" —
    /// it must complete the reconcile, not loop forever (parseRequirementsText throws on it).
    ///
    /// By design (for now) it does NOT uninstall previously installed packages:
    /// - an empty file is ambiguous — a botched/truncated upload would otherwise strip packages
    ///   from every node within one poll interval and break all Python UDFs;
    /// - there is no manifest of which packages the reconciler installed vs manual
    ///   SYSTEM INSTALL escape-hatch installs (the marker is only a content digest);
    /// - pip uninstall is not dependency-aware, so removals can break surviving packages.
    /// Use SYSTEM UNINSTALL PYTHON PACKAGE for targeted cleanup; a proper uninstall
    /// reconciliation would need a per-digest install manifest (potential follow-up).
    if (!cpython::PythonPackage::hasEffectiveRequirementLines(requirements_text))
    {
        /// DEBUG on later attempts: this re-fires every poll cycle while the file stays empty.
        if (attempt == 1)
            LOG_INFO(logger, "Python requirements file '{}' has no requirements, nothing to install", config.url);
        else
            LOG_DEBUG(logger, "Python requirements file '{}' has no requirements, nothing to install", config.url);
        return true;
    }

    /// Throws on malformed content (e.g. pip option lines), which gets logged and retried:
    /// a later fetch may return a fixed file.
    const auto requirement_lines = cpython::PythonPackage::parseRequirementsText(requirements_text);

    const auto digest = contentDigest(requirements_text);
    if (readMarker(python_user_base) == digest)
    {
        /// DEBUG on later attempts: this is the steady state of every poll cycle.
        if (attempt == 1)
            LOG_INFO(logger, "Python requirements from '{}' are already applied (digest {}), skipping", config.url, digest);
        else
            LOG_DEBUG(logger, "Python requirements from '{}' are already applied (digest {}), skipping", config.url, digest);
        return true;
    }

    LOG_INFO(
        logger,
        "Applying Python requirements from '{}': {} requirement(s), digest {} (attempt {})",
        config.url,
        requirement_lines.size(),
        digest,
        attempt);

    return installAndWait(requirements_text, digest, python_user_base, attempt);
}

std::string PythonRequirementsReconciler::fetchRequirementsText()
{
    S3::URI uri(config.url);

    if (!s3_client)
    {
        const auto & app_config = global_context->getConfigRef();
        const auto & settings = global_context->getSettingsRef();
        auto auth_settings = S3::AuthSettings::loadFromConfig(CONFIG_SECTION, app_config);

        S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
            auth_settings.region,
            global_context->getRemoteHostFilter(),
            static_cast<unsigned>(settings.s3_max_redirects),
            settings.enable_s3_requests_logging,
            /* for_disk_s3 = */ false,
            /* get_request_throttler = */ {},
            /* put_request_throttler = */ {});

        /// endpointOverride carries the full URI (scheme included), so the client picks up http/https from it.
        client_configuration.endpointOverride = uri.endpoint;

        /// The reconcile loop already retries with backoff, so keep the SDK-internal retries short:
        /// they would otherwise stretch a single fetch attempt to minutes and block stop() — the
        /// reconciler thread cannot be interrupted while it is inside an SDK call.
        client_configuration.retryStrategy
            = std::make_shared<Aws::Client::DefaultRetryStrategy>(/* maxRetries = */ 2, /* scaleFactor = */ 50);
        client_configuration.connectTimeoutMs = 5000;
        client_configuration.requestTimeoutMs = 30000;

        S3::ClientSettings client_settings{
            .use_virtual_addressing = uri.is_virtual_hosted_style,
            .disable_checksum = settings.s3_disable_checksum,
            .gcs_issue_compose_request = app_config.getBool("s3.gcs_issue_compose_request", false),
            .is_s3express_bucket = S3::isS3ExpressEndpoint(uri.endpoint),
        };

        s3_client = S3::ClientFactory::instance().create(
            client_configuration,
            client_settings,
            auth_settings.access_key_id,
            auth_settings.secret_access_key,
            auth_settings.server_side_encryption_customer_key_base64,
            auth_settings.server_side_encryption_kms_config,
            auth_settings.headers,
            S3::CredentialsConfiguration{
                /// Default to the environment credentials chain (IRSA / instance profile) so that
                /// k8s deployments work without explicit keys in the config.
      .use_environment_credentials = auth_settings.use_environment_credentials.value_or(true),
      .use_insecure_imds_request = auth_settings.use_insecure_imds_request.value_or(false),
      .expiration_window_seconds = auth_settings.expiration_window_seconds.value_or(S3::DEFAULT_EXPIRATION_WINDOW_SECONDS),
      .no_sign_request = auth_settings.no_sign_request.value_or(false),
      .role_arn = auth_settings.role_arn,                    /// <-- the fix
      .role_session_name = auth_settings.role_session_name,  /// <-- the fix
            },
            auth_settings.session_token);
    }

    try
    {
        ReadBufferFromS3 read_buffer(s3_client, uri.bucket, uri.key, uri.version_id, S3Settings::RequestSettings{}, ReadSettings{});
        /// Bound the read before buffering: a url misconfigured to point at a large object must not
        /// be fully materialized in memory just to fail the size check afterwards.
        LimitReadBuffer limited_buffer(read_buffer, {.read_no_more = MAX_REQUIREMENTS_BYTES + 1});
        std::string requirements_text;
        readStringUntilEOF(requirements_text, limited_buffer);
        return requirements_text;
    }
    catch (...)
    {
        /// Rebuild the client (and its credentials provider) on the next attempt.
        s3_client.reset();
        throw;
    }
}

bool PythonRequirementsReconciler::installAndWait(
    const std::string & requirements_text, const std::string & digest, const std::string & python_user_base, size_t attempt)
{
    auto * package_manager = global_context->getAsyncPythonPackageManager();
    if (package_manager == nullptr)
        throw Exception(ErrorCodes::RESOURCE_NOT_INITED, "AsyncPythonPackageManager is not initialized");

    /// Drop the previous attempt's entry so retried installs don't accumulate in the results map.
    if (!last_install_task_id.empty())
        package_manager->removeCompletedTask(last_install_task_id);

    cpython::PipInstallOptions install_options;
    install_options.index_url = config.index_url;
    install_options.extra_index_urls = config.extra_index_urls;

    auto completion = std::make_shared<std::promise<cpython::AsyncTaskResult>>();
    auto fulfilled = std::make_shared<std::atomic<bool>>(false);
    auto future = completion->get_future();

    const auto task_id = fmt::format("s3-requirements-{}-attempt-{}", digest.substr(0, 8), attempt);
    auto schedule_result = package_manager->scheduleInstallRequirements(
        task_id, requirements_text, install_options, [completion, fulfilled](const cpython::AsyncTaskResult & task_result) {
            if (task_result.isCompleted() && !fulfilled->exchange(true))
                completion->set_value(task_result);
        });

    if (schedule_result.hasError())
        throw Exception(
            ErrorCodes::RESOURCE_NOT_INITED, "Failed to schedule Python requirements install: {}", schedule_result.errorString());

    last_install_task_id = task_id;

    while (future.wait_for(std::chrono::milliseconds(500)) != std::future_status::ready)
    {
        if (isStopRequested())
        {
            /// Leave the marker unwritten; the next startup reconciles again (pip is idempotent).
            LOG_INFO(logger, "Stop requested while Python requirements install task '{}' is in flight", task_id);
            return false;
        }
    }

    const auto task_result = future.get();
    if (!task_result.isSuccess())
    {
        LOG_ERROR(logger, "Python requirements install task '{}' failed: {}", task_id, task_result.error.string());
        return false;
    }

    writeMarker(python_user_base, digest);
    LOG_INFO(logger, "Successfully applied Python requirements from '{}' (digest {})", config.url, digest);
    return true;
}

}

#endif
