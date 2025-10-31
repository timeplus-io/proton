#include "Server.h"

#include <memory>
#include <sys/resource.h>
#include <sys/stat.h>
#include <cerrno>
#include <pwd.h>
#include <unistd.h>
#include <Poco/Version.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/NetException.h>
#include <Poco/Util/HelpFormatter.h>
#include <Poco/Environment.h>
#include <Common/scope_guard_safe.h>
#include <base/phdr_cache.h>
#include <Common/ErrorHandlers.h>
#include <base/getMemoryAmount.h>
#include <base/getAvailableMemoryAmount.h>
#include <base/getFileSystemSpace.h>
#include <base/errnoToString.h>
#include <base/coverage.h>
#include <base/safeExit.h>
#include <Common/MemoryTracker.h>
#include <Common/VersionRevision.h>
#include <Common/DNSResolver.h>
#include <Common/CurrentMetrics.h>
#include <Common/Macros.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/getExecutablePath.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <Common/getMappedArea.h>
#include <Common/remapExecutable.h>
#include <Common/TLDListsHolder.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/IOThreadPool.h>
#include <IO/UseSSL.h>
#include <IO/Kafka/Connection.h>
#include <Interpreters/AsynchronousMetrics.h>
#include <Interpreters/DNSCacheUpdater.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/ExternalModelsLoader.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/loadMetadata.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Access/AccessControl.h>
#include <Storages/System/attachSystemTables.h>
#include <Storages/System/attachInformationSchemaTables.h>
#include <Storages/Cache/ExternalDataSourceCache.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Functions/registerFunctions.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Formats/registerFormats.h>
#include <Storages/registerStorages.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <QueryPipeline/ConnectionCollector.h>
#include <Databases/registerDatabases.h>
#include <Dictionaries/registerDictionaries.h>
#include <Disks/registerDisks.h>
#include <Disks/IVolume.h>
#include <IO/Resource/registerSchedulerNodes.h>
#include <IO/Resource/registerResourceManagers.h>
#include <Common/Config/ConfigReloader.h>
#include <Server/HTTPHandlerFactory.h>
#include "MetricsTransmitter.h"
#include <Common/StatusFile.h>
#include <Server/TCPHandlerFactory.h>
#include <Server/TCPServer.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/ThreadFuzzer.h>
#include <Common/getHashOfLoadedBinary.h>
#include <Common/filesystemHelpers.h>
#include <Common/Elf.h>
#include <Server/PostgreSQLHandlerFactory.h>
#include <Server/ProtocolServerAdapter.h>
#include <Server/HTTP/HTTPServer.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#if USE_PYTHON_UDF
#include <CPython/AsyncPythonPackageManager.h>
#endif
#include <Compression/CompressionCodecEncrypted.h>
#include <Core/ServerSettings.h>
#include <filesystem>

#include <Common/config_version.h>

/// proton: starts
#include <Bootstrap/Bootstrap.h>
#include <Bootstrap/Globals.h>
#include <Bootstrap/ServerDescriptor.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Apply/MetadataUpdater.h>
#include <Interpreters/BuiltinSchemasProvisioner.h>
#include <Interpreters/TelemetryCollector.h>
#include <Server/RestRouterHandlers/RestRouterFactory.h>
#include <Task/TaskScheduler.h>
#if USE_V8
#include <V8/Modules/DictionaryAccess/CacheDictionaryBridge.h>
#endif
#include <Common/getNumberOfPhysicalCPUCores.h>

#if USE_AWS_MSK_IAM || USE_AWS_S3
#include <IO/S3/Client.h>
#endif

#include <Poco/Net/NetworkInterface.h>

#if USE_V8
#include <V8/V8Includes.h>
#endif

bool LOG_PANIC_ABORT = true;

/// proton: ends

#if defined(OS_LINUX)
#    include <sys/mman.h>
#    include <sys/ptrace.h>
#    include <Common/hasLinuxCapability.h>
#endif

#if USE_SSL
#    include <Poco/Net/Context.h>
#    include <Poco/Net/SecureServerSocket.h>
#endif

#if USE_GRPC
#   include <Server/GRPCServer.h>
#endif

#if USE_JEMALLOC
#    include <jemalloc/jemalloc.h>
#endif

#if USE_AZURE_BLOB_STORAGE
#   include <azure/storage/common/internal/xml_wrapper.hpp>
#endif

namespace CurrentMetrics
{
    extern const Metric Revision;
    extern const Metric VersionInteger;
    extern const Metric MemoryTracking;
    extern const Metric MaxDDLEntryID;
    extern const Metric MaxPushedDDLEntryID;
    extern const Metric MemoryAmount;
    extern const Metric NumOfPhysicalCPUCores;
}

namespace ProfileEvents
{
    extern const Event MainConfigLoads;
}

namespace fs = std::filesystem;

#if USE_JEMALLOC
static bool jemallocOptionEnabled(const char *name)
{
    bool value;
    size_t size = sizeof(value);

    if (mallctl(name, reinterpret_cast<void *>(&value), &size, /* newp= */ nullptr, /* newlen= */ 0))
        throw Poco::SystemException("mallctl() failed");

    return value;
}
#else
static bool jemallocOptionEnabled(const char *) { return false; }
#endif

int mainServer(int argc, char ** argv)
{
    DB::Server app;

    if (jemallocOptionEnabled("opt.background_thread"))
    {
        LOG_ERROR(&app.logger(),
            "jemalloc.background_thread was requested, "
            "however Proton uses percpu_arena and background_thread most likely will not give any benefits, "
            "and also background_thread is not compatible with Proton watchdog "
            "(that can be disabled with PROTON_WATCHDOG_ENABLE=0)");
    }

    /// Do not fork separate process from watchdog if we attached to terminal.
    /// Otherwise it breaks gdb usage.
    /// Can be overridden by environment variable (cannot use server config at this moment).
    if (argc > 0)
    {
        const char * env_watchdog = getenv("PROTON_WATCHDOG_ENABLE"); /// NOLINT(concurrency-mt-unsafe)
        if (env_watchdog)
        {
            if (0 == strcmp(env_watchdog, "1"))
                app.shouldSetupWatchdog(argv[0]);

            /// Other values disable watchdog explicitly.
        }
        else if (!isatty(STDIN_FILENO) && !isatty(STDOUT_FILENO) && !isatty(STDERR_FILENO))
            app.shouldSetupWatchdog(argv[0]);
    }

    try
    {
        return app.run(argc, argv);
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
        auto code = DB::getCurrentExceptionCode();
        return code ? code : 1;
    }
}

namespace
{

size_t waitServersToFinish(std::vector<DB::ProtocolServerAdapter> & servers, std::mutex & mutex, size_t seconds_to_wait)
{
    const size_t sleep_max_ms = 1000 * seconds_to_wait;
    const size_t sleep_one_ms = 100;
    size_t sleep_current_ms = 0;
    size_t current_connections = 0;
    for (;;)
    {
        current_connections = 0;

        {
            std::lock_guard lock{mutex};
            for (auto & server : servers)
            {
                server.stop();
                current_connections += server.currentConnections();
            }
        }

        if (!current_connections)
            break;

        sleep_current_ms += sleep_one_ms;
        if (sleep_current_ms < sleep_max_ms)
            sleepForMilliseconds(sleep_one_ms);
        else
            break;
    }
    return current_connections;
}

/// proton: starts
/// The shutdown sequence is
/// 1) Stop TCP/HTTP etc servers to close all outstanding external TCP/HTTP connections
/// 2) Kill all unfinished queries
/// 3) Shutdown Context
///    - Disable periodic reload for Access Control
///    - Disable periodic updates for external dictionaries
///    - Disable periodic updates for external user defined functions
///    - Shutdown named sessions
///    - Shutdown system logs
///    - DatabaseCatalog::shutdown which shutdown all table engines -> DMT depends on DWAL
///    - Wait for merge mutate executor to finish
///    - Wait for fetch executor to finish
///    - Wait for moves executor to finish
///    - Wait for common executor to finish
///    - Clean CompiledExpressionCache
///    - Reset dictionaries XMLs
///    - Reset user defined executable function XMLs
///    - Reset embedded dictionaries
///    - Reset external dictionaries loader
///    - Reset model repository
///    - Reset buffer flush schedule pool
///    - Reset schedule pool
///    - Reset distributed schedule pool
///    - Reset message broker schedule pool
///    - Reset part commit pool
///    - Reset access control pool
///    - Reset trace control pool
///    - Shutdown system logs
/// 4) Static singletons in function scope dtor
///    - These singletons are dtored in the reverse order of their construction
///    - They construction order can be manually ordered
/// 5) Global singletons / variables dtor
///    - These global variables are dtored in the reverse order of their construction
///    - C++ basically can't guarantee the construction order unless we init them in a control way
///      otherwise we can't guarantee their dtor order. We need avoid these kind of global variables
///      as much as possible
/// proton: ends
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int SUPPORT_IS_DISABLED;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int SYSTEM_ERROR;
    extern const int FAILED_TO_GETPWUID;
    extern const int MISMATCHING_USERS_FOR_PROCESS_AND_DATA;
    extern const int NETWORK_ERROR;
    extern const int CORRUPTED_DATA;
}

/// proton starts:
namespace
{
bool networkInterfaceSupportsIPv6()
{
    try
    {
        for (const auto & ni : Poco::Net::NetworkInterface::list())
        {
            if (ni.supportsIPv6())
                return true;
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    return false;
}
}
/// proton ends


static std::string getCanonicalPath(std::string && path)
{
    Poco::trimInPlace(path);
    if (path.empty())
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "path configuration parameter is empty");
    if (path.back() != '/')
        path += '/';
    return std::move(path);
}

static std::string getUserName(uid_t user_id)
{
    /// Try to convert user id into user name.
    auto buffer_size = sysconf(_SC_GETPW_R_SIZE_MAX);
    if (buffer_size <= 0)
        buffer_size = 1024;
    std::string buffer;
    buffer.reserve(buffer_size);

    struct passwd passwd_entry;
    struct passwd * result = nullptr;
    const auto error = getpwuid_r(user_id, &passwd_entry, buffer.data(), buffer_size, &result);

    if (error)
        throwFromErrno("Failed to find user name for " + toString(user_id), ErrorCodes::FAILED_TO_GETPWUID, error);
    else if (result)
        return result->pw_name;
    return toString(user_id);
}

Poco::Net::SocketAddress makeSocketAddress(const std::string & host, UInt16 port, Poco::Logger * log)
{
    Poco::Net::SocketAddress socket_address;
    try
    {
        socket_address = Poco::Net::SocketAddress(host, port);
    }
    catch (const Poco::Net::DNSException & e)
    {
        const auto code = e.code();
        if (code == EAI_FAMILY
#if defined(EAI_ADDRFAMILY)
                    || code == EAI_ADDRFAMILY
#endif
           )
        {
            LOG_ERROR(log, "Cannot resolve listen_host ({}), error {}: {}. "
                "If it is an IPv6 address and your host has disabled IPv6, then consider to "
                "specify IPv4 address to listen in <listen_host> element of configuration "
                "file. Example: <listen_host>0.0.0.0</listen_host>",
                host, e.code(), e.message());
        }

        throw;
    }
    return socket_address;
}

Poco::Net::SocketAddress Server::socketBindListen(Poco::Net::ServerSocket & socket, const std::string & host, UInt16 port, [[maybe_unused]] bool secure) const
{
    auto address = makeSocketAddress(host, port, &logger());
#if !defined(POCO_CLICKHOUSE_PATCH) || POCO_VERSION < 0x01090100
    if (secure)
        /// Bug in old (<1.9.1) poco, listen() after bind() with reusePort param will fail because have no implementation in SecureServerSocketImpl
        /// https://github.com/pocoproject/poco/pull/2257
        socket.bind(address, /* reuseAddress = */ true);
    else
#endif
#if POCO_VERSION < 0x01080000
    socket.bind(address, /* reuseAddress = */ true);
#else
    socket.bind(address, /* reuseAddress = */ true, /* reusePort = */ config().getBool("listen_reuse_port", false));
#endif

    /// If caller requests any available port from the OS, discover it after binding.
    if (port == 0)
    {
        address = socket.address();
        LOG_DEBUG(&logger(), "Requested any available port (port == 0), actual port is {:d}", address.port());
    }

    socket.listen(/* backlog = */ config().getUInt("listen_backlog", 4096));

    return address;
}

void Server::createServer(
    [[maybe_unused]] Poco::Util::AbstractConfiguration & config,
    const std::string & listen_host,
    const char * port_name,
    uint64_t port,
    bool listen_try,
    bool start_server,
    std::vector<ProtocolServerAdapter> & servers,
    CreateServerFunc && func) const
{
    /// If we already have an active server for this listen_host/port_name, don't create it again
    for (const auto & server : servers)
        if (!server.isStopping() && server.getListenHost() == listen_host && server.getPortName() == port_name)
            return;

    try
    {
        servers.push_back(func(port));
        if (start_server)
        {
            servers.back().start();
            LOG_INFO(&logger(), "Listening for {}", servers.back().getDescription());
        }
        global_context->registerServerPort(port_name, port);
    }
    catch (const Poco::Exception &)
    {
        if (listen_try)
        {
            LOG_WARNING(&logger(), "Listen [{}]:{} failed: {}. If it is an IPv6 or IPv4 address and your host has disabled IPv6 or IPv4, "
                "then consider to "
                "specify not disabled IPv4 or IPv6 address to listen in <listen_host> element of configuration "
                "file. Example for disabled IPv6: <listen_host>0.0.0.0</listen_host> ."
                " Example for disabled IPv4: <listen_host>::</listen_host>",
                listen_host, port, getCurrentExceptionMessage(false));
        }
        else
        {
            throw Exception(ErrorCodes::NETWORK_ERROR, "Listen [{}]:{} failed: {}", listen_host, port, getCurrentExceptionMessage(false));
        }
    }
}


#if defined(OS_LINUX)
namespace
{

void setOOMScore(int value, LoggerRawPtr log)
{
    try
    {
        std::string value_string = std::to_string(value);
        DB::WriteBufferFromFile buf("/proc/self/oom_score_adj");
        buf.write(value_string.c_str(), value_string.size());
        buf.next();
        buf.close();
    }
    catch (const Poco::Exception & e)
    {
        LOG_WARNING(log, "Failed to adjust OOM score: '{}'.", e.displayText());
        return;
    }
    LOG_INFO(log, "Set OOM score adjustment to {}", value);
}

}
#endif


void Server::uninitialize()
{
    logger().information("shutting down");
    BaseDaemon::uninitialize();
}

int Server::run()
{
    if (config().hasOption("help"))
    {
        Poco::Util::HelpFormatter help_formatter(Server::options());
        auto header_str = fmt::format("{} [OPTION] [-- [ARG]...]\n"
                                      "positional arguments can be used to rewrite config.xml properties, for example, --http_port=8010",
                                      commandName());
        help_formatter.setHeader(header_str);
        help_formatter.format(std::cout);
        return 0;
    }
    if (config().hasOption("version"))
    {
        std::cout << VERSION_NAME << " server version " << VERSION_STRING << VERSION_OFFICIAL << "." << std::endl;
        return 0;
    }

    return Application::run(); // NOLINT
}

void Server::initialize(Poco::Util::Application & self)
{
    BaseDaemon::initialize(self);
    logger().information("starting up");

    LOG_INFO(&logger(), "OS name: {}, version: {}, architecture: {}",
        Poco::Environment::osName(),
        Poco::Environment::osVersion(),
        Poco::Environment::osArchitecture());
}

/// proton: starts
std::string Server::getDefaultPath() const
{
    return getCanonicalPath(config().getString("path", DBMS_DEFAULT_PATH));
}
/// proton: ends

std::string Server::getDefaultCorePath() const
{
    return getDefaultPath() + "cores";
}

void Server::defineOptions(Poco::Util::OptionSet & options)
{
    options.addOption(
        Poco::Util::Option("help", "h", "show help and exit")
            .required(false)
            .repeatable(false)
            .binding("help"));
    options.addOption(
        Poco::Util::Option("version", "V", "show version and exit")
            .required(false)
            .repeatable(false)
            .binding("version"));
    BaseDaemon::defineOptions(options);
}


void checkForUsersNotInMainConfig(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_path,
    const std::string & users_config_path,
    LoggerPtr log)
{
    if (config.getBool("skip_check_for_incorrect_settings", false))
        return;

    if (config.has("users") || config.has("profiles") || config.has("quotas"))
    {
        /// We cannot throw exception here, because we have support for obsolete 'conf.d' directory
        /// (that does not correspond to config.d or users.d) but substitute configuration to both of them.

        LOG_ERROR(log, "The <users>, <profiles> and <quotas> elements should be located in users config file: {} not in main config {}."
            " Also note that you should place configuration changes to the appropriate *.d directory like 'users.d'.",
            users_config_path, config_path);
    }
}

/// Unused in other builds
#if defined(OS_LINUX)
static String readString(const String & path)
{
    ReadBufferFromFile in(path);
    String contents;
    readStringUntilEOF(contents, in);
    return contents;
}

static int readNumber(const String & path)
{
    ReadBufferFromFile in(path);
    int result;
    readText(result, in);
    return result;
}

#endif

static void sanityChecks(Server * server)
{
    std::string data_path = server->getDefaultPath();
    std::string logs_path = server->config().getString("logger.log", "");

#if defined(OS_LINUX)
    try
    {
        const char * filename = "/sys/devices/system/clocksource/clocksource0/current_clocksource";
        String clocksource = readString(filename);
        if (clocksource.find("tsc") == std::string::npos && clocksource.find("kvm-clock") == std::string::npos)
            server->context()->addWarningMessage("Linux is not using a fast clock source. Performance can be degraded. Check " + String(filename));
    }
    catch (...)
    {
    }

    try
    {
        if (readNumber("/proc/sys/vm/overcommit_memory") == 2)
            server->context()->addWarningMessage("Linux memory overcommit is disabled.");
    }
    catch (...)
    {
    }

    try
    {
        if (readString("/sys/kernel/mm/transparent_hugepage/enabled").find("[always]") != std::string::npos)
            server->context()->addWarningMessage("Linux transparent hugepage are set to \"always\".");
    }
    catch (...)
    {
    }

    try
    {
        if (readNumber("/proc/sys/kernel/pid_max") < 30000)
            server->context()->addWarningMessage("Linux max PID is too low.");
    }
    catch (...)
    {
    }

    try
    {
        if (readNumber("/proc/sys/kernel/threads-max") < 30000)
            server->context()->addWarningMessage("Linux threads max count is too low.");
    }
    catch (...)
    {
    }

    std::string dev_id = getBlockDeviceId(data_path);
    if (getBlockDeviceType(dev_id) == BlockDeviceType::ROT && getBlockDeviceReadAheadBytes(dev_id) == 0)
        server->context()->addWarningMessage("Rotational disk with disabled readahead is in use. Performance can be degraded.");
#endif

    try
    {
        if (getAvailableMemoryAmount() < (2l << 30))
            server->context()->addWarningMessage("Available memory at server startup is too low (2GiB).");
    }
    catch (...)
    {
    }

    try
    {
        if (!enoughSpaceInDirectory(data_path, 1ull << 30))
            server->context()->addWarningMessage("Available disk space for data at server startup is too low (1GiB): " + String(data_path));
    }
    catch (...)
    {
    }

    try
    {
        if (!logs_path.empty())
        {
            auto logs_parent = fs::path(logs_path).parent_path();
            if (!enoughSpaceInDirectory(logs_parent, 1ull << 30))
                server->context()->addWarningMessage("Available disk space for logs at server startup is too low (1GiB): " + String(logs_parent));
        }
    }
    catch (...)
    {
    }

    if (server->context()->getMergeTreeSettings().allow_remote_fs_zero_copy_replication)
    {
        server->context()->addWarningMessage("The setting 'allow_remote_fs_zero_copy_replication' is enabled for MergeTree tables."
            " But the feature of 'zero-copy replication' is under development and is not ready for production."
            " The usage of this feature can lead to data corruption and loss. The setting should be disabled in production.");
    }
}

int Server::main(const std::vector<std::string> & /*args*/)
try
{
    Poco::Logger * log = &logger();

    /// proton: starts
#if USE_AWS_MSK_IAM || USE_AWS_S3
    LOG_INFO(log, "Initializing AWS SDK");
    /// Later on, we should make a refactor to move the common AWS code out from IO/S3 to somewhere like IO/AWS.
    S3::ClientFactory::instance();
#endif
    /// proton: ends

    UseSSL use_ssl;

    MainThreadStatus::getInstance();

    ServerSettings server_settings;
    server_settings.loadSettingsFromConfig(config());

    ///StackTrace::setShowAddresses(server_settings.show_addresses_in_stack_traces);

    /// When building openssl into clickhouse, clickhouse owns the configuration
    /// Therefore, the clickhouse openssl configuration should be kept separate from
    /// the OS. Default to the one in the standard config directory, unless overridden
    /// by a key in the config.
    /// Note: this has to be done once at server initialization, because 'setenv' is not thread-safe.
    if (config().has("opensslconf"))
    {
        std::string opensslconf_path = config().getString("opensslconf");
        setenv("OPENSSL_CONF", opensslconf_path.c_str(), true); /// NOLINT
    }
    else
    {
        const String config_path = config().getString("config-file", "config.xml");
        const auto config_dir = std::filesystem::path{config_path}.replace_filename("openssl.conf");
        setenv("OPENSSL_CONF", config_dir.c_str(), true); /// NOLINT
    }

    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerDatabases();
    registerStorages();
    registerDictionaries();
    registerDisks(/* global_skip_access_check= */ false);
    registerFormats();
    registerSchedulerNodes();
    registerResourceManagers();

    CurrentMetrics::set(CurrentMetrics::Revision, ProtonRevision::getVersionRevision());
    CurrentMetrics::set(CurrentMetrics::VersionInteger, ProtonRevision::getVersionInteger());
    CurrentMetrics::set(CurrentMetrics::MemoryAmount, getMemoryAmount());
    CurrentMetrics::set(CurrentMetrics::NumOfPhysicalCPUCores, getNumberOfPhysicalCPUCores());

    /** Context contains all that query execution is dependent:
      *  settings, available functions, data types, aggregate functions, databases, ...
      */
    auto shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());

    global_context->makeGlobalContext();
    global_context->setApplicationType(Context::ApplicationType::SERVER);

#if !defined(NDEBUG) || !defined(__OPTIMIZE__)
    global_context->addWarningMessage("Server was built in debug mode. It will work slowly.");
#endif

    if (ThreadFuzzer::instance().isEffective())
        global_context->addWarningMessage("ThreadFuzzer is enabled. Application will run slowly and unstable.");

#if defined(SANITIZER)
    global_context->addWarningMessage("Server was built with sanitizer. It will work slowly.");
#endif

    const auto memory_amount = getMemoryAmount();

    LOG_INFO(log, "Available RAM: {}; physical cores: {}; logical cores: {}.",
        formatReadableSizeWithBinarySuffix(memory_amount),
        getNumberOfPhysicalCPUCores(),  // on ARM processors it can show only enabled at current moment cores
        std::thread::hardware_concurrency());

    sanityChecks(this);

    /// proton: starts.

    /// Load configured global settings.
    global_context->applyGlobalSettingsFromConfig();

    /// Set the limits on Kafka connections.
    Kafka::ConnectionFactory::instance().setConnectionLimits({
        .max_alive_consumer_handles = config().getUInt64("external_stream.kafka.max_alive_consumer_handles", 100'000),
        .consumer_handle_share_soft_cap = config().getUInt64("external_stream.kafka.consumer_handle_share_soft_cap", 10),
        .max_alive_producer_handles = config().getUInt64("external_stream.kafka.max_alive_producer_handles", 100'000),
        .producer_handle_share_soft_cap = config().getUInt64("external_stream.kafka.producer_handle_share_soft_cap", 10),
    });
    /// proton: ends

    // Initialize global thread pool. Do it before we fetch configs from zookeeper
    // nodes (`from_zk`), because ZooKeeper interface uses the pool. We will
    // ignore `max_thread_pool_size` in configs we fetch from ZK, but oh well.
    GlobalThreadPool::initialize(
        server_settings.max_thread_pool_size,
        server_settings.max_thread_pool_free_size,
        server_settings.thread_pool_queue_size);

    Poco::ThreadPool server_pool(3, server_settings.max_connections);
    std::mutex servers_lock;
    std::vector<ProtocolServerAdapter> servers;
    std::vector<ProtocolServerAdapter> servers_to_start_before_tables;

    /// Wait for all threads to avoid possible use-after-free (for example logging objects can be already destroyed).
    /// proton: starts: proton has some threads that are not managed by the global thread pool(like v8), so we need to wait for them
    /// https://github.com/timeplus-io/proton-enterprise/pull/9865#discussion_r2209378216
    /// SCOPE_EXIT({
    ///     Stopwatch watch;
    ///     LOG_INFO(log, "Waiting for background threads");
    ///     GlobalThreadPool::instance().shutdown();
    ///     LOG_INFO(log, "Background threads finished in {} ms", watch.elapsedMilliseconds());
    /// });
    /// proton: ends

#if USE_AZURE_BLOB_STORAGE
    /// It makes sense to deinitialize libxml after joining of all threads
    /// in global pool because libxml uses thread-local memory allocations via
    /// 'pthread_key_create' and 'pthread_setspecific' which should be deallocated
    /// at 'pthread_exit'. Deinitialization of libxml leads to call of 'pthread_key_delete'
    /// and if it is done before joining of threads, allocated memory will not be freed
    /// and there may be memory leaks in threads that used libxml.
    GlobalThreadPool::instance().addOnDestroyCallback([]
    {
        Azure::Storage::_internal::XmlGlobalDeinitialize();
    });
#endif

    IOThreadPool::initialize(
        server_settings.max_io_thread_pool_size,
        server_settings.max_io_thread_pool_free_size,
        server_settings.io_thread_pool_queue_size);

    NamedCollectionFactory::instance().loadIfNot();

    /// Initialize global local cache for remote filesystem.
    if (config().has("local_cache_for_remote_fs"))
    {
        bool enable = config().getBool("local_cache_for_remote_fs.enable", false);
        if (enable)
        {
            String root_dir = config().getString("local_cache_for_remote_fs.root_dir");
            UInt64 limit_size = config().getUInt64("local_cache_for_remote_fs.limit_size");
            UInt64 bytes_read_before_flush
                = config().getUInt64("local_cache_for_remote_fs.bytes_read_before_flush", DBMS_DEFAULT_BUFFER_SIZE);
            ExternalDataSourceCache::instance().initOnce(global_context, root_dir, limit_size, bytes_read_before_flush);
        }
    }

    /// This object will periodically calculate some metrics.
    AsynchronousMetrics async_metrics(
        global_context, server_settings.asynchronous_metrics_update_period_s,
        [&]() -> std::vector<ProtocolServerMetrics>
        {
            std::vector<ProtocolServerMetrics> metrics;
            metrics.reserve(servers_to_start_before_tables.size());
            for (const auto & server : servers_to_start_before_tables)
                metrics.emplace_back(ProtocolServerMetrics{server.getPortName(), server.currentThreads()});

            std::lock_guard lock(servers_lock);
            for (const auto & server : servers)
                metrics.emplace_back(ProtocolServerMetrics{server.getPortName(), server.currentThreads()});
            return metrics;
        }
    );

    ConnectionCollector::init(global_context, server_settings.max_threads_for_connection_collector);

    Settings::checkNoSettingNamesAtTopLevel(config(), config_path);

    /// We need to reload server settings because config could be updated via zookeeper.
    server_settings.loadSettingsFromConfig(config());

#if defined(OS_LINUX)
    std::string executable_path = getExecutablePath();

    if (!executable_path.empty())
    {
        /// Integrity check based on checksum of the executable code.
        /// Note: it is not intended to protect from malicious party,
        /// because the reference checksum can be easily modified as well.
        /// And we don't involve asymmetric encryption with PKI yet.
        /// It's only intended to protect from faulty hardware.
        /// Note: it is only based on machine code.
        /// But there are other sections of the binary (e.g. exception handling tables)
        /// that are interpreted (not executed) but can alter the behaviour of the program as well.

        /// Please keep the below log messages in-sync with the ones in daemon/BaseDaemon.cpp
        if (stored_binary_hash.empty())
        {
            LOG_WARNING(log, "Integrity check of the executable skipped because the reference checksum could not be read.");
        }
        else
        {
            String calculated_binary_hash = getHashOfLoadedBinaryHex();
            if (calculated_binary_hash == stored_binary_hash)
            {
                LOG_INFO(log, "Integrity check of the executable successfully passed (checksum: {})", calculated_binary_hash);
            }
            else
            {
                /// If program is run under debugger, ptrace will fail.
                if (ptrace(PTRACE_TRACEME, 0, nullptr, nullptr) == -1)
                {
                    /// Program is run under debugger. Modification of it's binary image is ok for breakpoints.
                    global_context->addWarningMessage(fmt::format(
                        "Server is run under debugger and its binary image is modified (most likely with breakpoints).",
                        calculated_binary_hash));
                }
                else
                {
                    throw Exception(
                        ErrorCodes::CORRUPTED_DATA,
                        "Calculated checksum of the executable ({0}) does not correspond"
                        " to the reference checksum stored in the executable ({1})."
                        " This may indicate one of the following:"
                        " - the executable {2} was changed just after startup;"
                        " - the executable {2} was corrupted on disk due to faulty hardware;"
                        " - the loaded executable was corrupted in memory due to faulty hardware;"
                        " - the file {2} was intentionally modified;"
                        " - a logical error in the code.",
                        calculated_binary_hash,
                        stored_binary_hash,
                        executable_path);
                }
            }
        }
    }
    else
        executable_path = "/usr/bin/proton";    /// It is used for information messages.

    /// After full config loaded
    {
        if (config().getBool("remap_executable", false))
        {
            LOG_DEBUG(log, "Will remap executable in memory.");
            size_t size = remapExecutable();
            LOG_DEBUG(log, "The code ({}) in memory has been successfully remapped.", ReadableSize(size));
        }

        if (config().getBool("mlock_executable", false))
        {
            if (hasLinuxCapability(CAP_IPC_LOCK))
            {
                try
                {
                    /// Get the memory area with (current) code segment.
                    /// It's better to lock only the code segment instead of calling "mlockall",
                    /// because otherwise debug info will be also locked in memory, and it can be huge.
                    auto [addr, len] = getMappedArea(reinterpret_cast<void *>(mainServer));

                    LOG_TRACE(log, "Will do mlock to prevent executable memory from being paged out. It may take a few seconds.");
                    if (0 != mlock(addr, len))
                        LOG_WARNING(log, "Failed mlock: {}", errnoToString(ErrorCodes::SYSTEM_ERROR));
                    else
                        LOG_TRACE(log, "The memory map of proton executable has been mlock'ed, total {}", ReadableSize(len));
                }
                catch (...)
                {
                    LOG_WARNING(log, "Cannot mlock: {}", getCurrentExceptionMessage(false));
                }
            }
            else
            {
                LOG_INFO(log, "It looks like the process has no CAP_IPC_LOCK capability, binary mlock will be disabled."
                    " It could happen due to incorrect proton package installation."
                    " You could resolve the problem manually with 'sudo setcap cap_ipc_lock=+ep {}'."
                    " Note that it will not work on 'nosuid' mounted filesystems.", executable_path);
            }
        }
    }

    int default_oom_score = 0;

#if !defined(NDEBUG)
    /// In debug version on Linux, increase oom score so that clickhouse is killed
    /// first, instead of some service. Use a carefully chosen random score of 555:
    /// the maximum is 1000, and chromium uses 300 for its tab processes. Ignore
    /// whatever errors that occur, because it's just a debugging aid and we don't
    /// care if it breaks.
    default_oom_score = 555;
#endif

    int oom_score = config().getInt("oom_score", default_oom_score);
    if (oom_score)
        setOOMScore(oom_score, log);
#endif

    global_context->setRemoteHostFilter(config());

    /// proton: starts
    std::string path_str = getDefaultPath();
    fs::path path = path_str;
    std::string default_database = server_settings.default_database.toString();
    std::string neutron_database = config().getString("neutron_database", "neutron");
    std::vector<std::string> builtin_databases{default_database, neutron_database};

    auto [total_space, free_space, available_space, used_space] = getFileSystemSpace(path_str);

    LOG_INFO(
        log,
        "Proton core path is: {}. The device where this directory is located has total space: {}, free space: {}, available space: {}, "
        "used space: {}.",
        path_str,
        formatReadableSizeWithBinarySuffix(total_space),
        formatReadableSizeWithBinarySuffix(free_space),
        formatReadableSizeWithBinarySuffix(available_space),
        formatReadableSizeWithBinarySuffix(used_space));
    /// proton: ends

    /// Check that the process user id matches the owner of the data.
    const auto effective_user_id = geteuid();
    struct stat statbuf;
    if (stat(path_str.c_str(), &statbuf) == 0 && effective_user_id != statbuf.st_uid)
    {
        const auto effective_user = getUserName(effective_user_id);
        const auto data_owner = getUserName(statbuf.st_uid);
        std::string message = "Effective user of the process (" + effective_user +
            ") does not match the owner of the data (" + data_owner + ").";
        if (effective_user_id == 0)
        {
            message += " Run under 'sudo -u " + data_owner + "'.";
            throw Exception::createDeprecated(message, ErrorCodes::MISMATCHING_USERS_FOR_PROCESS_AND_DATA);
        }
        else
        {
            global_context->addWarningMessage(message);
        }
    }

    global_context->setPath(path_str);

    StatusFile status{path / "status", StatusFile::write_full_info};

    /// Try to increase limit on number of open files.
    {
        rlimit rlim;
        if (getrlimit(RLIMIT_NOFILE, &rlim))
            throw Poco::Exception("Cannot getrlimit");

        if (rlim.rlim_cur == rlim.rlim_max)
        {
            LOG_DEBUG(log, "rlimit on number of file descriptors is {}", rlim.rlim_cur);
        }
        else
        {
            rlim_t old = rlim.rlim_cur;
            rlim.rlim_cur = config().getUInt("max_open_files", static_cast<unsigned>(rlim.rlim_max));
            int rc = setrlimit(RLIMIT_NOFILE, &rlim);
            if (rc != 0)
                LOG_WARNING(log, "Cannot set max number of file descriptors to {}. Try to specify max_open_files according to your system limits. error: {}", rlim.rlim_cur, strerror(errno)); /// NOLINT(concurrency-mt-unsafe)
            else
                LOG_DEBUG(log, "Set max number of file descriptors to {} (was {}).", rlim.rlim_cur, old);
        }
    }

    /// Try to increase limit on number of threads.
    {
        rlimit rlim;
        if (getrlimit(RLIMIT_NPROC, &rlim))
            throw Poco::Exception("Cannot getrlimit");

        if (rlim.rlim_cur == rlim.rlim_max)
        {
            LOG_DEBUG(log, "rlimit on number of threads is {}", rlim.rlim_cur);
        }
        else
        {
            rlim_t old = rlim.rlim_cur;
            rlim.rlim_cur = rlim.rlim_max;
            int rc = setrlimit(RLIMIT_NPROC, &rlim);
            if (rc != 0)
            {
                LOG_WARNING(log, "Cannot set max number of threads to {}. error: {}", rlim.rlim_cur, strerror(errno));
                rlim.rlim_cur = old;
            }
            else
            {
                LOG_DEBUG(log, "Set max number of threads to {} (was {}).", rlim.rlim_cur, old);
            }
        }

        if (rlim.rlim_cur < 30000)
        {
            global_context->addWarningMessage("Maximum number of threads is lower than 30000. There could be problems with handling a lot of simultaneous queries.");
        }
    }

    static ServerErrorHandler error_handler;
    Poco::ErrorHandler::set(&error_handler);

    /// Initialize DateLUT early, to not interfere with running time of first query.
    LOG_DEBUG(log, "Initializing DateLUT.");
    DateLUT::instance();
    LOG_TRACE(log, "Initialized DateLUT with time zone '{}'.", DateLUT::instance().getTimeZone());

    /// Storage with temporary data for processing of heavy queries.
    if (!server_settings.tmp_policy.value.empty())
    {
        global_context->setTemporaryStoragePolicy(server_settings.tmp_policy, server_settings.max_temporary_data_on_disk_size);
    }
    else if (!server_settings.temporary_data_in_cache.value.empty())
    {
        global_context->setTemporaryStorageInCache(server_settings.temporary_data_in_cache, server_settings.max_temporary_data_on_disk_size);
    }
    else
    {
        std::string temporary_path = config().getString("tmp_path", path / "tmp/");
        global_context->setTemporaryStoragePath(temporary_path, server_settings.max_temporary_data_on_disk_size);
    }

    /** Directory with 'flags': files indicating temporary settings for the server set by system administrator.
      * Flags may be cleared automatically after being applied by the server.
      * Examples: do repair of local data; clone all replicated tables from replica.
      */
    {
        auto flags_path = path / "flags/";
        fs::create_directories(flags_path);
        global_context->setFlagsPath(flags_path);
    }

    /** Directory with user provided files that are usable by 'file' table function.
      */
    {

        std::string user_files_path = config().getString("user_files_path", path / "user_files/");
        global_context->setUserFilesPath(user_files_path);
        fs::create_directories(user_files_path);
    }

    {
        std::string dictionaries_lib_path = config().getString("dictionaries_lib_path", path / "dictionaries_lib/");
        global_context->setDictionariesLibPath(dictionaries_lib_path);
        fs::create_directories(dictionaries_lib_path);
    }

    {
        std::string user_scripts_path = config().getString("user_scripts_path", path / "user_scripts/");
        global_context->setUserScriptsPath(user_scripts_path);
        fs::create_directories(user_scripts_path);
    }

    /// top_level_domains_lists
    {
        const std::string & top_level_domains_path = config().getString("top_level_domains_path", path / "top_level_domains/");
        TLDListsHolder::getInstance().parseConfig(fs::path(top_level_domains_path) / "", config());
    }

    {
        fs::create_directories(path / "data/");
        fs::create_directories(path / "metadata/");
        fs::create_directories(path / "user_defined/");

        /// Directory with metadata of tables, which was marked as dropped by Atomic database
        fs::create_directories(path / "metadata_dropped/");
    }

#if USE_ROCKSDB
    /// Initialize merge tree metadata cache
    if (config().has("merge_tree_metadata_cache"))
    {
        fs::create_directories(path / "rocksdb/");
        size_t size = config().getUInt64("merge_tree_metadata_cache.lru_cache_size", 256 << 20);
        bool continue_if_corrupted = config().getBool("merge_tree_metadata_cache.continue_if_corrupted", false);
        try
        {
            LOG_DEBUG(
                log, "Initializing merge tree metadata cache lru_cache_size:{} continue_if_corrupted:{}", size, continue_if_corrupted);
            global_context->initializeMergeTreeMetadataCache(path_str + "/" + "rocksdb", size);
        }
        catch (...)
        {
            if (continue_if_corrupted)
            {
                /// Rename rocksdb directory and reinitialize merge tree metadata cache
                time_t now = time(nullptr);
                fs::rename(path / "rocksdb", path / ("rocksdb.old." + std::to_string(now)));
                global_context->initializeMergeTreeMetadataCache(path_str + "/" + "rocksdb", size);
            }
            else
            {
                throw;
            }
        }
    }
#endif

    if (config().has("macros"))
        global_context->setMacros(std::make_unique<Macros>(config(), "macros", log));

    /// Initialize main config reloader.
    std::string include_from_path = config().getString("include_from", "/etc/metrika.xml");

    if (config().has("query_masking_rules"))
    {
        SensitiveDataMasker::setInstance(std::make_unique<SensitiveDataMasker>(config(), "query_masking_rules"));
    }

    /// proton: starts.
    DB::bootstrap(global_context, log);

    /// Register REST API handler in advance
    RestRouterFactory::registerRestRouterHandlers();

    auto server_descriptor = global_context->getServerDescriptor();

    for (auto & server : servers_to_start_before_tables)
    {
        server.start();
        LOG_INFO(log, "Listening for {}", server.getDescription());
    }

    auto main_config_reloader = std::make_unique<ConfigReloader>(
        config_path,
        include_from_path,
        config().getString("path", ""),
        [&](ConfigurationPtr config, [[maybe_unused]] bool initial_loading)
        {
            Settings::checkNoSettingNamesAtTopLevel(*config, config_path);

            global_context->setConfig(config);

            ServerSettings new_server_settings;
            new_server_settings.loadSettingsFromConfig(*config);

            size_t max_server_memory_usage = new_server_settings.max_server_memory_usage;

            double max_server_memory_usage_to_ram_ratio = new_server_settings.max_server_memory_usage_to_ram_ratio;
            size_t default_max_server_memory_usage = static_cast<size_t>(memory_amount * max_server_memory_usage_to_ram_ratio);

            if (max_server_memory_usage == 0)
            {
                max_server_memory_usage = default_max_server_memory_usage;
                LOG_INFO(log, "Setting max_server_memory_usage was set to {}"
                              " ({} available * {:.2f} max_server_memory_usage_to_ram_ratio)",
                         formatReadableSizeWithBinarySuffix(max_server_memory_usage),
                         formatReadableSizeWithBinarySuffix(memory_amount),
                         max_server_memory_usage_to_ram_ratio);
            }
            else if (max_server_memory_usage > default_max_server_memory_usage)
            {
                max_server_memory_usage = default_max_server_memory_usage;
                LOG_INFO(log, "Setting max_server_memory_usage was lowered to {}"
                              " because the system has low amount of memory. The amount was"
                              " calculated as {} available"
                              " * {:.2f} max_server_memory_usage_to_ram_ratio",
                         formatReadableSizeWithBinarySuffix(max_server_memory_usage),
                         formatReadableSizeWithBinarySuffix(memory_amount),
                         max_server_memory_usage_to_ram_ratio);
            }

            total_memory_tracker.setHardLimit(max_server_memory_usage);
            total_memory_tracker.setDescription("(total)");
            total_memory_tracker.setMetric(CurrentMetrics::MemoryTracking);

            total_memory_tracker.setAllowUseJemallocMemory(new_server_settings.allow_use_jemalloc_memory);

            auto * global_overcommit_tracker = global_context->getGlobalOvercommitTracker();
            total_memory_tracker.setOvercommitTracker(global_overcommit_tracker);

            // FIXME logging-related things need synchronization -- see the 'Logger * log' saved
            // in a lot of places. For now, disable updating log configuration without server restart.
            //setTextLog(global_context->getTextLog());
            updateLevels(*config, logger());
            global_context->setMacros(std::make_unique<Macros>(*config, "macros", log));
            global_context->setExternalAuthenticatorsConfig(*config);

            global_context->loadOrReloadDictionaries(*config);
            global_context->loadOrReloadModels(*config);

            /// proton: starts, FIXME, kc switch to metastore
            /// global_context->loadOrReloadUserDefinedExecutableFunctions();
            /// proton: ends

            global_context->setRemoteHostFilter(*config);

            global_context->setMaxTableSizeToDrop(new_server_settings.max_table_size_to_drop);
            global_context->setMaxPartitionSizeToDrop(new_server_settings.max_partition_size_to_drop);
            /// Setup protection to avoid accidental DROP for big tables (that are greater than 50 GB by default)
            if (config->has("max_stream_size_to_drop"))
                global_context->setMaxTableSizeToDrop(config->getUInt64("max_stream_size_to_drop"));

            if (config->has("max_partition_size_to_drop"))
                global_context->setMaxPartitionSizeToDrop(config->getUInt64("max_partition_size_to_drop"));

            global_context->getProcessList().setMaxSize(new_server_settings.max_concurrent_queries);
            global_context->getProcessList().setMaxInsertQueriesAmount(new_server_settings.max_concurrent_insert_queries);
            global_context->getProcessList().setMaxSelectQueriesAmount(new_server_settings.max_concurrent_select_queries);

            if (config->has("resources"))
            {
                global_context->getResourceManager()->updateConfiguration(*config);
            }

            global_context->updateStorageConfiguration(*config);
            global_context->updateInterserverCredentials(*config);

            CompressionCodecEncrypted::Configuration::instance().tryLoad(*config, "encryption_codecs");

            NamedCollectionFactory::instance().reloadFromConfig(*config);
            ProfileEvents::increment(ProfileEvents::MainConfigLoads);
        },
        /* already_loaded = */ false);  /// Reload it right now (initial loading)

    /// Initialize access storages.
    auto access_control = global_context->getAccessControl();
    try
    {
        access_control->setUpFromMainConfig(config(), config_path);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Caught exception while setting up access control.");
        throw;
    }

    /// Reload config in SYSTEM RELOAD CONFIG query.
    global_context->setConfigReloadCallback([&]()
    {
        main_config_reloader->reload();
        access_control->reload();
    });

    /// Limit on total number of concurrently executed queries.
    global_context->getProcessList().setMaxSize(server_settings.max_concurrent_queries);

    /// Set up caches.

    size_t max_cache_size = static_cast<size_t>(memory_amount * server_settings.cache_size_to_ram_max_ratio);

    /// Size of cache for uncompressed blocks. Zero means disabled.
    size_t uncompressed_cache_size = server_settings.uncompressed_cache_size;
    if (uncompressed_cache_size > max_cache_size)
    {
        uncompressed_cache_size = max_cache_size;
        LOG_INFO(log, "Uncompressed cache size was lowered to {} because the system has low amount of memory",
            formatReadableSizeWithBinarySuffix(uncompressed_cache_size));
    }
    global_context->setUncompressedCache(uncompressed_cache_size);

    /// Load global settings from default_profile and system_profile.
    global_context->setDefaultProfiles(config());
    const Settings & settings = global_context->getSettingsRef();

    /// Initialize background executors after we load default_profile config.
    /// This is needed to load proper values of background_pool_size etc.
    global_context->initializeBackgroundExecutorsIfNeeded();

    if (settings.async_insert_threads)
        global_context->setAsynchronousInsertQueue(std::make_shared<AsynchronousInsertQueue>(
            global_context,
            settings.async_insert_threads));

#if USE_PYTHON_UDF
    /// Initialize AsyncPythonPackageManager for background Python package operations
    global_context->setAsyncPythonPackageManager(std::make_shared<DB::cpython::AsyncPythonPackageManager>(global_context));
#endif

    /// Size of cache for marks (index of MergeTree family of tables). It is mandatory.
    size_t mark_cache_size = server_settings.mark_cache_size;
    if (!mark_cache_size)
        LOG_ERROR(log, "Too low mark cache size will lead to severe performance degradation.");
    if (mark_cache_size > max_cache_size)
    {
        mark_cache_size = max_cache_size;
        LOG_INFO(log, "Mark cache size was lowered to {} because the system has low amount of memory",
            formatReadableSizeWithBinarySuffix(mark_cache_size));
    }
    global_context->setMarkCache(mark_cache_size);

    if (server_settings.index_uncompressed_cache_size)
        global_context->setIndexUncompressedCache(server_settings.index_uncompressed_cache_size);

    if (server_settings.index_mark_cache_size)
        global_context->setIndexMarkCache(server_settings.index_mark_cache_size);

    if (server_settings.mmap_cache_size)
        global_context->setMMappedFileCache(server_settings.mmap_cache_size);

#if USE_EMBEDDED_COMPILER
    /// 128 MB
    constexpr size_t compiled_expression_cache_size_default = 1024 * 1024 * 128;
    size_t compiled_expression_cache_size = config().getUInt64("compiled_expression_cache_size", compiled_expression_cache_size_default);

    constexpr size_t compiled_expression_cache_elements_size_default = 10000;
    size_t compiled_expression_cache_elements_size = config().getUInt64("compiled_expression_cache_elements_size", compiled_expression_cache_elements_size_default);

    CompiledExpressionCacheFactory::instance().init(compiled_expression_cache_size, compiled_expression_cache_elements_size);
#endif

    /// Set path for format schema files
    fs::path format_schema_path(config().getString("format_schema_path", path / "format_schemas/"));
    global_context->setFormatSchemaPath(format_schema_path);
    fs::create_directories(format_schema_path);

    /// Set the path for google proto files
    if (config().has("google_protos_path"))
        global_context->setGoogleProtosPath(fs::weakly_canonical(config().getString("google_protos_path")));

    /// Check sanity of MergeTreeSettings on server startup
    /// proton: starts. replace 'merge tree' to stream settings
    global_context->getStreamSettings().sanityCheck(settings);

    /// try set up encryption. There are some errors in config, error will be printed and server wouldn't start.
    CompressionCodecEncrypted::Configuration::instance().load(config(), "encryption_codecs");

    /// NOTE: global context should be destroyed *before* GlobalThreadPool::shutdown()
    /// Otherwise GlobalThreadPool::shutdown() will hang, since Context holds some threads.
    SCOPE_EXIT({
        async_metrics.stop();

        /** Ask to cancel background jobs all table engines,
          *  and also query_log.
          * It is important to do early, not in destructor of Context, because
          *  table engines could use Context on destroy.
          */
        LOG_INFO(log, "Shutting down storages.");

        /// proton starts : we like to shutdown ExternalGrokPatterns here explicitly since it depends on
        /// ScheduleThreadPool in `global_context` which will be deleted after calling `global_context->shutdown()`
        /// which causes use after free segfault when ExternalGrokPatterns/PlacementService tries to deactivate its task from the
        /// ScheduleThreadPool which doesn't exist any more
        TelemetryCollector::instance(global_context).shutdown();

        global_context->shutdown();

        /// proton: start.
        finalizePythonInterpreter();

        /// Materialized View shutdown operation depends on V8,
        /// we should dispose V8 after shutdown DatabaseCatelog
        maybeDisposeV8();
        /// proton: end.

        LOG_DEBUG(log, "Shut down storages.");

        if (!servers_to_start_before_tables.empty())
        {
            LOG_DEBUG(log, "Waiting for current connections to servers for tables to finish.");
            size_t current_connections = 0;
            {
                std::lock_guard lock(servers_lock);
                for (auto & server : servers_to_start_before_tables)
                {
                    server.stop();
                    current_connections += server.currentConnections();
                }
            }

            if (current_connections)
                LOG_INFO(log, "Closed all listening sockets. Waiting for {} outstanding connections.", current_connections);
            else
                LOG_INFO(log, "Closed all listening sockets.");

            if (current_connections > 0)
                current_connections = waitServersToFinish(servers_to_start_before_tables, servers_lock, config().getInt("shutdown_wait_unfinished", 5));

            if (current_connections)
                LOG_INFO(log, "Closed connections to servers for tables. But {} remain. Probably some tables of other users cannot finish their connections after context shutdown.", current_connections);
            else
                LOG_INFO(log, "Closed connections to servers for tables.");
        }

        /// Wait server pool to avoid use-after-free of destroyed context in the handlers
        server_pool.joinAll();

        /** Explicitly destroy Context. It is more convenient than in destructor of Server, because logger is still available.
          * At this moment, no one could own shared part of Context.
          */
        global_context.reset();
        shared_context.reset();
        LOG_DEBUG(log, "Destroyed global context.");
    });

    /// DNSCacheUpdater uses BackgroundSchedulePool which lives in shared context
    /// and thus this object must be created after the SCOPE_EXIT object where shared
    /// context is destroyed.
    /// In addition this object has to be created before the loading of the tables.
    std::unique_ptr<DNSCacheUpdater> dns_cache_updater;
    if (server_settings.disable_internal_dns_cache)
    {
        /// Disable DNS caching at all
        DNSResolver::instance().setDisableCacheFlag();
        LOG_DEBUG(log, "DNS caching disabled");
    }
    else
    {
        /// Initialize a watcher periodically updating DNS cache
        dns_cache_updater = std::make_unique<DNSCacheUpdater>(global_context, server_settings.dns_cache_update_period);
    }

    if (dns_cache_updater)
        dns_cache_updater->start();

    /// Set current database name before loading tables and databases because
    /// system logs may copy global context.
    global_context->setCurrentDatabaseNameInGlobalContext(default_database);

    /// proton: start.
    /// initialize v8 engine
    /// Setup config path since API Spec handler need access it
    global_context->setConfigPath(config_path);

    initPythonInterpreter();

    initV8();
    /// proton: end.

    LOG_INFO(log, "Loading metadata from {}", path_str);

    try
    {
        auto & database_catalog = DatabaseCatalog::instance();
        /// We load temporary database first, because projections need it.
        database_catalog.initializeAndLoadTemporaryDatabase();
        loadMetadataSystem(global_context);
        /// After attaching system databases we can initialize system log.
        global_context->initializeSystemLogs();
        /// After the system database is created, attach virtual system tables (in addition to query_log and part_log)
        attachSystemTablesServer(global_context, *database_catalog.getSystemDatabase());
        attachInformationSchema(global_context, *database_catalog.getDatabase(DatabaseCatalog::INFORMATION_SCHEMA));
        attachInformationSchema(global_context, *database_catalog.getDatabase(DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE));
        /// Firstly remove partially dropped databases, to avoid race with MaterializedMySQLSyncThread,
        /// that may execute DROP before loadMarkedAsDroppedTables() in background,
        /// and so loadMarkedAsDroppedTables() will find it and try to add, and UUID will overlap.
        database_catalog.loadMarkedAsDroppedTables();
        /// Then, load remaining databases
        loadMetadata(global_context, builtin_databases);
        startupSystemTables();
        database_catalog.loadDatabases();
        /// After loading validate that default database exists
        database_catalog.assertDatabaseExists(default_database);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Caught exception while loading metadata");
        throw;
    }
    LOG_DEBUG(log, "Loaded metadata.");

    /// proton : starts.
    auto & telemetry_collector = DB::TelemetryCollector::instance(global_context);
    telemetry_collector.startup();

    std::optional<BuiltinSchemasProvisioner> builtin_schemas_provisioner;
    builtin_schemas_provisioner.emplace(global_context, config());

    std::shared_ptr<Task::TaskScheduler> task_scheduler;
    {
        auto task_worker_threads = config().getUInt64("task.worker_threads", 0);
        if (task_worker_threads == 0)
            task_worker_threads = std::min<size_t>(getNumberOfPhysicalCPUCores() * 2, 16);
        const UInt64 max_scheduled_tasks = config().getUInt64("task.max_scheduled_tasks", 1024);
        task_scheduler = std::make_shared<Task::TaskScheduler>(task_worker_threads, max_scheduled_tasks);
        task_scheduler->startup();
        global_context->setTaskScheduler(task_scheduler);
    }

    /// Init trace collector only after trace_log system table was created
    /// Disable it if we collect test coverage information, because it will work extremely slow.
#if !WITH_COVERAGE
    /// Profilers cannot work reliably with any other libunwind or without PHDR cache.
    if (hasPHDRCache())
    {
        global_context->initializeTraceCollector();

        /// Set up server-wide memory profiler (for total memory tracker).
        UInt64 total_memory_profiler_step = config().getUInt64("total_memory_profiler_step", 0);
        if (total_memory_profiler_step)
        {
            total_memory_tracker.setProfilerStep(total_memory_profiler_step);
        }

        double total_memory_tracker_sample_probability = config().getDouble("total_memory_tracker_sample_probability", 0);
        if (total_memory_tracker_sample_probability > 0.0)
        {
            total_memory_tracker.setSampleProbability(total_memory_tracker_sample_probability);
        }
    }
#endif

    /// Describe multiple reasons when query profiler cannot work.

#if WITH_COVERAGE
    LOG_INFO(log, "Query Profiler and TraceCollector are disabled because they work extremely slow with test coverage.");
#endif

#if defined(SANITIZER)
    LOG_INFO(log, "Query Profiler disabled because they cannot work under sanitizers"
        " when two different stack unwinding methods will interfere with each other.");
#endif

#if !defined(__x86_64__)
    LOG_INFO(log, "Query Profiler is only tested on x86_64. It also known to not work under qemu-user.");
#endif

    if (!hasPHDRCache())
        LOG_INFO(log, "Query Profiler and TraceCollector are disabled because they require PHDR cache to be created"
            " (otherwise the function 'dl_iterate_phdr' is not lock free and not async-signal safe).");

#if defined(OS_LINUX)
    if (!TasksStatsCounters::checkIfAvailable())
    {
        LOG_INFO(log, "It looks like this system does not have procfs mounted at /proc location,"
            " neither proton-server process has CAP_NET_ADMIN capability."
            " 'taskstats' performance statistics will be disabled."
            " It could happen due to incorrect proton package installation."
            " You can try to resolve the problem manually with 'sudo setcap cap_net_admin=+ep {}'."
            " Note that it will not work on 'nosuid' mounted filesystems."
            " It also doesn't work if you run proton-server inside network namespace as it happens in some containers.",
            executable_path);
    }

    if (!hasLinuxCapability(CAP_SYS_NICE))
    {
        LOG_INFO(log, "It looks like the process has no CAP_SYS_NICE capability, the setting 'os_thread_priority' will have no effect."
            " It could happen due to incorrect proton package installation."
            " You could resolve the problem manually with 'sudo setcap cap_sys_nice=+ep {}'."
            " Note that it will not work on 'nosuid' mounted filesystems.",
            executable_path);
    }
#else
    LOG_INFO(log, "TaskStats is not implemented for this OS. IO accounting will be disabled.");
#endif

    {
        attachSystemTablesAsync(global_context, *DatabaseCatalog::instance().getSystemDatabase(), async_metrics);

        {
            std::lock_guard lock(servers_lock);
            createServers(config(), server_descriptor, server_pool, async_metrics, servers);
            if (servers.empty())
                throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
                                "No servers started (add valid listen_host and 'tcp_port' or 'http_port' "
                                "to configuration file.)");
        }

        async_metrics.start();

        {
            String level_str = config().getString("text_log.level", "");
            int level = level_str.empty() ? INT_MAX : Poco::Logger::parseLevel(level_str);
            setTextLog(global_context->getTextLog(), level);
        }

        buildLoggers(config(), logger());

        main_config_reloader->start();
        access_control->startPeriodicReloading();

        /// try to load dictionaries immediately, throw on error and die
        try
        {
            global_context->loadOrReloadDictionaries(config());
        }
        catch (...)
        {
            tryLogCurrentException(log, "Caught exception while loading dictionaries.");
            throw;
        }

        /// try to load embedded dictionaries immediately, throw on error and die
        try
        {
            global_context->tryCreateEmbeddedDictionaries(config());
        }
        catch (...)
        {
            tryLogCurrentException(log, "Caught exception while loading embedded dictionaries.");
            throw;
        }

        /// try to load models immediately, throw on error and die
        try
        {
            global_context->loadOrReloadModels(config());
        }
        catch (...)
        {
            tryLogCurrentException(log, "Caught exception while loading dictionaries.");
            throw;
        }

        {
            std::lock_guard lock(servers_lock);
            for (auto & server : servers)
            {
                server.start();
                LOG_INFO(log, "Listening for {}", server.getDescription());
            }

            global_context->setServerCompletelyStarted();
            LOG_INFO(log, "Ready for connections.");
        }

        SCOPE_EXIT_SAFE({
            LOG_INFO(log, "Received termination signal.");

            /// Stop reloading of the main config. This must be done before everything else because it
            /// can try to access/modify already deleted objects.
            /// E.g. it can recreate new servers or it may pass a changed config to some destroyed parts of ContextSharedPart.
            main_config_reloader.reset();
            access_control->stopPeriodicReloading();

            /// proton : starts
            /// Trigger last checkpoint and flush before offline
            Globals::getCheckpointCoordinator().triggerLastCheckpointAndFlush();
            /// proton : ends

            is_cancelled = true;

            LOG_INFO(log, "Waiting for current connections to close.");

            size_t current_connections = 0;
            {
                std::lock_guard lock(servers_lock);
                for (auto & server : servers)
                {
                    server.stop();
                    current_connections += server.currentConnections();
                }
            }

            if (current_connections)
                LOG_INFO(log, "Closed all listening sockets. Waiting for {} outstanding connections.", current_connections);
            else
                LOG_INFO(log, "Closed all listening sockets.");

            /// Killing remaining queries.
            if (server_settings.shutdown_wait_unfinished_queries)
                global_context->getProcessList().killAllQueries();

            if (current_connections)
                current_connections = waitServersToFinish(servers, servers_lock, config().getInt("shutdown_wait_unfinished", 5));

            if (current_connections)
                LOG_INFO(log, "Closed connections. But {} remain."
                    " Tip: To increase wait time add to config: <shutdown_wait_unfinished>60</shutdown_wait_unfinished>", current_connections);
            else
                LOG_INFO(log, "Closed connections.");

            dns_cache_updater.reset();

            if (current_connections)
            {
                /// There is no better way to force connections to close in Poco.
                /// Otherwise connection handlers will continue to live
                /// (they are effectively dangling objects, but they use global thread pool
                ///  and global thread pool destructor will wait for threads, preventing server shutdown).

                /// Dump coverage here, because std::atexit callback would not be called.
                dumpCoverageReportIfPossible();
                LOG_INFO(log, "Will shutdown forcefully.");
                safeExit(0);
            }
        });

        std::vector<std::unique_ptr<MetricsTransmitter>> metrics_transmitters;
        for (const auto & graphite_key : DB::getMultipleKeysFromConfig(config(), "", "graphite"))
        {
            metrics_transmitters.emplace_back(std::make_unique<MetricsTransmitter>(
                global_context->getConfigRef(), graphite_key, async_metrics));
        }

        waitForTerminationRequest();
    }

    return Application::EXIT_OK;
}
catch (...)
{
    /// Poco does not provide stacktrace.
    tryLogCurrentException("Application");
    auto code = getCurrentExceptionCode();
    return static_cast<UInt8>(code) ? code : -1;
}

/// Public facing servers
void Server::createServers(
    Poco::Util::AbstractConfiguration & config,
    ServerDescriptorPtr server_descriptor,
    Poco::ThreadPool & server_pool,
    AsynchronousMetrics & async_metrics,
    std::vector<ProtocolServerAdapter> & servers,
    bool start_servers)
{
    const Settings & settings = global_context->getSettingsRef();

    Poco::Timespan keep_alive_timeout(config.getUInt("keep_alive_timeout", 10), 0);
    Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
    http_params->setTimeout(settings.http_receive_timeout);
    http_params->setKeepAliveTimeout(keep_alive_timeout);
    std::vector<std::string> listen_hosts;
    bool listen_try = false;

    if (Poco::Net::Socket::supportsIPv6() && networkInterfaceSupportsIPv6())
        listen_hosts.emplace_back("::1");
    listen_hosts.emplace_back("0.0.0.0");
    listen_try = true;

    /// HTTP/HTTPS
    if (server_descriptor->http_port.isValid())
    {
        for (const auto & listen_host : listen_hosts)
        {
            const char * port_name = server_descriptor->http_port.is_tls ? "https.port" : "http.port";
            createServer(
                config,
                listen_host,
                port_name,
                server_descriptor->http_port.port,
                listen_try,
                start_servers,
                servers,
                [&](UInt16 port) -> ProtocolServerAdapter {
                    if (server_descriptor->http_port.is_tls)
                    {
#if USE_SSL
                        Poco::Net::SecureServerSocket socket;
                        auto address = socketBindListen(socket, listen_host, port, /* secure = */ true);
                        socket.setReceiveTimeout(settings.http_receive_timeout);
                        socket.setSendTimeout(settings.http_send_timeout);
                        return ProtocolServerAdapter(
                            listen_host,
                            port_name,
                            "https://" + address.toString(),
                            std::make_unique<HTTPServer>(
                                context(),
                                createHandlerFactory(*this, async_metrics, "HTTPSHandler-factory"),
                                server_pool,
                                socket,
                                http_params));
#else
                        UNUSED(port);
                        throw Exception{
                            "HTTPS protocol is disabled because Poco library was built without NetSSL support.",
                            ErrorCodes::SUPPORT_IS_DISABLED};
#endif
                    }
                    else
                    {
                        Poco::Net::ServerSocket socket;
                        auto address = socketBindListen(socket, listen_host, port);
                        socket.setReceiveTimeout(settings.http_receive_timeout);
                        socket.setSendTimeout(settings.http_send_timeout);
                        return ProtocolServerAdapter(
                            listen_host,
                            port_name,
                            "http://" + address.toString(),
                            std::make_unique<HTTPServer>(
                                context(),
                                createHandlerFactory(*this, async_metrics, "HTTPHandler-factory"),
                                server_pool,
                                socket,
                                http_params));
                    }
                });
        }
    }

    /// TCP/TCP Secure
    if (server_descriptor->tcp_port.isValid())
    {
        for (const auto & listen_host : listen_hosts)
        {
            const char * port_name = server_descriptor->tcp_port.is_tls ? "tcp_secure.port" : "tcp.port";
            createServer(
                config,
                listen_host,
                port_name,
                server_descriptor->tcp_port.port,
                listen_try,
                start_servers,
                servers,
                [&](UInt16 port) -> ProtocolServerAdapter {
                    if (server_descriptor->tcp_port.is_tls)
                    {
#if USE_SSL
                        Poco::Net::SecureServerSocket socket;
                        auto address = socketBindListen(socket, listen_host, port, /* secure = */ true);
                        socket.setReceiveTimeout(settings.receive_timeout);
                        socket.setSendTimeout(settings.send_timeout);
                        return ProtocolServerAdapter(
                            listen_host,
                            port_name,
                            "native protocol (tcp_secure): " + address.toString(),
                            std::make_unique<TCPServer>(
                                new TCPHandlerFactory(*this, /* secure */ true, /* proxy protocol */ false),
                                server_pool,
                                socket,
                                new Poco::Net::TCPServerParams));
#else
                        UNUSED(port);
                        throw Exception{
                            "SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.",
                            ErrorCodes::SUPPORT_IS_DISABLED};
#endif
                    }
                    else
                    {
                        Poco::Net::ServerSocket socket;
                        auto address = socketBindListen(socket, listen_host, port);
                        socket.setReceiveTimeout(settings.receive_timeout);
                        socket.setSendTimeout(settings.send_timeout);
                        return ProtocolServerAdapter(
                            listen_host,
                            port_name,
                            "native protocol (tcp): " + address.toString(),
                            std::make_unique<TCPServer>(
                                new TCPHandlerFactory(*this, /* secure */ false, /* proxy protocol */ false),
                                server_pool,
                                socket,
                                new Poco::Net::TCPServerParams));
                    }
                });
        }
    }

#if 0
    {
        for (const auto & listen_host : listen_hosts)
        {
            /// TCP with PROXY protocol, see https://github.com/wolfeidau/proxyv2/blob/master/docs/proxy-protocol.txt
            const char * port_name = "tcp_with_proxy_port";
            createServer(config, listen_host, port_name, listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
            {
                Poco::Net::ServerSocket socket;
                auto address = socketBindListen(socket, listen_host, port);
                socket.setReceiveTimeout(settings.receive_timeout);
                socket.setSendTimeout(settings.send_timeout);
                return ProtocolServerAdapter(
                    listen_host,
                    port_name,
                    "native protocol (tcp) with PROXY: " + address.toString(),
                    std::make_unique<TCPServer>(
                        new TCPHandlerFactory(*this, /* secure */ false, /* proxy protocol */ true),
                        server_pool,
                        socket,
                        new Poco::Net::TCPServerParams));
            });
        }
    }
#endif

    /// Table TCP/Table TCP Secure
    if (server_descriptor->table_tcp_port.isValid())
    {
        for (const auto & listen_host : listen_hosts)
        {
            const char * port_name = server_descriptor->table_tcp_port.is_tls ? "table_tcp_secure.port" : "table_tcp.port";
            createServer(
                config,
                listen_host,
                port_name,
                server_descriptor->table_tcp_port.port,
                listen_try,
                start_servers,
                servers,
                [&](UInt16 port) -> ProtocolServerAdapter {
                    if (server_descriptor->table_tcp_port.is_tls)
                    {
#if USE_SSL
                        Poco::Net::SecureServerSocket socket;
                        auto address = socketBindListen(socket, listen_host, port, /* secure = */ true);
                        socket.setReceiveTimeout(settings.receive_timeout);
                        socket.setSendTimeout(settings.send_timeout);
                        return ProtocolServerAdapter(
                            listen_host,
                            port_name,
                            "snapshot server (tcp_secure): " + address.toString(),
                            std::make_unique<TCPServer>(
                                new TCPHandlerFactory(*this, /* secure */ true, /* proxy protocol */ false, /* snapshot mode */ true),
                                server_pool,
                                socket,
                                new Poco::Net::TCPServerParams));
#else
                        UNUSED(port);
                        throw Exception{
                            "SSL support for table TCP protocol is disabled because Poco library was built without NetSSL support.",
                            ErrorCodes::SUPPORT_IS_DISABLED};
#endif
                    }
                    else
                    {
                        Poco::Net::ServerSocket socket;
                        auto address = socketBindListen(socket, listen_host, port);
                        socket.setReceiveTimeout(settings.receive_timeout);
                        socket.setSendTimeout(settings.send_timeout);
                        return ProtocolServerAdapter(
                            listen_host,
                            port_name,
                            "snapshot server (tcp): " + address.toString(),
                            std::make_unique<TCPServer>(
                                new TCPHandlerFactory(*this, /* secure */ false, /* proxy protocol */ false, /* snapshot mode */ true),
                                server_pool,
                                socket,
                                new Poco::Net::TCPServerParams));
                    }
                });
        }
    }

    /// PostgreSQL protocol
    if (server_descriptor->postgresql_port.isValid())
    {
        for (const auto & listen_host : listen_hosts)
        {
            const char * port_name = "postgresql.port";
            createServer(
                config,
                listen_host,
                port_name,
                server_descriptor->postgresql_port.port,
                listen_try,
                start_servers,
                servers,
                [&](UInt16 port) -> ProtocolServerAdapter {
                    Poco::Net::ServerSocket socket;
                    auto address = socketBindListen(socket, listen_host, port);
                    socket.setReceiveTimeout(Poco::Timespan());
                    socket.setSendTimeout(settings.send_timeout);
                    return ProtocolServerAdapter(
                        listen_host,
                        port_name,
                        "PostgreSQL compatibility protocol: " + address.toString(),
                        std::make_unique<TCPServer>(
                            new PostgreSQLHandlerFactory(*this), server_pool, socket, new Poco::Net::TCPServerParams));
                });
        }
    }

    /// Table HTTP/HTTPS
    if (server_descriptor->table_http_port.isValid())
    {
        for (const auto & listen_host : listen_hosts)
        {
            const char * port_name = server_descriptor->table_http_port.is_tls ? "table_https.port" : "table_http.port";
            createServer(
                config,
                listen_host,
                port_name,
                server_descriptor->table_http_port.port,
                listen_try,
                start_servers,
                servers,
                [&](UInt16 port) -> ProtocolServerAdapter {
                    if (server_descriptor->table_http_port.is_tls)
                    {
#if USE_SSL
                        Poco::Net::SecureServerSocket socket;
                        auto address = socketBindListen(socket, listen_host, port, /* secure = */ true);
                        socket.setReceiveTimeout(settings.http_receive_timeout);
                        socket.setSendTimeout(settings.http_send_timeout);
                        return ProtocolServerAdapter(
                            listen_host,
                            port_name,
                            "https://" + address.toString(),
                            std::make_unique<HTTPServer>(
                                context(),
                                createHandlerFactory(*this, async_metrics, "SnapshotHTTPSHandler-factory"),
                                server_pool,
                                socket,
                                http_params));
#else
                        UNUSED(port);
                        throw Exception{
                            "HTTPS protocol for table_http is disabled because Poco library was built without NetSSL support.",
                            ErrorCodes::SUPPORT_IS_DISABLED};
#endif
                    }
                    else
                    {
                        Poco::Net::ServerSocket socket;
                        auto address = socketBindListen(socket, listen_host, port);
                        socket.setReceiveTimeout(settings.http_receive_timeout);
                        socket.setSendTimeout(settings.http_send_timeout);
                        return ProtocolServerAdapter(
                            listen_host,
                            port_name,
                            "http://" + address.toString(),
                            std::make_unique<HTTPServer>(
                                context(),
                                createHandlerFactory(*this, async_metrics, "SnapshotHTTPHandler-factory"),
                                server_pool,
                                socket,
                                http_params));
                    }
                });
        }
    }

    const char * port_name = "grpc.port";
    if (config.has(port_name) && config.getBool("grpc.enabled", false))
    {
        [[maybe_unused]] UInt16 port = config.getUInt(port_name);

#if USE_GRPC
        for (const auto & listen_host : listen_hosts)
        {

            createServer(config, listen_host, port_name, port, listen_try, start_servers, servers, [&](UInt16 port_) -> ProtocolServerAdapter {
                Poco::Net::SocketAddress server_address(listen_host, port);
                return ProtocolServerAdapter(
                    listen_host,
                    port_name,
                    "gRPC protocol: " + server_address.toString(),
                    std::make_unique<GRPCServer>(*this, makeSocketAddress(listen_host, port_, &logger())));
            });
        }
#endif
    }

    port_name = "prometheus.port";
    if (config.has(port_name))
    {
        for (const auto & listen_host : listen_hosts)
        {
            UInt16 port = config.getUInt(port_name);
            /// Prometheus (if defined and not setup yet with http_port)
            createServer(config, listen_host, port_name, port, listen_try, start_servers, servers, [&](UInt16 port_) -> ProtocolServerAdapter
            {
                Poco::Net::ServerSocket socket;
                auto address = socketBindListen(socket, listen_host, port_);
                socket.setReceiveTimeout(settings.http_receive_timeout);
                socket.setSendTimeout(settings.http_send_timeout);
                return ProtocolServerAdapter(
                    listen_host,
                    port_name,
                    "Prometheus: http://" + address.toString(),
                    std::make_unique<HTTPServer>(
                        context(), createHandlerFactory(*this, async_metrics, "PrometheusHandler-factory"), server_pool, socket, http_params));
            });
        }
    }
}
}
