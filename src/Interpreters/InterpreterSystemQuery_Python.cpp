#include <Interpreters/InterpreterSystemQuery.h>

#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/PipPythonPackageRequest.h>
#include <Columns/ColumnString.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSystemQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

#if USE_PYTHON_UDF
#include <CPython/AsyncPythonPackageManager.h>
#include <CPython/PythonPackage.h>
#include <Interpreters/EnsurePython.h>
#endif

#include <Common/re2.h>


/// When Python UDF is disabled, these functions always throw and never return.
/// Clang with -Wmissing-noreturn (and -Werror) requires marking such functions as [[noreturn]].
#if !USE_PYTHON_UDF
#define IF_NO_PY_UDF_NORETURN [[noreturn]]
#else
#define IF_NO_PY_UDF_NORETURN
#endif


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int UNKNOWN_EXCEPTION;
extern const int NOT_IMPLEMENTED;
}


IF_NO_PY_UDF_NORETURN void InterpreterSystemQuery::executeInstallPythonPackage([[maybe_unused]] const ASTSystemQuery & query)
{
#if !USE_PYTHON_UDF
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Python UDF support is not enabled. Rebuild with USE_PYTHON_UDF=1 to enable Python package management.");
#else

    cpython::PythonPackage::ensurePythonInterpreterReady();

    const bool install_from_requirements = !query.python_package_requirements_text.empty();
    if (install_from_requirements && !query.python_package_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid install syntax: package name and REQUIREMENTS text cannot be used together");

    if (!install_from_requirements && query.python_package_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Python package name cannot be empty");

    cpython::PipInstallOptions install_options;
    install_options.index_url = query.python_package_index_url;
    install_options.extra_index_urls.assign(query.python_package_extra_index_urls.begin(), query.python_package_extra_index_urls.end());

    if (!install_options.index_url.empty())
        cpython::PythonPackage::validatePipIndexUrl(install_options.index_url, "--index-url");
    for (const auto & extra_index_url : install_options.extra_index_urls)
        cpython::PythonPackage::validatePipIndexUrl(extra_index_url, "--extra-index-url");

    std::string package_with_version;
    if (!install_from_requirements)
    {
        cpython::PythonPackage::validatePackageSpecification(query.python_package_name);

        package_with_version = query.python_package_name;
        if (!query.python_package_version.empty())
        {
            static const re2::RE2 version_pattern(R"(^[a-zA-Z0-9\.\-_]+$)");
            if (!re2::RE2::FullMatch(query.python_package_version, version_pattern))
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Invalid version format '{}'. Versions must contain only letters, numbers, dots, hyphens, and underscores",
                    query.python_package_version);
            }
            package_with_version += "==" + query.python_package_version;
        }
    }

    auto base_timeout_sec = getContext()->getSettingsRef().query_timeout_sec;
    auto timeout_ms = std::max(30000UL, std::min(300000UL, static_cast<unsigned long>(base_timeout_sec * 1000))); /// 30s-5min bounds

    auto package = install_from_requirements
        ? cpython::PythonPackage::fromRequirementsText(query.python_package_requirements_text, install_options)
        : cpython::PythonPackage(package_with_version, install_options);

    /// Pre-validation on initiator node: fail fast to avoid unnecessary raft operations
    try
    {
        if (install_from_requirements)
        {
            LOG_INFO(
                log,
                "Starting Python requirements installation (validating): requirements_bytes={}, timeout={}ms",
                query.python_package_requirements_text.size(),
                timeout_ms);
        }
        else
        {
            LOG_INFO(
                log,
                "Starting Python package installation (validating): package='{}', version='{}', timeout={}ms",
                package.name,
                package.version_spec.empty() ? "latest" : package.version_spec,
                timeout_ms);
        }
        package.validateInstall(log);
    }
    catch (const Exception & e)
    {
        const auto install_target = install_from_requirements ? "requirements text" : package_with_version;
        throw Exception(e.code(), "Failed to install Python package '{}': {}", install_target, e.message());
    }
    catch (...)
    {
        const auto install_target = install_from_requirements ? "requirements text" : package_with_version;
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to install Python package '{}': Unknown error", install_target);
    }

    auto & metastore = Globals::getMetaStore();
    std::string task_id = toString(UUIDHelpers::generateV4());
    std::string task_id_for_logging = task_id;

    std::vector<std::string> package_names;
    if (!install_from_requirements)
        package_names = {package.name};

    std::vector<std::string> package_versions;
    if (!install_from_requirements && !package.version_spec.empty())
        package_versions = {package.version_spec};

    const uint16_t request_version
        = (install_from_requirements || !install_options.index_url.empty() || !install_options.extra_index_urls.empty()) ? 2 : 1;

    try
    {
        auto python_package_req = std::make_shared<cluster::PipPythonPackageRequest>(
            cluster::protocol::PipPythonPackageRequestData::Install,
            std::move(package_names),
            std::move(package_versions),
            install_from_requirements ? query.python_package_requirements_text : "",
            install_options.index_url,
            install_options.extra_index_urls,
            getContext()->getNodeID(),
            timeout_ms,
            request_version,
            std::move(task_id));

        if (install_from_requirements)
        {
            LOG_INFO(
                log,
                "Initiating cluster-wide Python requirements install: requirements_bytes={}, task_id='{}', timeout={}ms",
                query.python_package_requirements_text.size(),
                task_id_for_logging,
                timeout_ms);
        }
        else
        {
            LOG_INFO(
                log,
                "Initiating cluster-wide Python package install: package='{}', version='{}', task_id='{}', timeout={}ms",
                package.name,
                package.version_spec.empty() ? "latest" : package.version_spec,
                task_id_for_logging,
                timeout_ms);
        }

        auto system_command_resp = metastore.pythonPackageOperation(std::move(python_package_req));

        if (system_command_resp->hasError())
        {
            const auto & err = system_command_resp->error();
            const auto install_target = install_from_requirements ? "requirements text" : query.python_package_name;
            LOG_ERROR(
                log,
                "Cluster operation failed for Python package '{}': error_code={}, error_message={}",
                install_target,
                err.error_code,
                err.error_message);
            throw Exception(err.error_code, "Failed to install Python package '{}': {}", install_target, err.error_message);
        }

        if (install_from_requirements)
        {
            LOG_INFO(log, "Successfully completed Python requirements installation: task_id='{}'", task_id_for_logging);
        }
        else
        {
            LOG_INFO(
                log,
                "Successfully completed Python package installation: package='{}', version='{}', task_id='{}'",
                package.name,
                package.version_spec.empty() ? "latest" : package.version_spec,
                task_id_for_logging);
        }
    }
    catch (const Exception &)
    {
        throw;
    }
    catch (...)
    {
        const auto install_target = install_from_requirements ? "requirements text" : query.python_package_name;
        LOG_ERROR(log, "Cluster operation failed for Python package '{}': Unknown exception", install_target);
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to install Python package '{}': Unknown error", install_target);
    }
#endif
}

IF_NO_PY_UDF_NORETURN void InterpreterSystemQuery::executeUninstallPythonPackage([[maybe_unused]] const ASTSystemQuery & query)
{
#if !USE_PYTHON_UDF
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Python UDF support is not enabled. Rebuild with USE_PYTHON_UDF=1 to enable Python package management.");
#else

    cpython::PythonPackage::validatePackageSpecification(query.python_package_name);
    cpython::PythonPackage::ensurePythonInterpreterReady();

    auto base_timeout_sec = getContext()->getSettingsRef().query_timeout_sec;
    auto timeout_ms = std::max(30000UL, std::min(300000UL, static_cast<unsigned long>(base_timeout_sec * 1000))); /// 30s-5min bounds

    /// Pre-validation on initiator node: fail fast to avoid unnecessary raft operations
    try
    {
        auto package = cpython::PythonPackage(query.python_package_name);
        LOG_INFO(
            log, "Starting Python package uninstallation (validating): package='{}', timeout={}ms", query.python_package_name, timeout_ms);
        package.validateUninstall(log);
    }
    catch (const Exception & e)
    {
        throw Exception(e.code(), "Failed to uninstall Python package '{}': {}", query.python_package_name, e.message());
    }
    catch (...)
    {
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to uninstall Python package '{}': Unknown error", query.python_package_name);
    }

    auto & metastore = Globals::getMetaStore();
    std::string task_id = toString(UUIDHelpers::generateV4());
    std::string task_id_for_logging = task_id;

    std::vector<std::string> package_names = {query.python_package_name};
    std::vector<std::string> package_versions;

    try
    {
        auto python_package_req = std::make_shared<cluster::PipPythonPackageRequest>(
            cluster::protocol::PipPythonPackageRequestData::Uninstall,
            std::move(package_names),
            std::move(package_versions),
            "",
            "",
            std::vector<std::string>{},
            getContext()->getNodeID(),
            timeout_ms,
            /*request_version=*/1,
            std::move(task_id));

        LOG_INFO(
            log,
            "Initiating cluster-wide Python package uninstall: package='{}', task_id='{}', timeout={}ms",
            query.python_package_name,
            task_id_for_logging,
            timeout_ms);

        auto system_command_resp = metastore.pythonPackageOperation(std::move(python_package_req));

        if (system_command_resp->hasError())
        {
            const auto & err = system_command_resp->error();
            LOG_ERROR(
                log,
                "Cluster operation failed for Python package '{}': error_code={}, error_message={}",
                query.python_package_name,
                err.error_code,
                err.error_message);
            throw Exception(err.error_code, "Failed to uninstall Python package '{}': {}", query.python_package_name, err.error_message);
        }

        LOG_INFO(
            log,
            "Successfully completed Python package uninstallation: package='{}', task_id='{}'",
            query.python_package_name,
            task_id_for_logging);
    }
    catch (const Exception &)
    {
        throw;
    }
    catch (...)
    {
        LOG_ERROR(log, "Cluster operation failed for Python package '{}': Unknown exception", query.python_package_name);
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to uninstall Python package '{}': Unknown error", query.python_package_name);
    }
#endif
}

BlockIO InterpreterSystemQuery::executeListPythonPackages(const ASTSystemQuery &)
{
#if !USE_PYTHON_UDF
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Python UDF support is not enabled. Rebuild with USE_PYTHON_UDF=1 to enable Python package management.");
#else

    cpython::PythonPackage::ensurePythonInterpreterReady();

    MutableColumns result_columns = MutableColumns(2);
    result_columns[0] = ColumnString::create();
    result_columns[1] = ColumnString::create();

    try
    {
        auto packages = cpython::PythonPackage::list();
        if (packages.empty())
            LOG_INFO(log, "No Python packages currently installed");
        else
            std::sort(packages.begin(), packages.end(), [](const auto & a, const auto & b) { return a.name < b.name; });

        for (const auto & package : packages)
        {
            /// Extract version from version_spec (remove == prefix if present)
            std::string version = package.version_spec;
            if (version.starts_with("=="))
                version = version.substr(2);

            result_columns[0]->insert(package.name);
            result_columns[1]->insert(version);
        }
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "Failed to list Python packages: {}", e.message());
        throw Exception(e.code(), "Failed to list Python packages: {}", e.message());
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to list Python packages: Unknown exception");
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Failed to list Python packages: Unknown error");
    }

    Block result_block = Block{
        {std::move(result_columns[0]), std::make_shared<DataTypeString>(), "package_name"},
        {std::move(result_columns[1]), std::make_shared<DataTypeString>(), "version"}};

    BlockIO res;
    res.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::move(result_block)));
    return res;
#endif
}
}
