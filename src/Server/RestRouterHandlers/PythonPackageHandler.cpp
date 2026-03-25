#include <Interpreters/executeSelectQuery.h>
#include <Server/RestRouterHandlers/PythonPackageHandler.h>

#if USE_PYTHON_UDF
#include <CPython/PythonPackage.h>

#include <boost/algorithm/string/replace.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_REQUEST_PARAMETER;
extern const int UNKNOWN_EXCEPTION;
extern const int BAD_ARGUMENTS;
extern const int ACCESS_DENIED;
extern const int RESOURCE_NOT_INITED;
extern const int TIMEOUT_EXCEEDED;
}

namespace
{
String buildResponse(const Poco::JSON::Array::Ptr & object, const String & query_id)
{
    Poco::JSON::Object obj;
    obj.set("request_id", query_id);
    obj.set("data", object);

    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    obj.stringify(resp_str_stream, 0);
    return resp_str_stream.str();
}

String buildSuccessResponse(const String & message, const String & query_id)
{
    Poco::JSON::Object obj;
    obj.set("request_id", query_id);
    obj.set("success", true);
    obj.set("message", message);

    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    obj.stringify(resp_str_stream, 0);
    return resp_str_stream.str();
}

String escapeSingleQuoted(const String & input)
{
    String escaped = input;
    boost::replace_all(escaped, "'", "''");
    return escaped;
}

std::vector<String> parseExtraIndexUrls(const Poco::JSON::Object::Ptr & payload)
{
    std::vector<String> extra_index_urls;
    if (!payload->has("extra_index_url"))
        return extra_index_urls;

    const auto value = payload->get("extra_index_url");
    if (value.isString())
    {
        extra_index_urls.push_back(value.toString());
        return extra_index_urls;
    }

    if (!value.isArray())
        throw Exception(ErrorCodes::BAD_REQUEST_PARAMETER, "'extra_index_url' must be a string or array of strings");

    auto array = value.extract<Poco::JSON::Array::Ptr>();
    for (size_t i = 0; i < array->size(); ++i)
    {
        const auto element = array->get(i);
        if (!element.isString())
            throw Exception(ErrorCodes::BAD_REQUEST_PARAMETER, "'extra_index_url' array must contain only strings");
        extra_index_urls.push_back(element.toString());
    }

    return extra_index_urls;
}

String buildInstallOptionsClause(const Poco::JSON::Object::Ptr & payload)
{
    String options_clause;

    if (payload->has("index_url"))
    {
        const auto index_url = payload->get("index_url");
        if (!index_url.isString())
            throw Exception(ErrorCodes::BAD_REQUEST_PARAMETER, "'index_url' must be a string");
        options_clause += fmt::format(" INDEX_URL '{}'", escapeSingleQuoted(index_url.toString()));
    }

    for (const auto & extra_index_url : parseExtraIndexUrls(payload))
        options_clause += fmt::format(" EXTRA_INDEX_URL '{}'", escapeSingleQuoted(extra_index_url));

    return options_clause;
}
}

bool PythonPackageHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    const bool has_name = payload->has("name");
    const bool has_packages = payload->has("packages");
    const bool has_requirements = payload->has("requirements");

    const size_t mode_count = static_cast<size_t>(has_name) + static_cast<size_t>(has_packages) + static_cast<size_t>(has_requirements);
    if (mode_count == 0)
    {
        error_msg = "One of 'name', 'packages', or 'requirements' is required";
        return false;
    }
    if (mode_count > 1)
    {
        error_msg = "Only one of 'name', 'packages', or 'requirements' can be provided";
        return false;
    }

    if (has_name)
    {
        if (!payload->get("name").isString())
        {
            error_msg = "'name' must be a string";
            return false;
        }
        if (payload->get("name").toString().empty())
        {
            error_msg = "Package name cannot be empty";
            return false;
        }
    }

    if (has_requirements)
    {
        if (!payload->get("requirements").isString())
        {
            error_msg = "'requirements' must be a string";
            return false;
        }
        if (payload->get("requirements").toString().empty())
        {
            error_msg = "Requirements text cannot be empty";
            return false;
        }
    }

    if (has_packages)
    {
        if (!payload->get("packages").isArray())
        {
            error_msg = "'packages' must be an array";
            return false;
        }

        auto packages = payload->getArray("packages");
        if (packages->size() == 0)
        {
            error_msg = "'packages' cannot be empty";
            return false;
        }

        for (size_t i = 0; i < packages->size(); ++i)
        {
            if (!packages->isObject(i))
            {
                error_msg = "'packages' array must contain objects";
                return false;
            }

            auto package_obj = packages->getObject(i);
            if (!package_obj->has("name") || !package_obj->get("name").isString() || package_obj->get("name").toString().empty())
            {
                error_msg = "Each package in 'packages' must have a non-empty string field 'name'";
                return false;
            }
        }
    }

    if (has_requirements && (payload->has("version") || payload->has("VERSION")))
    {
        error_msg = "'version' and 'VERSION' are only valid for single package install";
        return false;
    }

    if (has_packages && (payload->has("version") || payload->has("VERSION")))
    {
        error_msg = "Top-level 'version' and 'VERSION' are not allowed when 'packages' is provided";
        return false;
    }

    return true;
}

std::pair<String, Int32> PythonPackageHandler::executeGet(const Poco::JSON::Object::Ptr & /*payload*/) const
{
    try
    {
        auto packages = cpython::PythonPackage::list();

        Poco::JSON::Array::Ptr packages_array = new Poco::JSON::Array();

        for (const auto & package : packages)
        {
            Poco::JSON::Object::Ptr package_obj = new Poco::JSON::Object();
            package_obj->set("name", package.name);
            package_obj->set("version", package.version_spec);
            packages_array->add(package_obj);
        }

        return {buildResponse(packages_array, query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
    }
    catch (const Exception & e)
    {
        Int32 http_status;
        auto error_code = e.code();

        if (error_code == ErrorCodes::BAD_ARGUMENTS)
            http_status = HTTPResponse::HTTP_BAD_REQUEST;
        else if (error_code == ErrorCodes::ACCESS_DENIED)
            http_status = HTTPResponse::HTTP_FORBIDDEN;
        else if (error_code == ErrorCodes::RESOURCE_NOT_INITED)
            http_status = HTTPResponse::HTTP_SERVICE_UNAVAILABLE;
        else if (error_code == ErrorCodes::TIMEOUT_EXCEEDED)
            http_status = HTTPResponse::HTTP_REQUEST_TIMEOUT;
        else
            http_status = HTTPResponse::HTTP_INTERNAL_SERVER_ERROR;

        return {jsonErrorResponse(fmt::format("Failed to list Python packages: {}", e.message()), e.code()), http_status};
    }
    catch (const std::exception & e)
    {
        return {
            jsonErrorResponse(fmt::format("Failed to list Python packages: {}", e.what()), ErrorCodes::UNKNOWN_EXCEPTION),
            HTTPResponse::HTTP_INTERNAL_SERVER_ERROR};
    }
}

std::pair<String, Int32> PythonPackageHandler::executePost(const Poco::JSON::Object::Ptr & payload) const
{
    try
    {
        const String install_options_clause = buildInstallOptionsClause(payload);
        std::vector<String> install_queries;
        std::vector<String> install_targets;

        if (payload->has("requirements"))
        {
            String requirements_text = payload->get("requirements").toString();
            install_queries.push_back(fmt::format(
                "SYSTEM INSTALL PYTHON PACKAGE REQUIREMENTS '{}'{}", escapeSingleQuoted(requirements_text), install_options_clause));
            install_targets.push_back("requirements");
        }
        else if (payload->has("packages"))
        {
            auto packages = payload->getArray("packages");
            for (size_t i = 0; i < packages->size(); ++i)
            {
                auto package_obj = packages->getObject(i);
                String package_name = package_obj->get("name").toString();
                String package_version;

                if (package_obj->has("version") && package_obj->has("VERSION"))
                {
                    const auto lower = package_obj->get("version").toString();
                    const auto upper = package_obj->get("VERSION").toString();
                    if (lower != upper)
                    {
                        return {
                            jsonErrorResponse(
                                "Conflicting parameters: both 'version' and 'VERSION' are provided with different values in 'packages'",
                                ErrorCodes::BAD_REQUEST_PARAMETER),
                            HTTPResponse::HTTP_BAD_REQUEST};
                    }
                }

                if (package_obj->has("version"))
                    package_version = package_obj->get("version").toString();
                else if (package_obj->has("VERSION"))
                    package_version = package_obj->get("VERSION").toString();

                if (package_version.empty())
                {
                    install_queries.push_back(
                        fmt::format("SYSTEM INSTALL PYTHON PACKAGE '{}'{}", escapeSingleQuoted(package_name), install_options_clause));
                    install_targets.push_back(package_name);
                }
                else
                {
                    install_queries.push_back(fmt::format(
                        "SYSTEM INSTALL PYTHON PACKAGE '{}' '{}'{}",
                        escapeSingleQuoted(package_name),
                        escapeSingleQuoted(package_version),
                        install_options_clause));
                    install_targets.push_back(fmt::format("{}@{}", package_name, package_version));
                }
            }
        }
        else
        {
            String package_name = payload->get("name").toString();
            String package_version;

            if (payload->has("version") && payload->has("VERSION"))
            {
                const auto lower = payload->get("version").toString();
                const auto upper = payload->get("VERSION").toString();
                if (lower != upper)
                {
                    return {
                        jsonErrorResponse(
                            "Conflicting parameters: both 'version' and 'VERSION' are provided with different values",
                            ErrorCodes::BAD_REQUEST_PARAMETER),
                        HTTPResponse::HTTP_BAD_REQUEST};
                }
            }

            if (payload->has("version"))
                package_version = payload->get("version").toString();
            else if (payload->has("VERSION"))
                package_version = payload->get("VERSION").toString();

            if (package_version.empty())
            {
                install_queries.push_back(
                    fmt::format("SYSTEM INSTALL PYTHON PACKAGE '{}'{}", escapeSingleQuoted(package_name), install_options_clause));
                install_targets.push_back(package_name);
            }
            else
            {
                install_queries.push_back(fmt::format(
                    "SYSTEM INSTALL PYTHON PACKAGE '{}' '{}'{}",
                    escapeSingleQuoted(package_name),
                    escapeSingleQuoted(package_version),
                    install_options_clause));
                install_targets.push_back(fmt::format("{}@{}", package_name, package_version));
            }
        }

        for (const auto & query : install_queries)
        {
            LOG_DEBUG(log, "Executing Python package install query: {}", query);
            executeNonInsertQuery(query, query_context, /*callback=*/{}, /*internal=*/true);
        }

        String message;
        if (payload->has("requirements"))
            message = "Successfully installed Python requirements";
        else if (install_targets.size() == 1)
            message = fmt::format("Successfully installed Python package '{}'", install_targets.front());
        else
            message = fmt::format("Successfully installed {} Python packages", install_targets.size());

        return {buildSuccessResponse(message, query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
    }
    catch (const Exception & e)
    {
        Int32 http_status;
        auto error_code = e.code();

        if (error_code == ErrorCodes::BAD_ARGUMENTS || error_code == ErrorCodes::BAD_REQUEST_PARAMETER)
            http_status = HTTPResponse::HTTP_BAD_REQUEST;
        else if (error_code == ErrorCodes::ACCESS_DENIED)
            http_status = HTTPResponse::HTTP_FORBIDDEN;
        else if (error_code == ErrorCodes::RESOURCE_NOT_INITED)
            http_status = HTTPResponse::HTTP_SERVICE_UNAVAILABLE;
        else if (error_code == ErrorCodes::TIMEOUT_EXCEEDED)
            http_status = HTTPResponse::HTTP_REQUEST_TIMEOUT;
        else
            http_status = HTTPResponse::HTTP_INTERNAL_SERVER_ERROR;

        return {jsonErrorResponse(fmt::format("Failed to install Python package: {}", e.message()), e.code()), http_status};
    }
    catch (const std::exception & e)
    {
        return {
            jsonErrorResponse(fmt::format("Unexpected error: {}", e.what()), ErrorCodes::UNKNOWN_EXCEPTION),
            HTTPResponse::HTTP_INTERNAL_SERVER_ERROR};
    }
    catch (...)
    {
        return {jsonErrorResponse("Unknown error occurred", ErrorCodes::UNKNOWN_EXCEPTION), HTTPResponse::HTTP_INTERNAL_SERVER_ERROR};
    }
}

std::pair<String, Int32> PythonPackageHandler::executeDelete(const Poco::JSON::Object::Ptr & /*payload*/) const
{
    try
    {
        String package_name = getPathParameter("name", "");

        if (package_name.empty())
            return {jsonErrorResponse("Missing package name in path", ErrorCodes::BAD_REQUEST_PARAMETER), HTTPResponse::HTTP_BAD_REQUEST};

        String escaped_package_name = package_name;
        boost::replace_all(escaped_package_name, "'", "''");
        String query = fmt::format("SYSTEM UNINSTALL PYTHON PACKAGE '{}'", escaped_package_name);

        LOG_DEBUG(log, "Executing Python package uninstall query: {}", query);

        try
        {
            executeNonInsertQuery(query, query_context, /*callback=*/{}, /*internal=*/true);
        }
        catch (const Exception & e)
        {
            Int32 http_status;
            auto error_code = e.code();

            if (error_code == ErrorCodes::BAD_ARGUMENTS)
                http_status = HTTPResponse::HTTP_BAD_REQUEST;
            else if (error_code == ErrorCodes::ACCESS_DENIED)
                http_status = HTTPResponse::HTTP_FORBIDDEN;
            else if (error_code == ErrorCodes::RESOURCE_NOT_INITED)
                http_status = HTTPResponse::HTTP_SERVICE_UNAVAILABLE;
            else if (error_code == ErrorCodes::TIMEOUT_EXCEEDED)
                http_status = HTTPResponse::HTTP_REQUEST_TIMEOUT;
            else
                http_status = HTTPResponse::HTTP_INTERNAL_SERVER_ERROR;

            return {jsonErrorResponse(fmt::format("Failed to uninstall Python package: {}", e.message()), e.code()), http_status};
        }

        String message = fmt::format("Successfully uninstalled Python package '{}'", package_name);
        return {buildSuccessResponse(message, query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
    }
    catch (const std::exception & e)
    {
        return {
            jsonErrorResponse(fmt::format("Unexpected error: {}", e.what()), ErrorCodes::UNKNOWN_EXCEPTION),
            HTTPResponse::HTTP_INTERNAL_SERVER_ERROR};
    }
    catch (...)
    {
        return {jsonErrorResponse("Unknown error occurred", ErrorCodes::UNKNOWN_EXCEPTION), HTTPResponse::HTTP_INTERNAL_SERVER_ERROR};
    }
}
}

#endif
