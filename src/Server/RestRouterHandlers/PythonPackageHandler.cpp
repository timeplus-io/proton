#include <Server/RestRouterHandlers/PythonPackageHandler.h>

#if USE_PYTHON_UDF
#include <CPython/PythonPackage.h>
#include <Interpreters/EnsurePython.h>
#include <Interpreters/executeSelectQuery.h>
#include <Server/RestRouterHandlers/SchemaValidator.h>

#include <boost/algorithm/string/replace.hpp>

namespace DB
{

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

std::map<String, std::map<String, String>> INSTALL_PYTHON_PACKAGE_SCHEMA
    = {{"required", {{"name", "string"}}}, {"optional", {{"version", "string"}, {"VERSION", "string"}}}};

std::map<String, std::map<String, String>> UNINSTALL_PYTHON_PACKAGE_SCHEMA = {{"required", {{"name", "string"}}}};
}

namespace ErrorCodes
{
extern const int BAD_REQUEST_PARAMETER;
extern const int UNKNOWN_EXCEPTION;
extern const int BAD_ARGUMENTS;
extern const int ACCESS_DENIED;
extern const int RESOURCE_NOT_INITED;
extern const int TIMEOUT_EXCEEDED;
}

bool PythonPackageHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    if (!validateSchema(INSTALL_PYTHON_PACKAGE_SCHEMA, payload, error_msg))
        return false;

    String package_name = payload->get("name").toString();

    if (package_name.empty())
    {
        error_msg = "Package name cannot be empty";
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
        if (!payload->has("name"))
        {
            return {jsonErrorResponse("Missing required field 'name'", ErrorCodes::BAD_REQUEST_PARAMETER), HTTPResponse::HTTP_BAD_REQUEST};
        }

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

        if (package_name.empty())
            return {jsonErrorResponse("Package name cannot be empty", ErrorCodes::BAD_REQUEST_PARAMETER), HTTPResponse::HTTP_BAD_REQUEST};

        /// Construct the SYSTEM INSTALL PYTHON PACKAGE query
        String query;
        if (package_version.empty())
        {
            String escaped_package_name = package_name;
            boost::replace_all(escaped_package_name, "'", "''");
            query = fmt::format("SYSTEM INSTALL PYTHON PACKAGE '{}'", escaped_package_name);
        }
        else
        {
            String escaped_package_name = package_name;
            String escaped_package_version = package_version;
            boost::replace_all(escaped_package_name, "'", "''");
            boost::replace_all(escaped_package_version, "'", "''");
            query = fmt::format("SYSTEM INSTALL PYTHON PACKAGE '{}' '{}'", escaped_package_name, escaped_package_version);
        }

        LOG_DEBUG(log, "Executing Python package install query: {}", query);

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

            return {jsonErrorResponse(fmt::format("Failed to install Python package: {}", e.message()), e.code()), http_status};
        }

        String message = fmt::format("Successfully installed Python package '{}'", package_name);
        if (!package_version.empty())
            message += fmt::format(" version '{}'", package_version);

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

std::pair<String, Int32> PythonPackageHandler::executeDelete(const Poco::JSON::Object::Ptr & /*payload*/) const
{
    try
    {
        /// Get package name from path parameter (not JSON payload)
        String package_name = getPathParameter("name", "");

        if (package_name.empty())
            return {jsonErrorResponse("Missing package name in path", ErrorCodes::BAD_REQUEST_PARAMETER), HTTPResponse::HTTP_BAD_REQUEST};

        /// Construct the SYSTEM UNINSTALL PYTHON PACKAGE query
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
