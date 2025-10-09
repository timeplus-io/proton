#include <Server/RestRouterHandlers/PythonStatusHandler.h>

#if USE_PYTHON_UDF
#include <Python.h>
#endif

#include <Poco/JSON/Object.h>

namespace DB
{
std::pair<String, Int32> PythonStatusHandler::executeGet(const Poco::JSON::Object::Ptr & /*payload*/) const
{
#if USE_PYTHON_UDF
    bool is_enable_python = Py_IsInitialized();
#else
    bool is_enable_python = false;
#endif

    Poco::JSON::Object json_resp;
    json_resp.set("initialized", is_enable_python);

    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    json_resp.stringify(resp_str_stream, 0);
    return {resp_str_stream.str(), HTTPResponse::HTTP_OK};
}
}
