#include "APISpecHandler.h"
#include "YAMLParser.h"

#include <Poco/DirectoryIterator.h>
#include <Poco/Path.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int RESOURCE_NOT_FOUND;
}

namespace
{
String getSpecPath(const String & config_path)
{
    Poco::Path config_path_(config_path);
    Poco::Path spec_path_;

    if (config_path_.depth() > 1)
    {
        spec_path_ = config_path_.parent().parent();
        spec_path_ = spec_path_.append("daisy-spec");
        if (Poco::File(spec_path_).exists())
            return spec_path_.toString();
    }
    else
    {
        spec_path_ = config_path_.current();
        spec_path_ = spec_path_.append("spec");
        if (Poco::File(spec_path_).exists())
            return spec_path_.toString();

        spec_path_ = getenv("HOME");
        spec_path_ = spec_path_.append("daisy-spec");
        if (Poco::File(spec_path_).exists())
            return spec_path_.toString();

        if (Poco::File("/etc/daisy-spec/").exists())
        {
            return "/etc/daisy-spec/";
        }
    }
    return "";
}

}

std::pair<String, Int32> APISpecHandler::executeGet(const Poco::JSON::Object::Ptr & /*payload*/) const
{
    const String & spec_path = getSpecPath(query_context->getConfigPath());
    if (spec_path.empty())
    {
        return {
            jsonErrorResponse(
                "The daisy-spec file : '" + spec_path
                    + "' could not be found, please keep it consistent with the daisy-server path",
                ErrorCodes::RESOURCE_NOT_FOUND),
            HTTPResponse::HTTP_NOT_FOUND};
    }

    Poco::Path path(spec_path);
    path.append("rest-api");
    if (!Poco::File(path).exists())
    {
        return {
            jsonErrorResponse(
                "Failed to find the rest-api under the daisy-spec , path is : " + path.toString(), ErrorCodes::RESOURCE_NOT_FOUND),
            HTTPResponse::HTTP_NOT_FOUND};
    }

    Poco::JSON::Object doc_json;
    Poco::DirectoryIterator end_iter;
    for (Poco::DirectoryIterator iter(path); iter != end_iter; ++iter)
    {
        if (iter->isFile())
        {
            const auto & doc = parseToJson(iter->path());
            doc_json.set(iter.path().getBaseName(), doc);
        }
    }

    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    doc_json.stringify(resp_str_stream, 0);

    return {resp_str_stream.str(), HTTPResponse::HTTP_OK};
}

}
