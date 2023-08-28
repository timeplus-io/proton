#include "ClusterInfoHandler.h"

#include <Common/sendRequest.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int RESOURCE_NOT_INITED;
    extern const int UNSUPPORTED;
}

namespace
{
}

std::pair<String, Int32> ClusterInfoHandler::executeGet(const Poco::JSON::Object::Ptr & /* payload */) const
{
    return {jsonErrorResponse("Internal server error", ErrorCodes::UNSUPPORTED), HTTPResponse::HTTP_INTERNAL_SERVER_ERROR};
}
}
