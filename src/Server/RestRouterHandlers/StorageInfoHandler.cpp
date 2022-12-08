#include "StorageInfoHandler.h"

#include <DistributedMetadata/PlacementService.h>

#include <Access/ContextAccess.h>
#include <Databases/IDatabase.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMergeTree.h>
#include <Common/quoteString.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_REQUEST_PARAMETER;
    extern const int UNSUPPORTED;
    extern const int RESOURCE_NOT_FOUND;
    extern const int STREAM_IS_DROPPED;
    extern const int ACCESS_DENIED;
    extern const int LOGICAL_ERROR;
}

namespace
{
    /// @FORMAT:
    /// {
    ///     "request_id": xxx,
    ///     "data":
    ///     {
    ///         "total_bytes_on_disk": xxx,
    ///         "streams":
    ///         {
    ///             "stream-1":
    ///             {
    ///                 "streaming_data_bytes": xxx,
    ///                 "streaming_data_paths": ["xxx/path"],
    ///                 "historical_data_bytes": xxx,
    ///                 "historical_data_paths": ["xxx/path"]
    ///             },
    ///             ...
    ///         }
    ///     }
    /// }
    String buildResponse(const StreamStorageInfoPtr & storage_info, const String & query_id, bool is_simple = false)
    {
        Poco::JSON::Object resp(Poco::JSON_PRESERVE_KEY_ORDER);
        resp.set("request_id", query_id);

        if (storage_info)
            resp.set("data", storage_info->toJSON(is_simple));

        std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        resp.stringify(resp_str_stream, 0);
        return resp_str_stream.str();
    }
}

std::pair<String, Int32> StorageInfoHandler::executeGet(const Poco::JSON::Object::Ptr & /*payload*/) const
{
    const auto & database = getPathParameter("database");
    const auto & stream = getPathParameter("stream");
    bool is_simple = getQueryParameterBool("simple", false);
    bool need_sort = getQueryParameterBool("sort", true);
    try
    {
        const auto access = query_context->getAccess();
        // if (!access->isGranted(AccessType::SHOW_TABLES))
        //     return {jsonErrorResponse("Not enough privileges", ErrorCodes::ACCESS_DENIED), HTTPResponse::HTTP_BAD_REQUEST};

        auto disk_info = PlacementService::instance(query_context).loadStorageInfo(database, stream);

        /// Sort desc by bytes_on_disk, we want to show the stream with disk usage desc
        if (disk_info && need_sort)
            disk_info->sortingStreamByBytes();

        /// For normal users only get simple info
        // const bool is_simple = !access->isGranted(AccessType::ACCESS_MANAGEMENT);
        return {buildResponse(disk_info, query_context->getCurrentQueryId(), is_simple), HTTPResponse::HTTP_OK};
    }
    catch (...)
    {
        auto exception = std::current_exception();
        return {jsonErrorResponse(getExceptionMessage(exception, false), getExceptionErrorCode(exception)), HTTPResponse::HTTP_BAD_REQUEST};
    }
}
}
