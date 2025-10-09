#include <Server/RestRouterHandlers/StorageInfoHandler.h>

#include <Access/ContextAccess.h>
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/NativeLog/NativeLog.h>
#include <Databases/IDatabase.h>
#include <Storages/MatView/StorageMaterializedView.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Stream/StorageStream.h>
#include <Storages/Stream/StreamShardStore.h>
#include <Storages/System/StorageSystemPartsBase.h>
#include <base/map.h>
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
String getStreamName(const String & database, const String & stream)
{
    return fmt::format("{}.{}", backQuoteIfNeed(database), backQuoteIfNeed(stream));
}

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

Poco::Dynamic::Var StreamStorageInfoForStream::toJSON(bool is_simple) const
{
    Poco::JSON::Object obj(Poco::JSON_PRESERVE_KEY_ORDER);

    if (is_simple)
    {
        obj.set("bytes_on_disk", streaming_data_bytes + historical_data_bytes);
    }
    else
    {
        obj.set("streaming_data_bytes", streaming_data_bytes);
        Poco::JSON::Array streaming_paths(Poco::JSON_PRESERVE_KEY_ORDER);
        for (const auto & path : streaming_data_paths)
            streaming_paths.add(path);
        obj.set("streaming_data_paths", streaming_paths);

        obj.set("historical_data_bytes", historical_data_bytes);
        Poco::JSON::Array historical_paths(Poco::JSON_PRESERVE_KEY_ORDER);
        for (const auto & path : historical_data_paths)
            historical_paths.add(path);
        obj.set("historical_data_paths", historical_paths);
    }
    return obj;
}

Poco::Dynamic::Var StreamStorageInfo::toJSON(bool is_simple) const
{
    Poco::JSON::Object obj(Poco::JSON_PRESERVE_KEY_ORDER);
    obj.set("total_disk_space", total_disk_space);
    obj.set("total_available_disk_space", total_available_disk_space);
    obj.set("total_bytes_on_disk", total_bytes_on_disk);

    if (need_sort_by_bytes.has_value())
    {
        Poco::JSON::Object streams_info(Poco::JSON_PRESERVE_KEY_ORDER);
        auto streams_vec
            = collections::map<std::vector<StreamStorageInfoForStream>>(streams, [](const auto & elem) { return elem.second; });

        /// Sorting
        if (*need_sort_by_bytes)
            std::sort(streams_vec.begin(), streams_vec.end(), [](const auto & s1, const auto & s2) {
                return (s1.streaming_data_bytes + s1.historical_data_bytes) > (s2.streaming_data_bytes + s2.historical_data_bytes);
            });
        else
            std::sort(streams_vec.begin(), streams_vec.end(), [](const auto & s1, const auto & s2) {
                return (s1.streaming_data_bytes + s1.historical_data_bytes) < (s2.streaming_data_bytes + s2.historical_data_bytes);
            });

        for (const auto & stream : streams_vec)
            streams_info.set(getStreamName(stream.id.database_name, stream.id.table_name), stream.toJSON(is_simple));

        obj.set("streams", streams_info);
    }
    else
    {
        Poco::JSON::Object streams_info;
        for (const auto & [_, stream] : streams)
            streams_info.set(getStreamName(stream.id.database_name, stream.id.table_name), stream.toJSON(is_simple));
        obj.set("streams", streams_info);
    }
    return obj;
}

std::pair<String, Int32> StorageInfoHandler::executeGet(const Poco::JSON::Object::Ptr & /*payload*/) const
{
    const auto & database = getPathParameter("database");
    const auto & stream = getPathParameter("stream");
    bool is_simple = getQueryParameterBool("simple", false);
    bool need_sort = getQueryParameterBool("sort", true);
    try
    {
        /// const auto access = query_context->getAccess();
        /// if (!access->isGranted(AccessType::SHOW_TABLES))
        ///     return {jsonErrorResponse("Not enough privileges", ErrorCodes::ACCESS_DENIED), HTTPResponse::HTTP_BAD_REQUEST};

        auto disk_info = loadStorageInfo(database, stream);

        /// Sort desc by bytes_on_disk, we want to show the stream with disk usage desc
        if (disk_info && need_sort)
            disk_info->sortingStreamByBytes();

        /// For normal users only get simple info
        /// const bool is_simple = !access->isGranted(AccessType::ACCESS_MANAGEMENT);
        return {buildResponse(disk_info, query_context->getCurrentQueryId(), is_simple), HTTPResponse::HTTP_OK};
    }
    catch (...)
    {
        auto exception = std::current_exception();
        return {jsonErrorResponse(getExceptionMessage(exception, false), getExceptionErrorCode(exception)), HTTPResponse::HTTP_BAD_REQUEST};
    }
}

StreamStorageInfoPtr StorageInfoHandler::loadStorageInfo(const String & ns, const String & stream) const
{
    StreamStorageInfoPtr disk_info = std::make_shared<StreamStorageInfo>();

    /// Load from streaming store
    auto timeout_ms = query_context->getSettingsRef().query_timeout_sec * 1000;
    auto & metastore = Globals::getMetaStore();
    auto list_resp{metastore.listStreams(std::make_shared<cluster::ListStreamsRequest>(
        ns,
        stream,
        metastore.nodeID(),
        /*consistent_read_=*/false,
        timeout_ms,
        /*request_version=*/2))};
    if (list_resp->hasError())
        throw DB::Exception(
            list_resp->data().err.error_code, "Failed to list streams in streaming store: {}", list_resp->data().err.error_message);

    /// streams + 100 (reserved for other engines, such as system tables)
    disk_info->streams.reserve(list_resp->data().stream_descs.size() + 100);
    for (const auto & stream_desc : list_resp->data().stream_descs)
    {
        /// All shards are local
        if (!stream_desc->hasLocalShardReplica(query_context->getNodeID()))
            continue;

        StorageID storage_id{stream_desc->stream.ns, stream_desc->stream.name, stream_desc->stream.id};
        auto storage = DatabaseCatalog::instance().tryGetTable(storage_id, query_context);
        StreamStorageInfoForStream stream_info{};
        stream_info.id = storage_id;

        if (storage)
        {
            if (storage->as<StorageStream>())
            {
                auto stream_stats = Globals::getNativeLog().stats(stream_desc->localStreamShards(query_context->getNodeID()));
                if (stream_stats.hasError() || stream_stats.result.empty())
                    continue;
                std::tie(stream_info.streaming_data_bytes, stream_info.streaming_data_paths) = DB::sumOf(std::move(stream_stats.result));
            }
            else if (const auto & mv = storage->as<StorageMaterializedView>())
            {
                auto stream_stats = Globals::getNativeLog().stats(stream_desc->localStreamShards(query_context->getNodeID()));
                if (!stream_stats.hasError() && !stream_stats.result.empty())
                    std::tie(stream_info.streaming_data_bytes, stream_info.streaming_data_paths)
                        = DB::sumOf(std::move(stream_stats.result));

                auto path_sizes = mv->getCheckpointStat();
                for (auto & path_size : path_sizes)
                {
                    stream_info.streaming_data_bytes += path_size.size;
                    stream_info.streaming_data_paths.push_back(std::move(path_size.path));
                }
            }
        }
        disk_info->total_bytes_on_disk += stream_info.streaming_data_bytes;
        disk_info->streams[storage_id.getFullNameNotQuoted()] = std::move(stream_info);
    }

    if (disk_info->streams.empty())
    {
        String res_name;
        if (!ns.empty() && !stream.empty())
            res_name = fmt::format("stream '{}.{}'", ns, stream);
        else if (!ns.empty())
            res_name = fmt::format("namespace '{}'", ns);

        if (!res_name.empty())
            throw DB::Exception(DB::ErrorCodes::RESOURCE_NOT_FOUND, "Failed to get stats info for {} in streaming store", res_name);
    }

    /// Load from historical storage
    loadLocalStoragesInfo(disk_info, ns, stream);

    /// If no specified namespace and no specified stream name, we use actual total used space on disk
    auto use_general_total_bytes = ns.empty() && stream.empty();
    for (const auto & [_, disk] : query_context->getDisksMap())
    {
        if (!disk)
            continue;

        disk_info->total_disk_space = disk->getTotalSpace();
        disk_info->total_available_disk_space = disk->getAvailableSpace();
        if (use_general_total_bytes)
            disk_info->total_bytes_on_disk = disk_info->total_disk_space - disk_info->total_available_disk_space;
    }

    return disk_info;
}

auto StorageInfoHandler::getLocalStorageInfo(StoragePtr storage) const
{
    assert(storage);
    StoragesInfo info;
    auto storage_id = storage->getStorageID();
    info.database = storage_id.database_name;
    info.table = storage_id.table_name;
    info.storage = storage;
    info.need_inactive_parts = true; /// or false?

    /// For table not to be dropped and set of columns to remain constant.
    info.table_lock = info.storage->lockForShare(query_context->getCurrentQueryId(), query_context->getSettingsRef().lock_acquire_timeout);

    info.engine = info.storage->getName();

    uint64_t total_size = 0;
    std::vector<String> paths;
    if (info.engine == "Stream")
    {
        const auto & stream = assert_cast<const StorageStream &>(*info.storage);
        const auto & stream_shards = stream.getStreamShards();
        for (const auto & stream_shard : stream_shards)
        {
            info.data = stream_shard->getStorage();
            if (!info.data)
                continue; /// virtual storage, it's a remote shard

            total_size = info.data->getStorageSize();
            const auto & shard_paths = info.data->getDataPaths();
            paths.insert(paths.end(), shard_paths.begin(), shard_paths.end());
        }
    }
    else if (auto * data = dynamic_cast<MergeTreeData *>(info.storage.get()))
    {
        info.data = data;
        total_size = data->getStorageSize();
        paths = info.data->getDataPaths();
    }
    else if (storage)
    {
        total_size = storage->getStorageSize();
        paths = storage->getDataPaths();
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown engine {}", info.engine);


    return std::pair{total_size, std::move(paths)};
}

void StorageInfoHandler::loadLocalStoragesInfo(StreamStorageInfoPtr & disk_info, const String & ns, const String & stream) const
{
    const auto access = query_context->getAccess();
    if (stream.empty())
    {
        /// List all storages [in the specified database]
        Databases databases = DatabaseCatalog::instance().getDatabases();
        for (const auto & [database_name, database] : databases)
        {
            /// Check whether is the specified database
            if (!ns.empty() && ns != database_name)
                continue;

            /// Check if database can contain MergeTree tables,
            /// if not it's unnecessary to load all tables of database just to filter all of them.
            if (!database->canContainMergeTreeTables())
                continue;

            for (auto iterator = database->getTablesIterator(query_context); iterator->isValid(); iterator->next())
            {
                StoragesInfo info;
                info.database = database_name;
                info.table = iterator->name();
                info.storage = iterator->table();
                if (!info.storage)
                    continue;

                if (!dynamic_cast<MergeTreeData *>(info.storage.get()) && !info.storage->as<StorageMaterializedView>())
                    continue;

                // if (!access->isGranted(AccessType::SHOW_TABLES, info.database, info.table))
                //     continue;

                StorageID storage_id{info.database, info.table};
                auto & stream_info = disk_info->streams[storage_id.getFullNameNotQuoted()];
                stream_info.id = storage_id;
                std::tie(stream_info.historical_data_bytes, stream_info.historical_data_paths) = getLocalStorageInfo(info.storage);
                disk_info->total_bytes_on_disk += stream_info.historical_data_bytes;
            }
        }
    }
    else
    {
        assert(!ns.empty());
        StorageID storage_id{ns, stream};
        auto storage = DatabaseCatalog::instance().tryGetTable(storage_id, query_context);
        if (!storage)
            throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "Not found '{}'.", storage_id.getNameForLogs());

        // if (!access->isGranted(AccessType::SHOW_TABLES, ns, stream))
        //     throw Exception(ErrorCodes::ACCESS_DENIED, "Not enough privileges to show the stream '{}'", storage_id.getNameForLogs());

        auto & stream_info = disk_info->streams[storage_id.getFullNameNotQuoted()];
        stream_info.id = storage_id;

        if (!dynamic_cast<MergeTreeData *>(storage.get()) && !storage->as<StorageMaterializedView>())
            return; /// skip

        std::tie(stream_info.historical_data_bytes, stream_info.historical_data_paths) = getLocalStorageInfo(storage);
        disk_info->total_bytes_on_disk += stream_info.historical_data_bytes;
    }
}

}
