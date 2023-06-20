#include "StorageInfoHandler.h"

#include <Access/ContextAccess.h>
#include <Common/quoteString.h>
#include <Databases/IDatabase.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Streaming/StorageStream.h>
#include <Storages/Streaming/StreamShard.h>
#include <Storages/System/StorageSystemPartsBase.h>
#include <base/map.h>

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
    obj.set("total_bytes_on_disk", total_bytes_on_disk);

    if (need_sort_by_bytes.has_value())
    {
        Poco::JSON::Object streams_info(Poco::JSON_PRESERVE_KEY_ORDER);
        auto streams_vec = collections::map<std::vector<StreamStorageInfoForStream>>(streams, [](const auto & elem) { return elem.second; });

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
        const auto access = query_context->getAccess();
        // if (!access->isGranted(AccessType::SHOW_TABLES))
        //     return {jsonErrorResponse("Not enough privileges", ErrorCodes::ACCESS_DENIED), HTTPResponse::HTTP_BAD_REQUEST};

        auto disk_info = loadStorageInfo(database, stream);

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

StreamStorageInfoPtr StorageInfoHandler::loadStorageInfo(const String & ns, const String & stream) const
{
    if (!native_log.enabled())
        throw DB::Exception(DB::ErrorCodes::UNSUPPORTED, "The native log is not enabled, so the interface does not work.");

    StreamStorageInfoPtr disk_info = std::make_shared<StreamStorageInfo>();

    /// Load from streaming store
    auto list_resp{native_log.listStreams(ns, nlog::ListStreamsRequest{stream})};
    if (list_resp.hasError())
        throw DB::Exception(list_resp.error_code, "Failed to list streams in streaming store: {}", list_resp.error_message);

    /// streams + 100 (reserved for other engines, such as system tables)
    disk_info->streams.reserve(list_resp.streams.size() + 100);
    for (const auto & stream_desc : list_resp.streams)
    {
        StorageID storage_id{stream_desc.ns, stream_desc.stream};
        auto & stream_info = disk_info->streams[storage_id.getFullNameNotQuoted()];
        stream_info.id = storage_id;

        auto streaming_data_info_opt = native_log.getLocalStreamInfo(stream_desc);
        if (!streaming_data_info_opt.has_value())
            throw DB::Exception(
                DB::ErrorCodes::RESOURCE_NOT_FOUND,
                "Failed to get info of the stream '{} in streaming store",
                getStreamName(stream_desc.ns, stream_desc.stream));

        stream_info.streaming_data_bytes = streaming_data_info_opt->first;
        stream_info.streaming_data_paths = std::move(streaming_data_info_opt->second);
        disk_info->total_bytes_on_disk += stream_info.streaming_data_bytes;
    }

    /// Load from historical storage
    loadLocalStoragesInfo(disk_info, ns, stream);

    /// If no specified namespace and no specified stream name, we use actual total used space on disk
    if (ns.empty() && stream.empty())
    {
        disk_info->total_bytes_on_disk = 0;
        for (auto & [_, disk] : query_context->getDisksMap())
        {
            if (disk)
                disk_info->total_bytes_on_disk += disk->getTotalSpace() - disk->getAvailableSpace();
        }
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

            /// Iterator all parts
            MergeTreeData::DataPartStateVector all_parts_state;
            const auto & all_parts = info.getParts(all_parts_state, /*has_state_column*/ true);
            for (const auto & part : all_parts)
                total_size += part->getBytesOnDisk();

            const auto & shard_paths = info.data->getDataPaths();
            paths.insert(paths.end(), shard_paths.begin(), shard_paths.end());
        }
    }
    else if (auto * data = dynamic_cast<MergeTreeData *>(info.storage.get()))
    {
        info.data = data;
        MergeTreeData::DataPartStateVector all_parts_state;
        const auto & all_parts = info.getParts(all_parts_state, /*has_state_column*/ true);
        for (const auto & part : all_parts)
            total_size += part->getBytesOnDisk();

        paths = info.data->getDataPaths();
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

                if (!dynamic_cast<MergeTreeData *>(info.storage.get()))
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

        if (!dynamic_cast<MergeTreeData *>(storage.get()))
            return; /// skip
        std::tie(stream_info.historical_data_bytes, stream_info.historical_data_paths) = getLocalStorageInfo(storage);
        disk_info->total_bytes_on_disk += stream_info.historical_data_bytes;
    }
}

}
