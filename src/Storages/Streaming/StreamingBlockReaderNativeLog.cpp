#include "StreamingBlockReaderNativeLog.h"

#include <NativeLog/Cache/TailCache.h>
#include <NativeLog/Server/NativeLog.h>
#include <Storages/IStorage.h>
#include <base/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern int OK;
    extern int RESOURCE_NOT_FOUND;
}

StreamingBlockReaderNativeLog::StreamingBlockReaderNativeLog(
    std::shared_ptr<IStorage> storage_,
    Int32 shard_,
    Int64 sn,
    Int64 max_wait_ms,
    Int64 read_buf_size_,
    const nlog::SchemaProvider * schema_provider,
    UInt16 schema_version,
    std::vector<UInt16> column_positions_,
    Poco::Logger * logger_)
    : native_log(nlog::NativeLog::instance(nullptr))
    , tail_cache(native_log.getCache())
    , storage(std::move(storage_))
    , header(storage->getInMemoryMetadataPtr()->getSampleBlock())
    , fetch_request({})
    , read_buf_size(read_buf_size_)
    , read_buf(read_buf_size_, '\0')
    , schema_ctx(schema_provider == nullptr ? *this : *schema_provider, schema_version, std::move(column_positions_))
    , logger(logger_)
{
    auto storage_id{storage->getStorageID()};

    ns = storage_id.getDatabaseName();
    fetch_request.fetch_descs.emplace_back(storage_id.getTableName(), storage_id.uuid, shard_, sn, max_wait_ms, read_buf_size_);

    /// FIXME
    for (auto pos : schema_ctx.column_positions)
        column_names.push_back(header.getByPosition(pos).name);

    if (column_names.empty())
        for (const auto & col : header)
            column_names.push_back(col.name);
}

nlog::RecordPtrs StreamingBlockReaderNativeLog::read()
{
    /// Try read from cache
    auto & fetch_desc = fetch_request.fetch_descs[0];
    {
        auto [records, fallback_to_log] = tail_cache.get(fetch_desc.stream_shard, fetch_desc.max_wait_ms, fetch_desc.sn);
        if (!records.empty())
        {
            fetch_desc.sn = records.back()->getSN() + 1;
            fetch_desc.position = -1;
            return processCached(std::move(records));
        }
        else
        {
            if (!fallback_to_log)
                return {};

            LOG_DEBUG(logger, "Cache miss, fallback to file read, request_sn={}", fetch_desc.sn);
        }
    }

    auto resp{native_log.fetch(ns, fetch_request)};
    if (!resp.hasError())
    {
        assert(resp.fetched_data.size() == 1);
        auto & fetched_desc = resp.fetched_data[0];
        if (fetched_desc.errcode == ErrorCodes::OK)
        {
            if (fetched_desc.data.records)
            {
                auto records{fetched_desc.data.records->deserialize(read_buf, schema_ctx)};
                assert(!records.empty());

                if (read_buf.size() > static_cast<UInt64>(read_buf_size))
                    read_buf.resize(read_buf_size);

                /// Update next sn
                fetch_desc.sn = records.back()->getSN() + 1;

                /// Update the next file position for next sn
                fetch_desc.position = fetched_desc.data.records->endPosition();
                return records;
            }
            else
            {
                /// Empty records, either there are new records, or its the first time tail new records
                /// NativeLog server will return tail SN
                if (fetch_desc.sn != fetched_desc.data.fetch_offset_metadata.record_sn)
                    fetch_desc.sn = fetched_desc.data.fetch_offset_metadata.record_sn;
            }
        }
        else if (fetched_desc.errcode == ErrorCodes::RESOURCE_NOT_FOUND)
            throw DB::Exception(ErrorCodes::RESOURCE_NOT_FOUND, "Stream {} doesn't exist", fetch_desc.stream_shard.stream.name);

        return {};
    }
    else
    {
        LOG_ERROR(logger, "Failed to fetch records, error={}", resp.errString());
        return {};
    }
}

nlog::RecordPtrs StreamingBlockReaderNativeLog::processCached(nlog::RecordPtrs records)
{
    auto columns = column_names.size();
    nlog::RecordPtrs results;
    results.reserve(records.size());

    for (const auto & record : records)
    {
        Block block;
        block.reserve(columns);

        const auto & rb = record->getBlock();
        auto rows = rb.rows();
        for (const auto & column_name : column_names)
        {
            auto * col_with_type = rb.findByName(column_name);
            if (col_with_type)
            {
                block.insert(*col_with_type);
            }
            else
            {
                auto col{header.getByName(column_name)};
                col.column = col.type->createColumnConstWithDefaultValue(rows)->convertToFullColumnIfConst();
                block.insert(std::move(col));
            }
        }

        /// We need copy the record since it is shared by multiple queries
        results.push_back(record->clone(std::move(block)));
    }

    return results;
}
}
