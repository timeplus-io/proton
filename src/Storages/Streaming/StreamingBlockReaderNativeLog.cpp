#include "StreamingBlockReaderNativeLog.h"
#include "StreamShard.h"

#include <Columns/ColumnObject.h>
#include <NativeLog/Cache/TailCache.h>
#include <NativeLog/Server/NativeLog.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
    extern const int RESOURCE_NOT_FOUND;
}

StreamingBlockReaderNativeLog::StreamingBlockReaderNativeLog(
    std::shared_ptr<StreamShard> stream_shard_,
    Int64 sn,
    Int64 max_wait_ms,
    Int64 read_buf_size_,
    const nlog::SchemaProvider * schema_provider,
    UInt16 schema_version,
    SourceColumnsDescription::PhysicalColumnPositions column_positions_,
    Poco::Logger * logger_)
    : native_log(nlog::NativeLog::instance(nullptr))
    , tail_cache(native_log.getCache())
    , inmemory(stream_shard_->isInmemory())
    , stream_shard(std::move(stream_shard_))
    , schema(stream_shard->storageStream()->getInMemoryMetadataPtr()->getSampleBlock())
    , fetch_request({})
    , schema_ctx(schema_provider == nullptr ? *this : *schema_provider, schema_version, std::move(column_positions_))
    , logger(logger_)
{
    auto storage_id{stream_shard->storageStream()->getStorageID()};

    ns = storage_id.getDatabaseName();
    fetch_request.fetch_descs.emplace_back(storage_id.getTableName(), storage_id.uuid, stream_shard->getShard(), sn, max_wait_ms, read_buf_size_);

    /// FIXME
    for (auto pos : schema_ctx.column_positions.positions)
        column_names.push_back(schema.getByPosition(pos).name);

    if (column_names.empty())
        for (const auto & col : schema)
            column_names.push_back(col.name);
}

nlog::RecordPtrs StreamingBlockReaderNativeLog::read()
{
    auto setup_sn = [this](auto & records) {
        /// Setup the shard in case down stream likes to access to it
        for (auto & record : records)
            record->setShard(stream_shard->getShard());
    };

    /// Try read from cache
    auto & fetch_desc = fetch_request.fetch_descs[0];
    {
        auto [records, fallback_to_log] = tail_cache.get(fetch_desc.stream_shard, fetch_desc.max_wait_ms, fetch_desc.sn, inmemory);
        if (!records.empty())
        {
            fetch_desc.sn = records.back()->getSN() + 1;
            fetch_desc.position = -1;
            auto results = processCached(std::move(records));
            setup_sn(results);
            return results;
        }
        else
        {
            if (!fallback_to_log)
                return {};

            LOG_TRACE(logger, "Cache miss, fallback to file read, request_sn={}", fetch_desc.sn);
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
                //                LOG_INFO(
                //                    logger,
                //                    "fetched meta={} start_pos={} end_pos={}",
                //                    fetched_desc.data.fetch_sn_metadata.string(),
                //                    fetched_desc.data.records->startPosition(),
                //                    fetched_desc.data.records->endPosition());

                auto records{fetched_desc.data.records->deserialize(schema_ctx)};
                if (unlikely(records.empty()))
                    return {};

                /// Update next sn
                fetch_desc.sn = records.back()->getSN() + 1;

                /// Update the next file position for next sn
                fetch_desc.position = fetched_desc.data.eof ? 0 : fetched_desc.data.records->endPosition();

                setup_sn(records);

                return records;
            }
            else
            {
                /// Empty records, either there are new records, or its the first time tail new records
                /// NativeLog server will return tail SN
                if (fetch_desc.sn != fetched_desc.data.fetch_sn_metadata.record_sn)
                    fetch_desc.sn = fetched_desc.data.fetch_sn_metadata.record_sn;
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

        for (size_t i = 0; i < columns; ++i)
        {
            const auto & column_name = column_names[i];
            auto * col_with_type = rb.findByName(column_name);
            if (col_with_type)
            {
                /// In general, an object has a large number of subcolumns,
                /// so when a few subcolumns required for the object, we only copy partials to improve performance
                if (isObject(col_with_type->type) && !schema_ctx.column_positions.positions.empty())
                {
                    assert(column_names.size() == schema_ctx.column_positions.positions.size());
                    auto iter = schema_ctx.column_positions.subcolumns.find(i);
                    if (iter != schema_ctx.column_positions.subcolumns.end())
                    {
                        const auto & column_object = assert_cast<const ColumnObject &>(*(col_with_type->column));
                        block.insert(ColumnWithTypeAndName{
                            column_object.cloneWithSubcolumns(iter->second), col_with_type->type, col_with_type->name});
                        continue;
                    }
                }

                /// We will need deep copy since the block from cached can be shared between different clients
                /// Some client may modify the columns in place
                block.insert(ColumnWithTypeAndName{
                    col_with_type->column->cloneResized(col_with_type->column->size()), col_with_type->type, col_with_type->name});
            }
            else
            {
                auto col{schema.getByName(column_name)};
                col.column = col.type->createColumnConstWithDefaultValue(rows)->convertToFullColumnIfConst();
                block.insert(std::move(col));
            }
        }

        /// We need copy the record since it is shared by multiple queries
        results.push_back(record->clone(std::move(block)));
    }

    return results;
}

std::pair<String, Int32> StreamingBlockReaderNativeLog::getStreamShard() const
{
    return stream_shard->getStreamShard();
}

void StreamingBlockReaderNativeLog::resetSequenceNumber(UInt64 sn)
{
    fetch_request.fetch_descs[0].sn = sn;
}
}
