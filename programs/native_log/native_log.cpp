#include "native_log_cli_parser.h"

#include <Cluster/Common/utils.h>
#include <Cluster/LocalLog/Log/Log.h>
#include <Cluster/SchemaRecord/SchemaRecord.h>

#include <Columns/ColumnsNumber.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ReadBufferFromMemory.h>
#include <Loggers/OwnFormattingChannel.h>
#include <Loggers/OwnPatternFormatter.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#include <Common/BackgroundSchedulePool.h>
#include <Common/Exception.h>
#include <Common/ProtonCommon.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

namespace CurrentMetrics
{
extern const Metric BackgroundSchedulePoolNativeLogTask;
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
}

namespace DB
{
namespace ErrorCodes
{
extern const int RESOURCE_NOT_FOUND;
}
}

namespace
{
DB::Block createBlock(size_t value, size_t event_size, int64_t event_time)
{
    DB::Block block;

    auto uint64_type = std::make_shared<DB::DataTypeUInt64>();
    auto string_type = std::make_shared<DB::DataTypeString>();
    auto datetime64_type = std::make_shared<DB::DataTypeDateTime64>(3);

    auto id_col = uint64_type->createColumn();
    auto * id_col_inner = typeid_cast<DB::ColumnUInt64 *>(id_col.get());
    id_col_inner->insertValue(value);
    block.insert(DB::ColumnWithTypeAndName{std::move(id_col), uint64_type, "id"});

    auto data_col = string_type->createColumn();
    std::string data(event_size, '+');
    data_col->insertData(data.data(), data.size());

    block.insert(DB::ColumnWithTypeAndName{std::move(data_col), string_type, "data"});

    auto time_col = datetime64_type->createColumn();
    auto * time_col_inner = typeid_cast<DB::ColumnDecimal<DB::DateTime64> *>(time_col.get());
    time_col_inner->insertValue(event_time);
    block.insert(DB::ColumnWithTypeAndName{std::move(time_col), datetime64_type, DB::ProtonConsts::RESERVED_EVENT_TIME});

    return block;
}

template <bool compress>
cluster::EntryPtr createEntry(const DB::Block & block)
{
    auto approx_size = static_cast<size_t>(block.bytes() * 1.15);
    cluster::ByteVector buffer(approx_size);
    {
        DB::WriteBufferFromVector<cluster::ByteVector> wb(buffer);

        if constexpr (compress)
        {
            auto compression_codec = DB::CompressionMethodByte::ZSTD;
            DB::CompressedWriteBuffer compressed_wb(wb, DB::CompressionCodecFactory::instance().get(std::to_underlying(compression_codec)));

            DB::NativeWriter writer(compressed_wb, DBMS_TCP_PROTOCOL_VERSION, DB::Block{});
            writer.write(block);

            compressed_wb.finalize();
        }
        else
        {
            DB::NativeWriter writer(wb, DBMS_TCP_PROTOCOL_VERSION, DB::Block{});
            writer.write(block);
        }
    }
    /// std::cout << fmt::format("block_approx_size={}, actual={} capacity={}", approx_size, buffer.size(), buffer.capacity()) << "\n";

    auto entry = std::make_shared<cluster::Entry>(std::move(buffer));
    entry->version = 2;
    entry->max_event_timestamp = 111;
    entry->append_timestamp = 222;
    entry->sn = cluster::Constants::LogStartSN;
    entry->type = cluster::EntryType::Normal;
    return entry;
}
}

namespace
{
class NativeLog
{
public:
    NativeLog(cluster::nlog::NativeLogArgs nl_args_, LoggerPtr logger_)
        : nl_args(std::move(nl_args_))
        , thread_pool(std::make_unique<ThreadPool>(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, 8))
        , scheduler(std::make_unique<DB::NLOG::BackgroundSchedulePool>(8, CurrentMetrics::BackgroundSchedulePoolNativeLogTask, "SchedTest"))
        , logger(logger_)
    {
        cluster::nlog::LogConfigPtr config(std::make_shared<cluster::nlog::LogConfig>());

        LOG_INFO(logger, "native log started in log_dir={}", nl_args.log_root_directory);

        cluster::StreamID stream_id;
        cluster::StreamShard stream_shard("default", "test", stream_id, 0);
        bool had_clean_shutdown = true;
        log = cluster::nlog::Log::create(
            stream_shard, fs::path{nl_args.log_root_directory}, config, had_clean_shutdown, *scheduler, *thread_pool);
    }

    void run()
    {
        switch (nl_args.command)
        {
            case cluster::nlog::Command::Produce:
                return handleProduceCommand();
            case cluster::nlog::Command::Consume:
                return handleConsumeCommand();
            case cluster::nlog::Command::Trim:
                return handleTrimCommand();
            case cluster::nlog::Command::DumpIndex:
                return handleDumpIndex();
            default:
                LOG_ERROR(logger, "Unsupported command {}", magic_enum::enum_name(nl_args.command));
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported command {}", magic_enum::enum_name(nl_args.command));
        }
    }

private:
    void handleProduceCommand()
    {
        struct ProduceMetrics
        {
            uint64_t total_records = 0;
            uint64_t total_events = 0;
            uint64_t total_bytes = 0;
            uint64_t each_entry_bytes = 0;
            uint64_t each_batch_bytes = 0;
        };

        ProduceMetrics metrics;

        constexpr int64_t event_timestamp = 1612286044;
        constexpr int64_t append_timestamp = 1612286044;

        uint64_t total_events = nl_args.produce_args->num_records;
        int64_t event_size = nl_args.produce_args->event_size;
        int64_t batch_size = nl_args.produce_args->batch_size;

        double elapsed{};
        for (uint64_t k = 0; k < total_events; k += batch_size)
        {
            cluster::EntryPtrs entries;
            entries.reserve(batch_size);

            for (uint64_t m = k; m < k + batch_size && m < total_events; ++m)
            {
                auto block = createBlock(m, event_size, event_timestamp + m);
                auto entry = createEntry</*compress=*/false>(block);
                entry->max_event_timestamp = event_timestamp + m;
                entry->append_timestamp = append_timestamp + m;
                entry->sn = cluster::Constants::LogStartSN + m;
                entries.push_back(std::move(entry));
            }

            Stopwatch stopwatch;
            [[maybe_unused]] auto append_result = log->append(entries);
            elapsed += stopwatch.elapsedSeconds();
        }

        /// metrics stat
        {
            auto block_for_profile = createBlock(0, event_size, event_timestamp);
            auto entry_for_profile = createEntry</*compress=*/false>(block_for_profile);

            metrics.total_records = total_events / batch_size;
            metrics.total_events = total_events;
            metrics.each_entry_bytes = entry_for_profile->approximateSerializedSize();
            metrics.each_batch_bytes = metrics.each_entry_bytes * batch_size;
            metrics.total_bytes = metrics.each_entry_bytes * metrics.total_events;
        }

        double records_per_second = metrics.total_records / elapsed;
        double events_per_second = metrics.total_events / elapsed;
        double bytes_per_second = metrics.total_bytes / elapsed;
        double gigabytes_per_second = bytes_per_second / (1024 * 1024 * 1024);

        LOG_INFO(
            logger,
            "\n\nProduce Metrics:\n"
            "Records: {}\n"
            "Events: {}\n"
            "Total Bytes: {}\n"
            "Record Batch Size: {}\n"
            "Event Size: {}\n"
            "Record Size: {}\n"
            "Records per Second (RPS): {:.2f}\n"
            "Events per Second (EPS): {:.2f}\n"
            "Bytes per Second (BPS): {:.2f}\n"
            "Gigabytes per Second (GBps): {:.2f}\n"
            "Elapsed Time: {} seconds\n\n",
            metrics.total_records,
            metrics.total_events,
            metrics.total_bytes,
            batch_size,
            metrics.each_entry_bytes,
            metrics.each_batch_bytes,
            records_per_second,
            events_per_second,
            bytes_per_second,
            gigabytes_per_second,
            elapsed);
    }

    void handleConsumeCommand()
    {
        struct ConsumeMetrics
        {
            uint64_t total_events = 0;
            uint64_t total_bytes = 0;
        };

        Stopwatch stopwatch;
        ConsumeMetrics metrics;

        std::optional<int64_t> max_sn;
        if (nl_args.consume_args->num_records != 0)
            max_sn = nl_args.consume_args->start_sn + nl_args.consume_args->num_records;

        int64_t next_sn = nl_args.consume_args->start_sn;
        std::optional<cluster::FetchHint> fetch_hint;

        while (metrics.total_events < nl_args.consume_args->num_records)
        {
            auto fetch_result = log->fetch(
                /*start_sn=*/next_sn,
                /*max_size=*/nl_args.consume_args->buf_size,
                /*max_wait_ms=*/0,
                /*fetch_hint=*/fetch_hint,
                /*FetchIsolation=*/cluster::FetchIsolation::LogEnd);

            if (fetch_result.hasError())
            {
                LOG_ERROR(logger, "Failed to fetch log entries starting with sn={}, {}", next_sn, fetch_result.errorString());
                break;
            }

            if (fetch_result.result.empty())
            {
                LOG_INFO(logger, "Reaching the end of the log");
                break;
            }

            metrics.total_bytes += cluster::totalApproximateSerializedSizeOf<true>(fetch_result.result.entries);
            metrics.total_events += fetch_result.result.entries.size();

            next_sn = fetch_result.result.entries.back()->sn + 1;
            fetch_hint = fetch_result.result.fetch_hint;

            if (nl_args.consume_args->print_each_entry)
            {
                for (const auto & entry : fetch_result.result.entries)
                {
                    if (!max_sn || (entry->sn < max_sn.value()))
                        LOG_INFO(logger, "{}", entry->string());
                    else
                        break;
                }
            }
        }

        auto elapsed = stopwatch.elapsedSeconds();

        /// Calculate throughput metrics
        double events_per_second = metrics.total_events / elapsed;
        double bytes_per_second = metrics.total_bytes / elapsed;
        double gigabytes_per_second = bytes_per_second / (1024 * 1024 * 1024);

        /// Logging the metrics with refined output
        LOG_INFO(
            logger,
            "\n\nConsumed Metrics:\n"
            "Events Consumed: {}\n"
            "Total Bytes Consumed: {}\n"
            "Events per Second (EPS): {:.2f}\n"
            "Bytes per Second (BPS): {:.2f}\n"
            "Gigabytes per Second (GBps): {:.2f}\n"
            "Elapsed Time: {:.2f} seconds\n\n",
            metrics.total_events,
            metrics.total_bytes,
            events_per_second,
            bytes_per_second,
            gigabytes_per_second,
            elapsed);
    }

    void handleTrimCommand()
    {
        auto errcode = log->trim(nl_args.trim_args->start_sn);
        if (errcode == DB::ErrorCodes::OK)
            LOG_INFO(logger, "Finished trimming log start_s=={}", nl_args.trim_args->start_sn);
        else
            LOG_ERROR(
                logger,
                "Failed to trim log start_sn={}, error_code={} error_message={}",
                nl_args.trim_args->start_sn,
                errcode,
                DB::ErrorCodes::getName(errcode));
    }

    template <typename IndexEntry>
    void dumpIndex(
        const std::string_view index_file_name,
        std::function<std::string(const IndexEntry &)> fmt_index_entry
        = [](const IndexEntry & index_entry) { return index_entry.string(); })
    {
        auto filename = fmt::format("{}/{}", nl_args.log_root_directory, index_file_name);
        cluster::nlog::AppendOnlyFile file(filename, /*file_max_size_=*/0, /*preallocate_=*/false);

        auto size_result = file.size();
        if (size_result.hasError())
        {
            LOG_ERROR(logger, "Failed to get {} file size, {}", index_file_name, size_result.errorString());
            return;
        }

        LOG_INFO(logger, "Dumping {} with {} index entries", filename, size_result.result / IndexEntry::exactSerializedSize());

        std::vector<char> read_buf;
        read_buf.resize(size_result.result);

        auto read_result = file.read(read_buf.data(), read_buf.size(), /*read_offset*/ 0);
        if (read_result.hasError())
        {
            LOG_ERROR(logger, "Failed to read {}, {}", index_file_name, size_result.errorString());
            return;
        }

        if (read_result.result % IndexEntry::exactSerializedSize() != 0)
        {
            LOG_ERROR(
                logger,
                "Corrupted {}, it doesn't contain complete index entries. Read size={}, file_size={}",
                index_file_name,
                read_result.result,
                size_result.result);
            return;
        }

        auto entries = read_result.result / IndexEntry::exactSerializedSize();

        DB::ReadBufferFromMemory rb{read_buf.data(), read_result.result};

        for (size_t i = 0; i < entries; ++i)
        {
            IndexEntry index_entry;
            index_entry.deserialize(rb, /*version=*/1);
            LOG_INFO(logger, "index={} {}", i, fmt_index_entry(index_entry));
        }
    }

    void dumpSequencePositionIndex()
    {
        dumpIndex<cluster::nlog::SequencePosition>("sn-pos.idx", [](const cluster::nlog::SequencePosition & sn_pos) {
            auto [seg_id, file_pos] = cluster::nlog::Loglet::segmentIDAndFilePosition(sn_pos.file_position);
            return fmt::format("segment_id={} sn={} file_position={}", seg_id, sn_pos.sn, file_pos);
        });
    }

    void dumpTimestampSequenceIndex() { dumpIndex<cluster::nlog::TimestampSequence>("ts-sn.idx"); }

    void dumpEpochSequenceIndex() { dumpIndex<cluster::EpochSequence>("epoch-sn.idx"); }


    void handleDumpIndex()
    {
        switch (nl_args.dump_index_args->index_type)
        {
            case cluster::nlog::DumpIndexArgs::IndexType::SequencePosition:
            {
                dumpSequencePositionIndex();
                break;
            }
            case cluster::nlog::DumpIndexArgs::IndexType::TimestampSequence:
            {
                dumpTimestampSequenceIndex();
                break;
            }
            case cluster::nlog::DumpIndexArgs::IndexType::EpochSequence:
            {
                dumpEpochSequenceIndex();
                break;
            }
        }
    }


private:
    cluster::nlog::NativeLogArgs nl_args;

    cluster::nlog::LogPtr log;
    std::unique_ptr<ThreadPool> thread_pool;
    std::unique_ptr<DB::NLOG::BackgroundSchedulePool> scheduler;

    LoggerPtr logger;
};

void run(const cluster::nlog::NativeLogArgs & nl_args, LoggerPtr logger)
{
    NativeLog native_log(nl_args, logger);
    native_log.run();
}

}

int mainNativeLog(int argc, char ** argv)
{
    auto nl_args = cluster::nlog::parseArgs(argc, argv);
    if (!nl_args.valid())
        return 1;

    /// Setup logger
    Poco::AutoPtr<OwnPatternFormatter> pf(new OwnPatternFormatter(true));
    Poco::AutoPtr<DB::OwnFormattingChannel> console_channel(new DB::OwnFormattingChannel(pf, new Poco::ConsoleChannel));
    Poco::Logger::root().setChannel(console_channel);
    Poco::Logger::root().setLevel("debug");

    auto logger = getLogger("native_log");

    try
    {
        run(nl_args, logger);
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(logger, "Failed to run: {} ", e.message());
    }
    catch (const std::exception & e)
    {
        DB::tryLogCurrentException(logger, DB::getExceptionStackTraceString(e));
    }
    catch (...)
    {
        DB::tryLogCurrentException(logger, "Unknown exception");
    }
    return 0;
}
