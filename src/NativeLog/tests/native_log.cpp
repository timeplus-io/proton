#include "create_record.h"
#include "native_log_cli_parser.h"

#include <NativeLog/Record/Record.h>
#include <NativeLog/Base/Concurrent/UnboundedQueue.h>
#include <NativeLog/Server/NativeLog.h>

#include <base/ClockUtils.h>
#include <base/logger_useful.h>
#include <loggers/OwnFormattingChannel.h>
#include <loggers/OwnPatternFormatter.h>
#include <Common/Exception.h>

#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>

#include <algorithm>
#include <chrono>

namespace DB
{
namespace ErrorCodes
{
    extern const int RESOURCE_NOT_FOUND;
}
}

namespace
{
class NativeLog
{
public:
    NativeLog(nlog::NativeLogArgs nl_args_, Poco::Logger * logger_) : nl_args(std::move(nl_args_)), logger(logger_)
    {
        nlog::NativeLogConfig config;
        config.log_dirs = {fs::path{nl_args.log_root_directory}};
        config.meta_dir = nl_args.meta_root_directory;

        LOG_INFO(logger, "native log started in log_dir={} meta_dir={}", nl_args.log_root_directory, nl_args.meta_root_directory);

        server.reset(new nlog::NativeLog(config));
        server->startup();
    }

    void run()
    {
        if (nl_args.command == "stream")
            handleStreamCommand();
        else if (nl_args.command == "produce")
            handleProduceCommand();
        else if (nl_args.command == "consume")
            handleConsumeCommand();
        else if (nl_args.command == "trim")
            handleTrimCommand();
        else
        {
            LOG_ERROR(logger, "Unsupported command {}", nl_args.command);
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported command {}", nl_args.command);
        }
    }

private:
    using SequenceQueue = nlog::UnboundedQueue<int64_t>;
    using SequenceContainer = std::vector<std::shared_ptr<SequenceQueue>>;
    using SequenceContainerPtr = std::unique_ptr<SequenceContainer>;

    void handleStreamCommand()
    {
        assert(nl_args.stream_args.has_value());

        if (nl_args.stream_args->command == "create")
            handleStreamCreateCommand();
        else if (nl_args.stream_args->command == "delete")
            handleStreamDeleteCommand();
        else if (nl_args.stream_args->command == "list")
            handleStreamListCommand();
        else
        {
            LOG_ERROR(logger, "Unsupported stream command {}", nl_args.stream_args->command);
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported command {}", nl_args.stream_args->command);
        }
    }

    void handleStreamCreateCommand()
    {
        nlog::CreateStreamRequest request(nl_args.stream_args->stream, DB::UUIDHelpers::Nil, nl_args.stream_args->shards, 1);
        request.compacted = nl_args.stream_args->compacted;
        auto response = server->createStream(nl_args.stream_args->ns, request);
        LOG_INFO(logger, "\n\n\nStream creation succeeded with response: {}\n\n\n", response.string());
    }

    void handleStreamDeleteCommand()
    {
        nlog::DeleteStreamRequest request(nl_args.stream_args->stream, DB::UUIDHelpers::Nil);
        auto response = server->deleteStream(nl_args.stream_args->ns, request);
        LOG_INFO(logger, "\n\n\nStream deletion succeeded with response: {}\n\n\n", response.string());
    }

    void handleStreamListCommand()
    {
        nlog::ListStreamsRequest request(nl_args.stream_args->stream);
        auto response = server->listStreams(nl_args.stream_args->ns, request);
        for (const auto & stream_desc : response.streams)
            LOG_INFO(logger, "\n\n\nStream : {}\n\n\n", stream_desc.string());
    }

    void handleProduceCommand()
    {
        struct ProduceMetrics
        {
            std::atomic<uint64_t> total_records = 0;
            std::atomic<uint64_t> total_events = 0;
            std::atomic<uint64_t> total_bytes = 0;
        };

        ProduceMetrics metrics;

        auto resp{server->listStreams(nl_args.produce_args->ns, nlog::ListStreamsRequest{nl_args.produce_args->stream})};
        if (resp.streams.empty())
            throw DB::Exception(DB::ErrorCodes::RESOURCE_NOT_FOUND, "Stream {} not found", nl_args.produce_args->stream);

        auto shards = resp.streams[0].shards;
        auto stream_id = resp.streams[0].id;

        SequenceContainerPtr sns;
        if (nl_args.produce_args->validate_sns)
            sns.reset(createSequenceContainer(shards));

        auto start = DB::MonotonicSeconds::now();
        ThreadPool pool(nl_args.produce_args->concurrency);

        for (int64_t i = 0; i < nl_args.produce_args->concurrency; ++i)
        {
            pool.scheduleOrThrow([&metrics, &sns, i, shards, stream_id, this]() {
                LOG_INFO(logger, "producer={} starts producing", i);
                auto pstart = DB::MonotonicMilliseconds::now();

                auto share = nl_args.produce_args->num_records / nl_args.produce_args->concurrency;
                if (i == 0)
                    share += nl_args.produce_args->num_records % nl_args.produce_args->concurrency;

                auto ns{nl_args.produce_args->ns};
                nlog::AppendRequest request(nl_args.produce_args->stream, stream_id, 0, createRecord(nl_args.produce_args->record_batch_size));

                for (int64_t j = 0; j < share; ++j)
                {
                    request.stream_shard.shard = j % shards;

                    auto append_resp{server->append(ns, request)};

                    if (sns)
                        sns->at(request.stream_shard.shard)->add(append_resp.sn);

                    /// reset SN to 0
                    request.record->setSN(0);
                    metrics.total_bytes += request.record->totalSerializedBytes();
                    metrics.total_records += 1;
                    metrics.total_events += nl_args.produce_args->record_batch_size;
                }

                auto elapsed = DB::MonotonicMilliseconds::now() - pstart;
                LOG_INFO(logger, "producer={} finishes producing {} records, elapsed={}ms", i, share, elapsed);
            });
        }

        pool.wait();

        auto elapsed = DB::MonotonicSeconds::now() - start;
        if (elapsed == 0)
            elapsed = 1;

        auto record = createRecord(nl_args.produce_args->record_batch_size);

        LOG_INFO(
            logger,
            "\n\n\nProduce {} records with {} events in {} bytes with record_batch_size={} record_size={} threads={}. Overall rps={} "
            "eps={} bps={}, elapsed={} seconds\n\n\n",
            metrics.total_records,
            metrics.total_events,
            metrics.total_bytes,
            nl_args.produce_args->record_batch_size,
            record->ballparkSize(),
            nl_args.produce_args->concurrency,
            metrics.total_records / elapsed,
            metrics.total_events / elapsed,
            metrics.total_bytes / elapsed,
            elapsed);

        if (sns)
            validateSequences(sns, nl_args.produce_args->stream);
    }

    void handleConsumeCommand()
    {
        struct ConsumeMetrics
        {
            std::atomic<uint64_t> total_records = 0;
            std::atomic<uint64_t> total_bytes = 0;
        };
        ConsumeMetrics metrics;

        auto resp{server->listStreams(nl_args.consume_args->ns, nlog::ListStreamsRequest{nl_args.consume_args->stream})};
        if (resp.streams.empty())
            throw DB::Exception(DB::ErrorCodes::RESOURCE_NOT_FOUND, "Stream {} not found", nl_args.consume_args->stream);

        auto shards = resp.streams[0].shards;
        auto stream_id = resp.streams[0].id;

        SequenceContainerPtr sns;
        if (nl_args.consume_args->validate_sns)
            sns.reset(createSequenceContainer(shards));

        auto start = DB::MonotonicSeconds::now();

        auto concurrency = nl_args.consume_args->single_thread ? 1 : shards;
        ThreadPool pool(concurrency);

        for (int32_t i = 0; i < concurrency; ++i)
        {
            pool.scheduleOrThrow([=, &metrics, &sns, this]() {
                LOG_INFO(logger, "consumer={} starts consuming", i);
                auto pstart = DB::MonotonicMilliseconds::now();

                auto share = nl_args.consume_args->num_records / concurrency;
                if (i == 0)
                    share += nl_args.consume_args->num_records % concurrency;

                auto ns{nl_args.consume_args->ns};
                std::vector<nlog::FetchRequest::FetchDescription> fetch_descs;

                auto bufsize = nl_args.consume_args->buf_size;

                int32_t owned_shards = 1;
                if (concurrency == 1)
                {
                    for (int32_t shard = 0; shard < shards; ++shard)
                        fetch_descs.push_back({nl_args.consume_args->stream, stream_id, shard, nl_args.consume_args->start_sn, bufsize});

                    owned_shards = shards;
                }
                else
                    fetch_descs.push_back({nl_args.consume_args->stream, stream_id, i, nl_args.consume_args->start_sn, bufsize});

                int64_t consumed_records_so_far = 0;
                /// Save the next offset to consume per shard
                std::vector<int64_t> next_sns(shards, 0);

                nlog::FetchRequest request(std::move(fetch_descs));

                int32_t empty_records = 0;
                while (empty_records != owned_shards)
                {
                    LOG_INFO(
                        logger,
                        "Fetching stream={}, shard={}, start_sn={}, max_size={}",
                        request.fetch_descs[0].stream_shard.stream.name,
                        request.fetch_descs[0].stream_shard.shard,
                        request.fetch_descs[0].sn,
                        request.fetch_descs[0].max_size);

                    empty_records = 0;

                    auto fetch_resp{server->fetch(ns, request)};
                    for (const auto & fetch_data : fetch_resp.fetched_data)
                    {
                        assert(fetch_data.data.isValid());
                        if (fetch_data.data.records == nullptr)
                        {
                            /// No data, fetch next
                            next_sns[fetch_data.stream_shard.shard] = fetch_data.data.fetch_offset_metadata.record_sn;
                            ++empty_records;
                            continue;
                        }

                        fetch_data.data.records->applyRecordMetadata(
                            [&](nlog::RecordPtr record, uint64_t) {
                                metrics.total_records += 1;
                                metrics.total_bytes += record->totalSerializedBytes();

                                if (sns)
                                    sns->at(fetch_data.stream_shard.shard)->add(record->getSN());

                                next_sns[fetch_data.stream_shard.shard] = record->getSN() + 1;
                                consumed_records_so_far += 1;
                                return consumed_records_so_far >= share;
                            },
                            {});

                        /// Setup the next offset to consume per shard
                        for (auto & desc : request.fetch_descs)
                            desc.sn = next_sns[desc.stream_shard.shard];
                    }

                    if (consumed_records_so_far >= share)
                        break;
                }

                auto elapsed = DB::MonotonicMilliseconds::now() - pstart;
                LOG_INFO(logger, "consumer={} finishes consuming {} records, elapsed={}ms", i, share, elapsed);
            });
        }

        pool.wait();

        auto elapsed = DB::MonotonicSeconds::now() - start;
        if (elapsed == 0)
            elapsed = 1;

        LOG_INFO(
            logger,
            "\n\n\nConsume {} records with in {} bytes threads={}. Overall eps={} bps={}, elapsed={} "
            "seconds\n\n\n",
            metrics.total_records,
            metrics.total_bytes,
            concurrency,
            metrics.total_records / elapsed,
            metrics.total_bytes / elapsed,
            elapsed);

        if (sns)
            validateSequences(sns, nl_args.consume_args->stream);
    }

    void handleTrimCommand() { }


    SequenceContainer * createSequenceContainer(int32_t shards)
    {
        /// Per shard sns
        auto sequences = new SequenceContainer;
        for (int32_t i = 0; i < shards; ++i)
            sequences->push_back(std::make_shared<SequenceQueue>());

        return sequences;
    }

    void validateSequences(SequenceContainerPtr & sns, const std::string & stream)
    {
        for (int32_t shard = 0; auto & shard_sns : *sns)
        {
            /// Validate sns
            auto sorted_sns{shard_sns->snap()};
            std::sort(sorted_sns.begin(), sorted_sns.end());

            LOG_INFO(
                logger,
                "Start validating sns for stream={} shard={}: first_sn={} last_sn={}",
                stream,
                shard,
                *sorted_sns.begin(),
                *sorted_sns.rbegin());

            for (size_t i = 0; i < sorted_sns.size() - 1; ++i)
            {
                if (sorted_sns[i] + 1 != sorted_sns[i + 1])
                    LOG_FATAL(
                        logger,
                        "Not expected in stream={} shard={}, record index={} sn={}, next record index={} sn={}",
                        stream,
                        shard,
                        i,
                        sorted_sns[i],
                        i + 1,
                        sorted_sns[i + 1]);
            }
            LOG_INFO(logger, "Offset validation succeeded for stream={} shard={} !", stream, shard++);
        }
    }

private:
    nlog::NativeLogArgs nl_args;
    Poco::Logger * logger;

    std::unique_ptr<nlog::NativeLog> server;
};

void run(const nlog::NativeLogArgs & nl_args, Poco::Logger * logger)
{
    using namespace std::chrono_literals;

    NativeLog native_log(nl_args, logger);
    native_log.run();

    std::cout << "\nProcess any key to exit...";
    std::getchar();
}
}

int main(int argc, char ** argv)
{
    auto nl_args = nlog::parseArgs(argc, argv);
    if (!nl_args.valid())
        return 1;

    /// Setup logger
    Poco::AutoPtr<OwnPatternFormatter> pf(new OwnPatternFormatter(true));
    Poco::AutoPtr<DB::OwnFormattingChannel> console_channel(new DB::OwnFormattingChannel(pf, new Poco::ConsoleChannel));
    Poco::Logger::root().setChannel(console_channel);
    Poco::Logger::root().setLevel("debug");

    auto * logger = &Poco::Logger::get("native_log");

    nl_args.log_root_directory = "/root/native_log/log";
    nl_args.meta_root_directory = "/root/native_log/meta";

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
