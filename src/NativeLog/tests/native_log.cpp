#include "native_log_cli_parser.h"

#include <NativeLog/Schemas/MemoryRecords.h>
#include <NativeLog/Server/NativeLogServer.h>

#include <base/ClockUtils.h>
#include <base/logger_useful.h>
#include <loggers/OwnFormattingChannel.h>
#include <loggers/OwnPatternFormatter.h>
#include <Common/Exception.h>

#include <flatbuffers/flatbuffers.h>
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
flatbuffers::Offset<nlog::Record> createRecord(flatbuffers::FlatBufferBuilder & fbb, int64_t record_size, uint16_t offset_delta)
{
    /// Build header
    std::vector<int8_t> head_value{'v', 'a', 'l', 'u', 'e'};
    auto header = nlog::CreateRecordHeader(fbb, fbb.CreateString("key"), fbb.CreateVector(head_value));

    /// Build record
    std::vector<int8_t> key_data{'k', 'e', 'y'};
    std::vector<int8_t> value_data(record_size, 't');

    return nlog::CreateRecord(
        fbb,
        DB::UTCMilliseconds::now(),
        offset_delta,
        fbb.CreateVector(key_data),
        fbb.CreateVector(value_data),
        fbb.CreateVector(std::vector<decltype(header)>{header}));
}

flatbuffers::Offset<nlog::RecordBatch> createBatch(flatbuffers::FlatBufferBuilder & fbb, int64_t record_size, int64_t batch_size)
{
    std::vector<flatbuffers::Offset<nlog::Record>> records;
    for (int64_t offset = 0; offset < batch_size; ++offset)
        records.push_back(createRecord(fbb, record_size, offset));

    /// Build record batch
    return nlog::CreateRecordBatch(
        fbb, 123, nlog::MemoryRecords::FLAGS_MAGIC | 0X8000000000000000 | 0X1, 0, 0, 0, 0, 0, 0, -1, -1, -1, fbb.CreateVector(records));
}

nlog::MemoryRecords generateRecordBatch(int64_t record_size, int64_t record_batch_size)
{
    flatbuffers::FlatBufferBuilder fbb(record_batch_size * record_size + record_size * 16 + 128);
    fbb.ForceDefaults(true);

    auto batch = createBatch(fbb, record_size, record_batch_size);
    nlog::FinishSizePrefixedRecordBatchBuffer(fbb, batch);

    auto payload{fbb.GetBufferSpan()};
    size_t size, offset;
    std::shared_ptr<uint8_t[]> data{fbb.ReleaseRaw(size, offset)};
    return nlog::MemoryRecords(std::move(data), size, std::move(payload));
}

class NativeLog
{
public:
    NativeLog(nlog::NativeLogArgs nl_args_, Poco::Logger * logger_) : nl_args(std::move(nl_args_)), logger(logger_)
    {
        nlog::NativeLogConfig config;
        config.log_dirs = {fs::path{nl_args.log_root_directory}};
        config.meta_dir = nl_args.meta_root_directory;

        LOG_INFO(logger, "native log started in log_dir={} meta_dir={}", nl_args.log_root_directory, nl_args.meta_root_directory);

        server.reset(new nlog::NativeLogServer(config));
        server->startup();
    }

    void run()
    {
        if (nl_args.command == "topic")
            handleTopicCommand();
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
    using OffsetsQueue = nlog::UnboundedQueue<std::pair<int64_t, int64_t>>;
    using OffsetsContainer = std::vector<std::shared_ptr<OffsetsQueue>>;
    using OffsetsContainerPtr = std::unique_ptr<OffsetsContainer>;

    void handleTopicCommand()
    {
        assert(nl_args.topic_args.has_value());

        if (nl_args.topic_args->command == "create")
            handleTopicCreateCommand();
        else if (nl_args.topic_args->command == "delete")
            handleTopicDeleteCommand();
        else if (nl_args.topic_args->command == "list")
            handleTopicListCommand();
        else
        {
            LOG_ERROR(logger, "Unsupported topic command {}", nl_args.topic_args->command);
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unsupported command {}", nl_args.topic_args->command);
        }
    }

    void handleTopicCreateCommand()
    {
        nlog::CreateTopicRequest request;
        request.name = nl_args.topic_args->name;
        request.partitions = nl_args.topic_args->partitions;
        request.replicas = 1;
        request.compacted = nl_args.topic_args->compacted;
        auto response = server->createTopic(nl_args.topic_args->ns, request);
        LOG_INFO(logger, "\n\n\nTopic creation succeeded with response: {}\n\n\n", response.string());
    }

    void handleTopicDeleteCommand()
    {
        nlog::DeleteTopicRequest request;
        request.name = nl_args.topic_args->name;
        auto response = server->deleteTopic(nl_args.topic_args->ns, request);
        LOG_INFO(logger, "\n\n\nTopic deletion succeeded with response: {}\n\n\n", response.string());
    }

    void handleTopicListCommand()
    {
        nlog::ListTopicsRequest request;
        request.topic = nl_args.topic_args->name;
        auto response = server->listTopics(nl_args.topic_args->ns, request);
        for (const auto & topic_info : response.topics)
            LOG_INFO(logger, "\n\n\nTopic: {}\n\n\n", topic_info.string());
    }

    void handleProduceCommand()
    {
        struct ProduceMetrics
        {
            std::atomic<uint64_t> total_records = 0;
            std::atomic<uint64_t> total_bytes = 0;
        };

        ProduceMetrics metrics;

        auto resp{server->listTopics(nl_args.produce_args->ns, {nl_args.produce_args->topic})};
        if (resp.topics.empty())
            throw DB::Exception(DB::ErrorCodes::RESOURCE_NOT_FOUND, "Topic {} not found", nl_args.produce_args->topic);

        auto partitions = resp.topics[0].partitions;

        OffsetsContainerPtr offsets;
        if (nl_args.produce_args->validate_offsets)
            offsets.reset(createOffsetsContainer(partitions));

        auto start = DB::MonotonicSeconds::now();
        ThreadPool pool(nl_args.produce_args->concurrency);

        for (int64_t i = 0; i < nl_args.produce_args->concurrency; ++i)
        {
            pool.scheduleOrThrow([&metrics, &offsets, i, partitions, this]() {
                LOG_INFO(logger, "producer={} starts producing", i);
                auto pstart = DB::MonotonicMilliseconds::now();

                auto share = nl_args.produce_args->num_records / nl_args.produce_args->concurrency;
                if (i == 0)
                    share += nl_args.produce_args->num_records % nl_args.produce_args->concurrency;

                auto num_batches = share / nl_args.produce_args->record_batch_size;
                auto last_batch_size = share % nl_args.produce_args->record_batch_size;

                auto ns{nl_args.produce_args->ns};
                nlog::ProduceRequest request(
                    nl_args.produce_args->topic,
                    0,
                    generateRecordBatch(nl_args.produce_args->record_size, nl_args.produce_args->record_batch_size));

                for (int64_t j = 0; j < num_batches; ++j)
                {
                    request.partition = j % partitions;

                    auto produce_resp{server->produce(ns, request)};

                    if (offsets)
                        offsets->at(request.partition)
                            ->add(std::pair<int64_t, int64_t>(produce_resp.first_offset, produce_resp.last_offset));

                    /// Since NativeLog validates that base offset shall be zero
                    request.batch.setBaseOffset(0);
                    metrics.total_bytes += request.batch.sizeInBytes();
                    metrics.total_records += nl_args.produce_args->record_batch_size;
                }

                if (last_batch_size)
                {
                    nlog::ProduceRequest last_request(
                        nl_args.produce_args->topic, 0, generateRecordBatch(nl_args.produce_args->record_size, last_batch_size));
                    last_request.partition = (request.partition + 1) % partitions;
                    auto produce_resp{server->produce(ns, last_request)};

                    if (offsets)
                        offsets->at(request.partition)
                            ->add(std::pair<int64_t, int64_t>(produce_resp.first_offset, produce_resp.last_offset));

                    metrics.total_bytes += last_request.batch.sizeInBytes();
                    metrics.total_records += last_batch_size;
                }

                auto elapsed = DB::MonotonicMilliseconds::now() - pstart;
                LOG_INFO(logger, "producer={} finishes producing {} records, elapsed={}ms", i, share, elapsed);
            });
        }

        pool.wait();

        auto elapsed = DB::MonotonicSeconds::now() - start;
        if (elapsed == 0)
            elapsed = 1;

        LOG_INFO(
            logger,
            "\n\n\nProduce {} records in {} bytes with record_batch_size={} record_size={} threads={}. Overall eps={} bps={}, elapsed={} "
            "seconds\n\n\n",
            metrics.total_records,
            metrics.total_bytes,
            nl_args.produce_args->record_batch_size,
            nl_args.produce_args->record_size,
            nl_args.produce_args->concurrency,
            metrics.total_records / elapsed,
            metrics.total_bytes / elapsed,
            elapsed);

        if (offsets)
            validateOffsets(offsets, nl_args.produce_args->topic);
    }

    void handleConsumeCommand()
    {
        struct ConsumeMetrics
        {
            std::atomic<uint64_t> total_records = 0;
            std::atomic<uint64_t> total_bytes = 0;
        };
        ConsumeMetrics metrics;

        auto resp{server->listTopics(nl_args.consume_args->ns, {nl_args.consume_args->topic})};
        if (resp.topics.empty())
            throw DB::Exception(DB::ErrorCodes::RESOURCE_NOT_FOUND, "Topic {} not found", nl_args.consume_args->topic);

        auto partitions = resp.topics[0].partitions;

        OffsetsContainerPtr offsets;
        if (nl_args.consume_args->validate_offsets)
            offsets.reset(createOffsetsContainer(partitions));

        auto start = DB::MonotonicSeconds::now();

        auto concurrency = nl_args.consume_args->single_thread ? 1 : partitions;
        ThreadPool pool(concurrency);

        for (int32_t i = 0; i < concurrency; ++i)
        {
            pool.scheduleOrThrow([=, &metrics, &offsets, this]() {
                LOG_INFO(logger, "consumer={} starts consuming", i);
                auto pstart = DB::MonotonicMilliseconds::now();

                auto share = nl_args.consume_args->num_records / concurrency;
                if (i == 0)
                    share += nl_args.consume_args->num_records % concurrency;

                auto ns{nl_args.consume_args->ns};
                nlog::FetchRequest request;

                auto bufsize = nl_args.consume_args->buf_size;

                if (concurrency == 1)
                {
                    for (int32_t partition = 0; partition < partitions; ++partition)
                        request.offsets.push_back({nl_args.consume_args->topic, partition, nl_args.consume_args->start_offset, bufsize});
                }
                else
                    request.offsets.push_back({nl_args.consume_args->topic, i, nl_args.consume_args->start_offset, bufsize});

                int64_t consumed_records_so_far = 0;
                /// Save the next offset to consume per partition
                std::vector<int64_t> next_offsets(partitions, 0);

                while (1)
                {
                    LOG_INFO(
                        logger,
                        "Fetching topic={}, partition={}, start_offset={}, max_size={}",
                        request.offsets[0].topic,
                        request.offsets[0].partition,
                        request.offsets[0].offset,
                        request.offsets[0].max_size);

                    auto fetch_resp{server->fetch(ns, request)};
                    for (const auto & fetch_data : fetch_resp.data)
                    {
                        assert(fetch_data.data.isValid());
                        if (fetch_data.data.records == nullptr)
                        {
                            /// No data, fetch next
                            next_offsets[fetch_data.partition] = fetch_data.data.fetch_offset_metadata.message_offset;
                            continue;
                        }

                        fetch_data.data.records->apply(
                            [&](const nlog::MemoryRecords & records, uint64_t) {
                                metrics.total_records += records.records().size();
                                metrics.total_bytes += records.sizeInBytes();

                                if (offsets)
                                    offsets->at(fetch_data.partition)
                                        ->add(std::pair<int64_t, int64_t>(records.baseOffset(), records.lastOffset()));

                                next_offsets[fetch_data.partition] = records.nextOffset();
                                consumed_records_so_far += records.records().size();
                                return consumed_records_so_far >= share;
                            },
                            {},
                            bufsize);

                        /// Setup the next offset to consume per partition
                        for (auto & offset : request.offsets)
                            offset.offset = next_offsets[offset.partition];
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
            "\n\n\nConsume {} records in {} bytes threads={}. Overall eps={} bps={}, elapsed={} "
            "seconds\n\n\n",
            metrics.total_records,
            metrics.total_bytes,
            concurrency,
            metrics.total_records / elapsed,
            metrics.total_bytes / elapsed,
            elapsed);

        if (offsets)
            validateOffsets(offsets, nl_args.consume_args->topic);
    }

    void handleTrimCommand() { }


    OffsetsContainer * createOffsetsContainer(int32_t partitions)
    {
        /// Per partition offsets
        auto offsets = new OffsetsContainer;
        for (int32_t i = 0; i < partitions; ++i)
            offsets->push_back(std::make_shared<OffsetsQueue>());

        return offsets;
    }

    void validateOffsets(OffsetsContainerPtr & offsets, const std::string & topic)
    {
        for (int32_t partition = 0; auto & partition_offsets : *offsets)
        {
            /// Validate offsets
            auto sorted_offsets{partition_offsets->snap()};
            std::sort(sorted_offsets.begin(), sorted_offsets.end());

            LOG_INFO(
                logger,
                "Start validating offsets for topic={} partition={}: first_offset={} last_offset={}",
                topic,
                partition,
                sorted_offsets.begin()->first,
                sorted_offsets.rbegin()->second);

            for (size_t i = 0; i < sorted_offsets.size() - 1; ++i)
            {
                if (sorted_offsets[i].second + 1 != sorted_offsets[i + 1].first)
                    LOG_FATAL(
                        logger,
                        "Not expected in topic={} partition={}, batch index={} first_offset={} last_offset={}, next batch index={} "
                        "first_offset={} "
                        "last_offset={}",
                        topic,
                        partition,
                        i,
                        sorted_offsets[i].first,
                        sorted_offsets[i].second,
                        i + 1,
                        sorted_offsets[i + 1].first,
                        sorted_offsets[i + 1].second);
            }
            LOG_INFO(logger, "Offset validation succeeded for topic={} partition={} !", topic, partition++);
        }
    }

private:
    nlog::NativeLogArgs nl_args;
    Poco::Logger * logger;

    std::unique_ptr<nlog::NativeLogServer> server;
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
