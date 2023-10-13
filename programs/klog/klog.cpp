#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <KafkaLog/KafkaWAL.h>
#include <KafkaLog/KafkaWALConsumer.h>
#include <KafkaLog/KafkaWALConsumerMultiplexer.h>
#include <KafkaLog/KafkaWALContext.h>
#include <KafkaLog/KafkaWALSettings.h>
#include <Common/ProtonCommon.h>
#include <Common/TerminalSize.h>
#include <Common/ThreadPool.h>

#include <boost/program_options.hpp>

#include <fstream>

using namespace std;
using namespace DB;
using namespace nlog;
using namespace klog;

namespace
{
Block prepareData(Int32 batch_size)
{
    Block block;

    auto uint64_type = make_shared<DataTypeUInt64>();
    auto float64_type = make_shared<DataTypeFloat64>();
    auto datetime64_type = make_shared<DataTypeDateTime64>(3);
    auto string_type = make_shared<DataTypeString>();

    auto id_col = uint64_type->createColumn();
    /// auto id_col = make_shared<ColumnInt64>();
    auto id_col_inner = typeid_cast<ColumnUInt64 *>(id_col.get());
    for (Int32 i = 0; i < batch_size; ++i)
    {
        id_col_inner->insertValue(i);
    }

    ColumnWithTypeAndName id_col_with_type{std::move(id_col), uint64_type, "id"};
    block.insert(id_col_with_type);

    auto cpu_col = float64_type->createColumn();
    /// auto cpu_col = make_shared<ColumnFloat64>();
    auto cpu_col_inner = typeid_cast<ColumnFloat64 *>(cpu_col.get());
    for (Int32 i = 0; i < batch_size; ++i)
    {
        cpu_col_inner->insertValue(13.338 + i);
    }

    ColumnWithTypeAndName cpu_col_with_type(std::move(cpu_col), float64_type, "cpu");
    block.insert(cpu_col_with_type);

    String log{"2021.01.13 03:48:02.311031 [ 4070  ] {} <Information> Application: It looks like the process has no CAP_IPC_LOCK "
               "capability, binary mlock will be disabled. It could happen due to incorrect proton package installation. You could resolve "
               "the problem manually with 'sudo setcap cap_ipc_lock=+ep /home/ghost/code/private-daisy/build/programs/proton '. Note     "
               "that it will not work on 'nosuid' mounted filesystems"};
    auto raw_col = string_type->createColumn();
    for (Int32 i = 0; i < batch_size; ++i)
    {
        raw_col->insertData(log.data(), log.size());
    }

    ColumnWithTypeAndName raw_col_with_type(std::move(raw_col), string_type, "raw");
    block.insert(raw_col_with_type);

    auto time_col = datetime64_type->createColumn();
    /// auto time_col = make_shared<ColumnDecimal<DateTime64>>;
    auto time_col_inner = typeid_cast<ColumnDecimal<DateTime64> *>(time_col.get());

    for (Int32 i = 0; i < batch_size; ++i)
    {
        time_col_inner->insertValue(static_cast<Int64>(1612286044.256326 + static_cast<double>(i)));
    }

    ColumnWithTypeAndName time_col_with_type(std::move(time_col), datetime64_type, ProtonConsts::RESERVED_EVENT_TIME);
    block.insert(time_col_with_type);

    return block;
}

void dumpData(Block & block)
{
    for (size_t idx = 0; idx < block.rows(); ++idx)
    {
        for (auto & col : block)
        {
            const auto type = col.type->getName();
            if (type == "float64")
            {
                std::cout << col.name << "=" << col.column->getFloat64(idx) << "\n";
            }
            else if (type == "uint64")
            {
                std::cout << col.name << "=" << col.column->getUInt(idx) << "\n";
            }
            else if (type == "datetime64")
            {
                std::cout << col.name << "=" << col.column->getFloat64(idx) << "\n";
            }
            else if (type == "string")
            {
                std::cout << col.name << "=" << col.column->getDataAt(idx) << "\n";
            }
        }
    }
}

struct ProducerSettings
{
    Int32 request_required_acks = 1;
    Int32 concurrency = 1;
    Int32 iterations = 1;
    Int32 batch_size = 1;
    Int32 wal_client_pool_size = 1;
    String mode = "sync";

    String topic;
};

struct ConsumerSettings
{
    vector<TopicPartitionOffset> kafka_topic_partition_offsets;
    String auto_offset_reset = "earliest";
    Int32 consume_callback_max_messages = 1000;
    Int32 max_messages = 100;
    Int32 auto_commit_interval_ms = 5000;
    Int32 wal_client_pool_size = 1;
    String mode = "sync";
    bool incremental = false;
    bool dumpdata = false;

    static vector<TopicPartitionOffset> parseTopicPartitionOffsets(const String & s)
    {
        vector<TopicPartitionOffset> tops;

        TopicPartitionOffset tpo;

        Int32 state = 0;
        auto iter = s.begin();
        auto prev = iter;

        for (; iter != s.end(); ++iter)
        {
            if (*iter == ',')
            {
                String token{prev, iter};
                if (state == 0)
                {
                    tpo.topic = token;
                    prev = iter + 1;
                    state = 1;
                }
                else if (state == 1)
                {
                    tpo.partition = stoi(token);
                    prev = iter + 1;
                    state = 2;
                }
                else
                {
                    return {};
                }
            }
            else if (*iter == ':')
            {
                if (state == 2)
                {
                    String token{prev, iter};
                    tpo.offset = stoll(token);
                    tops.push_back(tpo);

                    prev = iter + 1;
                    state = 0;
                }
                else
                {
                    return {};
                }
            }
        }

        if (prev != iter)
        {
            if (state == 2)
            {
                String token{prev, iter};
                tpo.offset = stoll(token);
                tops.push_back(tpo);
            }
            else
            {
                return {};
            }
        }

        return tops;
    }
};

struct AdminTopicSettings
{
    String mode;
    String name;
    Int32 partitions;
    Int32 replication_factor;
};

struct BenchmarkSettings
{
    String command = "produce";

    ProducerSettings producer_settings;
    ConsumerSettings consumer_settings;
    AdminTopicSettings topic_settings;

    unique_ptr<KafkaWALSettings> wal_settings;

    bool exit = true;
};

BenchmarkSettings parseProduceSettings(po::parsed_options & cmd_parsed, const char * progname)
{
    using boost::program_options::value;

    po::options_description desc = createOptionsDescription("produce options", getTerminalWidth());

    auto options = desc.add_options();
    options("help", "help message");
    options("concurrency", value<Int32>()->default_value(1), "number of parallel ingestion");
    options("iterations", value<Int32>()->default_value(1), "number of iterations");
    options("batch_size", value<Int32>()->default_value(100), "number of rows in one Kafka message");
    options("wal_client_pool_size", value<Int32>()->default_value(1), "WAL client pool size");
    options("mode", value<String>()->default_value("sync"), "sync or async data ingestion");

    options("message_send_max_retries", value<Int32>()->default_value(2), "number of retries when ingestion failed");
    options("retry_backoff_ms", value<Int32>()->default_value(100), "backoff time between retry");
    options("enable_idempotence", value<bool>()->default_value(true), "idempotently ingest data into Kafka");
    options("request_required_acks", value<Int32>()->default_value(1), "number of acks to wait per Kafka message");
    options(
        "queue_buffering_max_messages",
        value<Int32>()->default_value(1),
        "number of message to buffer in client before sending to Kafka brokers");
    options(
        "queue_buffering_max_ms",
        value<Int32>()->default_value(1),
        "max time to buffer message on client side before sending to Kafka brokers");
    options(
        "message_delivery_async_poll_ms",
        value<Int32>()->default_value(100),
        "interval to poll delivery report for ingested message async");
    options(
        "message_delivery_sync_poll_ms", value<Int32>()->default_value(10), "interval to poll delivery report for ingested message sync");
    options("compression_codec", value<String>()->default_value("snappy"), "none,gzip,snappy,lz4,zstd,inherit");
    options("kafka_brokers", value<String>()->default_value("localhost:9092"), "Kafka broker lists");
    options("kafka_topic", value<String>()->default_value("daisy"), "Kafka topic");
    options("debug", value<String>()->default_value(""), "librdkafka components to debug, broker,topic,msg");

    vector<String> opts = po::collect_unrecognized(cmd_parsed.options, po::include_positional);
    opts.erase(opts.begin());

    /// Parse `produce` args
    po::variables_map option_map;
    try
    {
        po::store(po::command_line_parser(opts).options(desc).run(), option_map);
    }
    catch (...)
    {
        cerr << getCurrentExceptionMessage(false, true) << endl;
        cout << "Usage: " << progname << " " << desc << "\n";
        return {};
    }

    if (option_map.count("help"))
    {
        cout << "Usage: " << progname << " " << desc << "\n";
        return {};
    }

    auto mode = option_map["mode"].as<String>();
    if (mode != "async" && mode != "sync")
    {
        cout << "Usage: " << progname << " " << desc << "\n";
        return {};
    }

    auto settings = make_unique<KafkaWALSettings>();
    settings->brokers = option_map["kafka_brokers"].as<String>();
    settings->retry_backoff_ms = option_map["retry_backoff_ms"].as<Int32>();
    settings->message_send_max_retries = option_map["message_send_max_retries"].as<Int32>();
    settings->queue_buffering_max_messages = option_map["queue_buffering_max_messages"].as<Int32>();
    settings->queue_buffering_max_ms = option_map["queue_buffering_max_ms"].as<Int32>();
    settings->message_delivery_async_poll_ms = option_map["message_delivery_async_poll_ms"].as<Int32>();
    settings->message_delivery_sync_poll_ms = option_map["message_delivery_sync_poll_ms"].as<Int32>();
    settings->enable_idempotence = option_map["enable_idempotence"].as<bool>();
    settings->compression_codec = option_map["compression_codec"].as<String>();
    settings->debug = option_map["debug"].as<String>();

    BenchmarkSettings bench_settings;

    bench_settings.wal_settings = std::move(settings);
    bench_settings.producer_settings.request_required_acks = option_map["request_required_acks"].as<Int32>();
    bench_settings.producer_settings.topic = option_map["kafka_topic"].as<String>();
    bench_settings.producer_settings.wal_client_pool_size = option_map["wal_client_pool_size"].as<Int32>();
    bench_settings.producer_settings.iterations = option_map["iterations"].as<Int32>();
    bench_settings.producer_settings.batch_size = option_map["batch_size"].as<Int32>();
    bench_settings.producer_settings.concurrency = option_map["concurrency"].as<Int32>();
    bench_settings.producer_settings.mode = mode;

    bench_settings.command = "produce";
    bench_settings.exit = false;

    return bench_settings;
}

BenchmarkSettings parseConsumeSettings(po::parsed_options & cmd_parsed, const char * progname)
{
    using boost::program_options::value;

    po::options_description desc = createOptionsDescription("consume options", getTerminalWidth());

    auto options = desc.add_options();
    options("help", "help message");
    options("group_id", value<String>(), "consumer group id");
    options("auto_offset_reset", value<String>()->default_value("earliest"), "earliest|latest|stored");
    options("queued_min_messages", value<Int32>()->default_value(10000), "number of queued messages in client side");
    options("kafka_topic_partition_offsets", value<String>(), "topic,partition,offset:topic,partition,offset:...");
    options("max_messages", value<Int32>()->default_value(100), "maximum message to consume");
    options("auto_commit_interval_ms", value<Int32>()->default_value(5000), "offsets commit interval");
    options("wal_client_pool_size", value<Int32>()->default_value(1), "WAL client pool size");
    options("mode", value<String>()->default_value("sync"), "sync or async data consumption");
    options("kafka_brokers", value<String>()->default_value("localhost:9092"), "Kafka broker lists");
    options("debug", value<String>()->default_value(""), "librdkafka components to debug, cgrp,topic,fetch");
    options("incremental", value<bool>()->default_value(false), "incremental topic/partition consuming");
    options("dumpdata", value<bool>()->default_value(false), "dump the parsed data to console");

    vector<String> opts = po::collect_unrecognized(cmd_parsed.options, po::include_positional);
    opts.erase(opts.begin());

    /// Parse `produce` args
    po::variables_map option_map;
    try
    {
        po::store(po::command_line_parser(opts).options(desc).run(), option_map);
    }
    catch (...)
    {
        cerr << getCurrentExceptionMessage(false, true) << endl;
        cout << "Usage: " << progname << " " << desc << "\n";
        return {};
    }

    if (option_map.count("help"))
    {
        cout << "Usage: " << progname << " " << desc << "\n";
        return {};
    }

    auto mode = option_map["mode"].as<String>();
    if (mode != "async" && mode != "sync")
    {
        cout << "Usage: " << progname << " " << desc << "\n";
        return {};
    }

    for (const auto & option : {"group_id", "kafka_topic_partition_offsets"})
    {
        if (!option_map.count(option) || option_map[option].as<String>().empty())
        {
            cout << "`--" << option << "` argument is required.\n";
            cout << "Usage: " << progname << " " << desc << "\n";
            return {};
        }
    }

    auto settings = make_unique<KafkaWALSettings>();
    settings->brokers = option_map["kafka_brokers"].as<String>();
    settings->group_id = option_map["group_id"].as<String>();
    settings->queued_min_messages = option_map["queued_min_messages"].as<Int32>();
    settings->auto_commit_interval_ms = option_map["auto_commit_interval_ms"].as<Int32>();
    settings->debug = option_map["debug"].as<String>();

    /// parse topic, partition, offsets
    BenchmarkSettings bench_settings;

    bench_settings.wal_settings = std::move(settings);
    bench_settings.consumer_settings.auto_offset_reset = option_map["auto_offset_reset"].as<String>();
    if (bench_settings.consumer_settings.auto_offset_reset == "stored")
    {
        bench_settings.consumer_settings.auto_offset_reset = "-1000";
    }
    bench_settings.consumer_settings.consume_callback_max_messages = option_map["max_messages"].as<Int32>();
    bench_settings.consumer_settings.max_messages = option_map["max_messages"].as<Int32>();
    bench_settings.consumer_settings.wal_client_pool_size = option_map["wal_client_pool_size"].as<Int32>();
    bench_settings.consumer_settings.mode = option_map["mode"].as<String>();
    bench_settings.consumer_settings.kafka_topic_partition_offsets
        = ConsumerSettings::parseTopicPartitionOffsets(option_map["kafka_topic_partition_offsets"].as<String>());
    bench_settings.consumer_settings.incremental = option_map["incremental"].as<bool>();
    bench_settings.consumer_settings.dumpdata = option_map["dumpdata"].as<bool>();

    if (bench_settings.consumer_settings.kafka_topic_partition_offsets.empty())
    {
        cout << "`--kafka_topic_partition_offsets` argument " << option_map["kafka_topic_partition_offsets"].as<String>()
             << " is invalid.\n";
        cout << "Usage: " << progname << " " << desc << "\n";

        return {};
    }

    bench_settings.exit = false;
    bench_settings.command = "consume";
    return bench_settings;
}

BenchmarkSettings parseTopicSettings(po::parsed_options & cmd_parsed, const char * progname)
{
    using boost::program_options::value;

    po::options_description desc = createOptionsDescription("topic options", getTerminalWidth());

    auto options = desc.add_options();
    options("help", "help message");
    options("mode", value<String>()->default_value("create"), "create or delete or describe Kafka topic");
    options("partitions", value<Int32>()->default_value(1), "number of partitions");
    options("replication_factor", value<Int32>()->default_value(1), "number of replicas per partition");
    options("name", value<String>(), "Kafka topic name");
    options("kafka_brokers", value<String>()->default_value("localhost:9092"), "Kafka broker lists");
    options("debug", value<String>()->default_value(""), "librdkafka components to debug, broker,topic,msg");

    vector<String> opts = po::collect_unrecognized(cmd_parsed.options, po::include_positional);
    opts.erase(opts.begin());

    /// Parse `topic` args
    po::variables_map option_map;
    try
    {
        po::store(po::command_line_parser(opts).options(desc).run(), option_map);
    }
    catch (...)
    {
        cerr << getCurrentExceptionMessage(false, true) << endl;
        cout << "Usage: " << progname << " " << desc << "\n";
        return {};
    }

    if (option_map.count("help"))
    {
        cout << "Usage: " << progname << " " << desc << "\n";
        return {};
    }

    auto mode = option_map["mode"].as<String>();
    if (mode != "create" && mode != "delete" && mode != "describe")
    {
        cout << "Usage: " << progname << " " << desc << "\n";
        return {};
    }

    if (!option_map.count("name") || option_map["name"].as<String>().empty())
    {
        cout << "`--name` argument is required.\n";
        cout << "Usage: " << progname << " " << desc << "\n";
        return {};
    }

    auto settings = make_unique<KafkaWALSettings>();
    settings->brokers = option_map["kafka_brokers"].as<String>();
    settings->debug = option_map["debug"].as<String>();

    BenchmarkSettings bench_settings;

    bench_settings.wal_settings = std::move(settings);
    bench_settings.topic_settings.mode = mode;
    bench_settings.topic_settings.name = option_map["name"].as<String>();
    bench_settings.topic_settings.partitions = option_map["partitions"].as<Int32>();
    bench_settings.topic_settings.replication_factor = option_map["replication_factor"].as<Int32>();

    bench_settings.command = "topic";
    bench_settings.exit = false;

    return bench_settings;
}

BenchmarkSettings parseArgs(int argc, char ** argv)
{
    namespace po = boost::program_options;
    using boost::program_options::value;

    po::options_description global = createOptionsDescription("Global options", getTerminalWidth());
    auto cmds = global.add_options();
    cmds("command", value<String>()->default_value("help"), "<produce|consume|topic> subcommand to execute");
    cmds("subargs", value<vector<String>>(), "Arguments for subcommand");

    po::positional_options_description pos;
    pos.add("command", 1).add("subargs", -1);

    po::variables_map option_map;
    po::parsed_options cmd_parsed = po::command_line_parser(argc, argv).options(global).positional(pos).allow_unregistered().run();
    po::store(cmd_parsed, option_map);

    if (option_map.count("help"))
    {
        cout << argv[0] << " <produce | consume | topic> <args|--help>" << endl;
        return {};
    }

    String cmd = option_map["command"].as<String>();
    if (cmd == "produce")
    {
        return parseProduceSettings(cmd_parsed, argv[0]);
    }
    else if (cmd == "consume")
    {
        return parseConsumeSettings(cmd_parsed, argv[0]);
    }
    else if (cmd == "topic")
    {
        return parseTopicSettings(cmd_parsed, argv[0]);
    }
    else
    {
        cout << argv[0] << " <produce | consume | topic> <args|--help>" << endl;
        return {};
    }
}

String calculateFileName(const BenchmarkSettings & settings)
{
    String filename = "kafka_wal_latency";
    filename += "_concurrency_" + to_string(settings.producer_settings.concurrency);
    filename += "_batch_size_" + to_string(settings.producer_settings.batch_size);
    filename += "_iteration_" + to_string(settings.producer_settings.iterations);
    filename += "_idem_" + to_string(settings.wal_settings->enable_idempotence);
    filename += "_required_acks_" + to_string(settings.producer_settings.request_required_acks);
    filename += "_q_buf_max_ms_" + to_string(settings.wal_settings->queue_buffering_max_ms);
    filename += "_q_buf_max_msg_" + to_string(settings.wal_settings->queue_buffering_max_messages);
    filename += "_msg_delivery_sync_poll_ms_" + to_string(settings.wal_settings->message_delivery_sync_poll_ms);
    filename += "_msg_delivery_async_poll_ms_" + to_string(settings.wal_settings->message_delivery_async_poll_ms);
    filename += "_compression_codec_" + settings.wal_settings->compression_codec;
    filename += ".txt";
    return filename;
}

using ResultQueue = vector<Int32>;
using ResultQueues = vector<ResultQueue>;
using TimePoint = chrono::time_point<chrono::steady_clock>;
using RecordContainer = unordered_map<UInt64, pair<shared_ptr<nlog::Record>, TimePoint>>;

KafkaWALPtrs createDWals(const BenchmarkSettings & bench_settings, Int32 size)
{
    KafkaWALPtrs wals;
    wals.reserve(size);
    for (Int32 i = 0; i < size; ++i)
    {
        auto settings = make_unique<KafkaWALSettings>();
        /// make a copy
        *settings = *bench_settings.wal_settings;
        wals.push_back(make_shared<KafkaWAL>(std::move(settings)));
        wals.back()->startup();
    }
    return wals;
}

struct Data : public klog::ConsumeCallbackData
{
    mutex & cmutex;
    RecordContainer & inflights;
    ResultQueue & result_queue;
    Int32 & total;
    Int32 & failed;
    UInt64 correlation_id;
    DB::Block header;

    Data(
        mutex & cmutex_, RecordContainer & inflights_, ResultQueue & result_queue_, Int32 & total_, Int32 & failed_, UInt64 correlation_id_)
        : cmutex(cmutex_)
        , inflights(inflights_)
        , result_queue(result_queue_)
        , total(total_)
        , failed(failed_)
        , correlation_id(correlation_id_)
        , header(prepareData(0))
    {
    }

    const DB::Block & getSchema(UInt16) const override { return header; }
};

void ingestAsync(KafkaWALPtr & wal, ResultQueue & result_queue, mutex & stdout_mutex, const BenchmarkSettings & bench_settings)
{
    auto callback = [](const auto & result, const auto & data) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        Data * d = static_cast<Data *>(data.get());

        TimePoint start;
        {
            lock_guard<mutex> lock(d->cmutex);

            d->total += 1;
            if (result.err)
            {
                d->failed++;
            }

            auto iter = d->inflights.find(d->correlation_id);
            assert(iter != d->inflights.end());
            start = iter->second.second;
            d->inflights.erase(iter);
        }

        auto latency = chrono::duration_cast<chrono::microseconds>(chrono::steady_clock::now() - start).count();
        d->result_queue.push_back(static_cast<Int32>(latency));

        delete d;
    };

    Int32 failed = 0;
    Int32 total = 0;

    KafkaWALContext ctx{bench_settings.producer_settings.topic};
    ctx.request_required_acks = bench_settings.producer_settings.request_required_acks;

    auto result = wal->describe(bench_settings.producer_settings.topic);
    if (result.err)
    {
        cout << "Failed to describe ingest topic=" << bench_settings.producer_settings.topic << " error=" << result.err << "\n";
        return;
    }

    mutex cmutex;
    RecordContainer inflights;

    for (Int32 i = 0; i < bench_settings.producer_settings.iterations; ++i)
    {
        auto record
            = make_shared<nlog::Record>(OpCode::ADD_DATA_BLOCK, prepareData(bench_settings.producer_settings.batch_size), nlog::NO_SCHEMA);
        record->setShard(i % result.partitions);
        record->addHeader("_idem", to_string(i));

        shared_ptr<Data> data{new Data(cmutex, inflights, result_queue, total, failed, i)};

        auto item = make_pair(record, chrono::steady_clock::now());
        {
            lock_guard<mutex> lock(cmutex);
            inflights.emplace(i, item);
        }

        Int32 err = wal->append(*record.get(), std::move(callback), std::move(data), ctx);
        if (err)
        {
            lock_guard<mutex> lock(cmutex);
            inflights.erase(i);
            failed++;
        }
    }

    /// We need poll-wait all inflights result to be delivered
    while (true)
    {
        {
            lock_guard<mutex> lock(cmutex);
            if (total + failed == bench_settings.producer_settings.iterations)
            {
                break;
            }
        }

        {
            lock_guard<mutex> lock(stdout_mutex);
            cout << "thread id=" << this_thread::get_id() << " polling: " << bench_settings.producer_settings.iterations - total - failed
                 << " records\n";
        }
        this_thread::sleep_for(1000ms);
    }

    {
        lock_guard<mutex> lock(stdout_mutex);
        cout << "thread id=" << this_thread::get_id() << " total=" << total << ", failed=" << failed << "\n";
    }
}

void ingestSync(KafkaWALPtr & wal, ResultQueue & result_queue, mutex & stdout_mutex, const BenchmarkSettings & bench_settings)
{
    Int32 failed = 0;

    KafkaWALContext ctx{bench_settings.producer_settings.topic};
    ctx.request_required_acks = bench_settings.producer_settings.request_required_acks;

    auto dresult = wal->describe(bench_settings.producer_settings.topic);
    if (dresult.err)
    {
        cout << "Failed to describe ingest topic=" << bench_settings.producer_settings.topic << " error=" << dresult.err << "\n";
        return;
    }

    for (Int32 i = 0; i < bench_settings.producer_settings.iterations; ++i)
    {
        Record record{OpCode::ADD_DATA_BLOCK, prepareData(bench_settings.producer_settings.batch_size), nlog::NO_SCHEMA};
        record.setShard(i % dresult.partitions);
        record.addHeader("_idem", to_string(i));

        auto start = chrono::steady_clock::now();
        const AppendResult & result = wal->append(record, ctx);
        if (result.err)
        {
            failed++;
            continue;
        }

        auto latency = chrono::duration_cast<chrono::microseconds>(std::chrono::steady_clock::now() - start).count();
        result_queue.push_back(static_cast<Int32>(latency));
        /// cout << "producing record with sequence number : " << result.sn << " (partition, partition_key)=" << result.partitions << ":" << i << "\n";
    }

    if (failed)
    {
        lock_guard<mutex> lock(stdout_mutex);
        cout << "thread id=" << this_thread::get_id() << " failed: " << failed << "\n";
    }
}

void doIngest(KafkaWALPtr & wal, ResultQueue & result_queue, mutex & stdout_mutex, const BenchmarkSettings & bench_settings)
{
    if (bench_settings.producer_settings.mode == "sync")
    {
        ingestSync(wal, result_queue, stdout_mutex, bench_settings);
    }
    else
    {
        ingestAsync(wal, result_queue, stdout_mutex, bench_settings);
    }
}

Int64 ingest(KafkaWALPtrs & wals, ResultQueues & result_queues, const BenchmarkSettings & bench_settings)
{
    auto bench_start = chrono::steady_clock::now();
    ThreadPool worker_pool{static_cast<size_t>(bench_settings.producer_settings.concurrency)};
    mutex stdout_mutex;

    for (Int32 jobid = 0; jobid < bench_settings.producer_settings.concurrency; ++jobid)
    {
        result_queues[jobid].reserve(bench_settings.producer_settings.iterations);
        worker_pool.scheduleOrThrowOnError([&, jobid] {
            {
                lock_guard<mutex> lock(stdout_mutex);
                cout << "thread id=" << this_thread::get_id() << " got jobid=" << jobid << " and grabbed walid=" << jobid % wals.size()
                     << " to ingest data\n";
            }

            auto & result_queue = result_queues[jobid];
            auto & wal = wals[jobid % wals.size()];

            doIngest(wal, result_queue, stdout_mutex, bench_settings);
        });
    }

    worker_pool.wait();
    cout << "data ingestion is done" << endl;
    return chrono::duration_cast<chrono::microseconds>(chrono::steady_clock::now() - bench_start).count();
}

struct ConsumeContext : public klog::ConsumeCallbackData
{
    mutex & stdout_mutex;
    atomic_uint64_t & consumed;
    KafkaWALContext & ctx;
    KafkaWALPtr & dwal;
    bool dumpdata;
    DB::Block header;

    ConsumeContext(mutex & stdout_mutex_, atomic_uint64_t & consumed_, KafkaWALContext & ctx_, KafkaWALPtr & dwal_, bool dumpdata_)
        : stdout_mutex(stdout_mutex_), consumed(consumed_), ctx(ctx_), dwal(dwal_), dumpdata(dumpdata_), header(prepareData(0))
    {
    }

    const DB::Block & getSchema(UInt16) const override { return header; }
};

void doConsume(nlog::RecordPtrs records, klog::ConsumeCallbackData * data)
{
    if (records.empty())
    {
        return;
    }

    auto cctx = dynamic_cast<ConsumeContext *>(data);

    cctx->consumed += records.size();
    for (const auto & record : records)
    {
        lock_guard<mutex> lock(cctx->stdout_mutex);

        cout << "partition=" << record->getShard() << " offset=" << record->getSN() << " idem=" << record->getHeader("_idem") << endl;

        if (cctx->dumpdata)
        {
            dumpData(record->getBlock());
        }
    }

    cctx->dwal->commit(records.back()->getSN(), cctx->ctx);
}

void consume(KafkaWALPtrs & wals, const BenchmarkSettings & bench_settings)
{
    mutex stdout_mutex;
    ThreadPool worker_pool{bench_settings.consumer_settings.kafka_topic_partition_offsets.size()};

    auto table_schema{prepareData(0)};

    for (size_t jobid = 0; jobid < bench_settings.consumer_settings.kafka_topic_partition_offsets.size(); ++jobid)
    {
        worker_pool.scheduleOrThrowOnError([&, jobid] {
            auto & wal = wals[jobid % wals.size()];
            auto & tpo = bench_settings.consumer_settings.kafka_topic_partition_offsets[jobid];
            auto dumpdata = bench_settings.consumer_settings.dumpdata;

            KafkaWALContext ctx{tpo.topic, tpo.partition, tpo.offset};
            ctx.auto_offset_reset = bench_settings.consumer_settings.auto_offset_reset;
            ctx.consume_callback_max_messages = bench_settings.consumer_settings.max_messages;
            // ctx.schemas.push_back(table_schema); ? error: no member named 'schemas' in 'DWAL::KafkaWALContext'

            atomic_uint64_t consumed = 0;
            uint64_t batch = 100;
            uint64_t max_messages = bench_settings.consumer_settings.max_messages;

            ConsumeContext cctx{stdout_mutex, consumed, ctx, wal, dumpdata};

            if (bench_settings.consumer_settings.mode == "sync")
            {
                for (; consumed < max_messages;)
                {
                    auto count = (max_messages - consumed) > batch ? batch : (max_messages - consumed);
                    auto result{wal->consume(static_cast<uint32_t>(count), 50, ctx)};

                    if (result.err == 0)
                    {
                        doConsume(result.records, &cctx);
                    }
                    else
                    {
                        lock_guard<mutex> lock(stdout_mutex);
                        cout << "failed to consume " << tpo.topic << "," << tpo.partition << "," << tpo.offset << endl;
                    }
                }
            }
            else
            {
                /// callback style
                for (; consumed < max_messages;)
                {
                    if (wal->consume(doConsume, &cctx, ctx) != 0)
                    {
                        lock_guard<mutex> lock(stdout_mutex);
                        cout << "failed to consume " << tpo.topic << "," << tpo.partition << "," << tpo.offset << endl;
                    }
                }
            }

            wal->stopConsume(ctx);
            cout << "consumed=" << consumed << endl;
        });
    }

    worker_pool.wait();
    cout << "data consumption is done" << endl;
}

void incrementalConsume(const BenchmarkSettings & bench_settings, Int32 size)
{
    KafkaWALConsumerMultiplexerPtrs wals;
    wals.reserve(size);

    mutex stdout_mutex;

    for (Int32 i = 0; i < size; ++i)
    {
        auto settings = make_unique<KafkaWALSettings>();
        *settings = *bench_settings.wal_settings;
        wals.push_back(make_shared<KafkaWALConsumerMultiplexer>(std::move(settings)));
        wals.back()->startup();
    }

    atomic_uint64_t consumed = 0;

    struct CallbackData : public klog::ConsumeCallbackData
    {
        const BenchmarkSettings & settings;
        atomic_uint64_t & consumed;
        mutex & stdout_mutex;
        Int32 jobid;
        KafkaWALConsumerMultiplexerPtr & wal;
        DB::Block header;

        CallbackData(
            const BenchmarkSettings & settings_,
            atomic_uint64_t & consumed_,
            mutex & stdout_mutex_,
            KafkaWALConsumerMultiplexerPtr & wal_,
            Int32 jobid_)
            : settings(settings_), consumed(consumed_), stdout_mutex(stdout_mutex_), jobid(jobid_), wal(wal_), header(prepareData(0))
        {
        }

        const DB::Block & getSchema(UInt16) const override { return header; }
    };

    auto callback = [](RecordPtrs records, klog::ConsumeCallbackData * data) -> void {
        auto pdata = static_cast<CallbackData *>(data);

        pdata->consumed += records.size();

        for (const auto & record : records)
        {
            TopicPartitionOffset offset(record->getStream(), record->getShard(), record->getSN());
            auto err = pdata->wal->commit(offset);
            assert(!err);

            lock_guard<mutex> lock(pdata->stdout_mutex);

            if (err)
            {
                cout << "jobid=" << pdata->jobid << " failed to commit, topic=" << record->getStream()
                     << " partition=" << record->getShard() << " offset=" << record->getSN() << " error=" << err << "\n";
            }

            cout << "jobid=" << pdata->jobid << " topic=" << record->getStream() << " partition=" << record->getShard()
                 << " offset=" << record->getSN() << " idem=" << record->getHeader("_idem") << endl;

            if (pdata->settings.consumer_settings.dumpdata)
            {
                dumpData(record->getBlock());
            }
        }
    };

    vector<shared_ptr<CallbackData>> datas;
    for (Int32 i = 0; i < size; ++i)
    {
        datas.push_back(make_shared<CallbackData>(bench_settings, consumed, stdout_mutex, wals[i], i));
    }

    /// Add a topic/partition at a time
    size_t i = 0;
    for (const auto & tpo : bench_settings.consumer_settings.kafka_topic_partition_offsets)
    {
        this_thread::sleep_for(20000ms);

        cout << "adding topic=" << tpo.topic << " partition=" << tpo.partition << " offset=" << tpo.offset << "\n";

        /// The multiplexers are sharing the same consumer group id
        /// Assign one to consume the partition
        auto & wal = wals[i % wals.size()];
        auto res = wal->addSubscription(tpo, callback, datas[i % wals.size()].get());

        assert(!res.err);
        if (res.err)
        {
            cout << "failed to subscribe, error=" << res.err << "\n";
        }
        ++i;
    }

    /// Remove a topic/partition at a time
    i = 0;
    for (const auto & tpo : bench_settings.consumer_settings.kafka_topic_partition_offsets)
    {
        this_thread::sleep_for(20000ms);

        cout << "Removing topic=" << tpo.topic << " partition=" << tpo.partition << " offset=" << tpo.offset << "\n";

        auto & wal = wals[i % wals.size()];
        auto res = wal->removeSubscription(tpo);
        assert(!res);
        if (res)
        {
            cout << "failed to unsubscribe, error=" << res << "\n";
        }
        ++i;
    }

    while (consumed <= static_cast<uint64_t>(bench_settings.consumer_settings.max_messages))
    {
        this_thread::sleep_for(1000ms);
    }
    cout << "incremental data consumption is done" << endl;

    for (auto & wal : wals)
    {
        wal->shutdown();
    }
}

void admin(KafkaWALPtrs & wals, const BenchmarkSettings & bench_settings)
{
    KafkaWALContext ctx{bench_settings.topic_settings.name};
    ctx.partitions = bench_settings.topic_settings.partitions;
    ctx.replication_factor = bench_settings.topic_settings.replication_factor;

    if (bench_settings.topic_settings.mode == "create")
    {
        if (wals[0]->create(bench_settings.topic_settings.name, ctx) != 0)
        {
            cout << "failed to create topic " << bench_settings.topic_settings.name << "\n";
        }
        else
        {
            cout << "create topic " << bench_settings.topic_settings.name << " successfully\n";
        }
    }
    else if (bench_settings.topic_settings.mode == "delete")
    {
        if (wals[0]->remove(bench_settings.topic_settings.name, ctx) != 0)
        {
            cout << "failed to delete topic " << bench_settings.topic_settings.name << "\n";
        }
        else
        {
            cout << "delete topic " << bench_settings.topic_settings.name << " successfully\n";
        }
    }
    else
    {
        auto result = wals[0]->describe(bench_settings.topic_settings.name);
        if (result.err != 0)
        {
            cout << "failed to describe topic " << bench_settings.topic_settings.name << "\n";
        }
        else
        {
            cout << "topic " << bench_settings.topic_settings.name << " has " << result.partitions << " partitions \n";
        }
    }
}

void dumpStats(Int64 total_cost, const ResultQueues & result_queues, const BenchmarkSettings & bench_settings)
{
    auto qps = static_cast<Int32>(
        (bench_settings.producer_settings.iterations * bench_settings.producer_settings.concurrency) / (total_cost / 1000000.0));

    ofstream f{calculateFileName(bench_settings)};

    f << "qps : " << qps << "\n";
    for (const auto & result_queue : result_queues)
    {
        for (auto latency : result_queue)
        {
            f << latency << "\n";
        }
    }

    cout << "status is dumped" << endl;
}
}

int mainKLog(int argc, char ** argv)
{
    auto bench_settings = parseArgs(argc, argv);
    if (bench_settings.exit)
    {
        return 1;
    }

    if (bench_settings.command == "produce")
    {
        KafkaWALPtrs wals{createDWals(bench_settings, bench_settings.producer_settings.wal_client_pool_size)};

        ResultQueues result_queues{static_cast<size_t>(bench_settings.producer_settings.concurrency)};

        auto total_cost = ingest(wals, result_queues, bench_settings);
        dumpStats(total_cost, result_queues, bench_settings);
    }
    else if (bench_settings.command == "consume")
    {
        if (bench_settings.consumer_settings.incremental)
        {
            incrementalConsume(bench_settings, bench_settings.consumer_settings.wal_client_pool_size);
        }
        else
        {
            KafkaWALPtrs wals{createDWals(bench_settings, bench_settings.consumer_settings.wal_client_pool_size)};
            consume(wals, bench_settings);
        }
    }
    else
    {
        /// admin topic
        KafkaWALPtrs wals{createDWals(bench_settings, 1)};
        admin(wals, bench_settings);
    }

    return 0;
}
