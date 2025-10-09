#include <Cluster/Requests/CreateStreamRequest.h>
#include <Cluster/examples/create_stream_request.pb.h>

#include <Compression/CompressedWriteBuffer.h>
#include <Core/UUID.h>
#include <IO/ReadBufferFromString.h>
#include <Common/Stopwatch.h>

void checkHeader([[maybe_unused]] const auto & request_header_des, [[maybe_unused]] const auto & request_header)
{
    assert(request_header_des->opCode() == request_header.opCode());
    assert(request_header_des->requestVersion() == request_header.requestVersion());
    assert(request_header_des->correlationID() == request_header.correlationID());
}

void checkRequestBody([[maybe_unused]] const auto & request_des, [[maybe_unused]] const auto & request)
{
    assert(request_des->opCode() == request.opCode());

    [[maybe_unused]] const auto & data_des = static_cast<cluster::CreateStreamRequest *>(request_des.get())->data();
    [[maybe_unused]] const auto & data = request.data();

    assert(data_des.sql_ddl == data.sql_ddl);
    assert(data_des.desc == data.desc);
}

void createStreamRequest()
{
    cluster::CreateStreamRequest request(
        "default",
        "test_stream",
        DB::UUIDHelpers::generateV4(),
        10,
        3,
        "ATTACH STREAM test(i int);",
        "CREATE STREAM test(i int)",
        /*request_version_=*/1);

    cluster::Placements placements;
    placements.node_shard_replicas.emplace(1, std::vector{cluster::ShardReplica{0, 1}, cluster::ShardReplica{1, 1}});

    auto & data = request.data();
    /// Init request to make it valid
    data.setPlacements(std::move(placements));
    data.setFlushMessages(1000);
    data.setFlushInterval(5000);

    cluster::RequestHeader request_header(request.opCode(), /*request_version=*/1, 101);
    auto serialized_with_header = request.serializeWithHeader(request_header);

    DB::ReadBufferFromString rb(serialized_with_header);

    /// First parse header
    auto request_header_des = cluster::RequestHeader::parse(rb);
    checkHeader(request_header_des, request_header);

    /// Then parse request body
    auto request_des = cluster::CreateStreamRequest::parse(request_header_des->requestOpCode(), request_header_des->requestVersion(), rb);
    checkRequestBody(request_des, std::move(request));

    std::cout << "verify the normal serialize/deserialize works well\n";
}

void stressWriteBench()
{
    cluster::CreateStreamRequest request(
        "default",
        "test_stream",
        DB::UUIDHelpers::generateV4(),
        10,
        3,
        "ATTACH STREAM test(i int);",
        "CREATE STREAM test(i int)",
        /*request_version_=*/1);

    cluster::Placements placements;
    placements.node_shard_replicas.emplace(1, std::vector{cluster::ShardReplica{0, 1}, cluster::ShardReplica{1, 1}});

    auto & data = request.data();
    /// Init request to make it valid
    data.setPlacements(std::move(placements));
    data.setFlushMessages(1000);
    data.setFlushInterval(5000);

    cluster::RequestHeader request_header(request.opCode(), /*request_version=*/1, 101);


    double sum_duration{};

    constexpr int32_t avg_loop_count = 10, loop_count = 1000'000;

    for (int32_t k = 0; k < avg_loop_count; ++k)
    {
        /// current the codec inside requestdata has not been used, thus modify the codec is meaningless
        request.setCodec(DB::CompressionMethodByte::NONE);

        Stopwatch watch;

        for (int32_t i = 0; i < loop_count; i++)
            request.serializeWithHeader(request_header);

        sum_duration += loop_count * 1e9 / watch.elapsedNanoseconds();
    }

    std::cout << "write uncompressed: " << static_cast<uint64_t>(sum_duration / avg_loop_count) << " eps\n";
}

void stressReadBench()
{
    cluster::CreateStreamRequest request(
        "default",
        "test_stream",
        DB::UUIDHelpers::generateV4(),
        10,
        3,
        "ATTACH STREAM test(i int);",
        "CREATE STREAM test(i int)",
        /*request_version_=*/1);

    cluster::Placements placements;
    placements.node_shard_replicas.emplace(1, std::vector{cluster::ShardReplica{0, 1}, cluster::ShardReplica{1, 1}});

    auto & data = request.data();
    /// Init request to make it valid
    data.setPlacements(std::move(placements));
    data.setFlushMessages(1000);
    data.setFlushInterval(5000);

    cluster::RequestHeader request_header(request.opCode(), /*request_version=*/1, 101);
    const auto serialized_with_header = request.serializeWithHeader(request_header);


    double sum_duration{};

    constexpr int32_t avg_loop_count = 10, loop_count = 1000'000;

    for (int32_t k = 0; k < avg_loop_count; ++k)
    {

        Stopwatch watch;

        for (int32_t i = 0; i < loop_count; ++i)
        {
            DB::ReadBufferFromString rb(serialized_with_header);

            auto request_header_des = cluster::RequestHeader::parse(rb);

            cluster::CreateStreamRequest::parse(request_header_des->requestOpCode(), request_header_des->requestVersion(), rb);
        }

        sum_duration += loop_count * 1e9 / watch.elapsedNanoseconds();
    }

    std::cout << "read uncompressed: " << static_cast<uint64_t>(sum_duration / avg_loop_count) << " eps\n";
}

namespace
{
void convertToProto(const cluster::CreateStreamRequest & cluster_request, CreateStreamRequest & proto_request)
{
    /// current not support
    /// proto_request.set_data_version(cluster_request.getDataVersion());

    /// Convert nested message 'CreateStreamRequestData'
    auto & proto_data = *proto_request.mutable_request_data();
    /// proto_data.set_sql_ddl(cluster_request.data().getSQLDDL());

    /// Convert nested message 'StreamDescription'
    const auto & cluster_data = cluster_request.data();
    const auto & cluster_desc = cluster_data.desc;
    auto & proto_desc = *proto_data.mutable_desc();

    /// current not support fix me
    /// proto_desc.set_version(cluster_desc.getVersion());

    auto & proto_stream = *proto_desc.mutable_stream();
    proto_stream.set_ns(cluster_desc.stream.ns);
    proto_stream.set_name(cluster_desc.stream.name);
    /// proto_stream.set_id(cluster_desc.stream.id);

    proto_desc.set_shards(cluster_desc.shards);
    proto_desc.set_replicas(cluster_desc.replicas);
    proto_desc.set_sql_def(cluster_desc.sql_def);

    proto_desc.set_flush_ms(cluster_desc.flush_ms);
    proto_desc.set_flush_messages(cluster_desc.flush_messages);
    proto_desc.set_retention_ms(cluster_desc.retention_ms);
    proto_desc.set_retention_bytes(cluster_desc.retention_bytes);
    proto_desc.set_compacted(cluster_desc.compacted);

    /// Convert the 'Placements' structure to protobuf representation
    const auto & cluster_placements = cluster_data.desc.placements;
    auto * proto_placements = proto_request.mutable_request_data()->mutable_desc()->mutable_placements();

    for (const auto & pair : cluster_placements.node_shard_replicas)
    {
        auto & proto_shard_replica_list = (*proto_placements->mutable_node_shard_replicas())[pair.first];

        for (const auto & shard_replica : pair.second)
        {
            auto * proto_shard_replica = proto_shard_replica_list.add_replicas();
            proto_shard_replica->set_shard(shard_replica.shard);
            proto_shard_replica->set_replica(shard_replica.replica);
        }
    }
}

/// Convert from the protobuf message back to your class.
void convertFromProto(const CreateStreamRequest & proto_request, cluster::CreateStreamRequest & cluster_request)
{
    /// Get basic fields
    /// current not support fix me
    /// cluster_request.setDataVersion(proto_request.data_version());

    /// Convert nested CreateStreamRequestData
    const auto & proto_data = proto_request.request_data();
    auto & cluster_data = cluster_request.data();

    /// current not support fix me
    /// cluster_data.setSQLDDL(proto_data.sql_ddl());

    /// Convert StreamDescription
    const auto & proto_desc = proto_data.desc();
    auto & cluster_desc = cluster_data.desc;

    cluster_desc.stream.ns = proto_desc.stream().ns();
    cluster_desc.stream.name = proto_desc.stream().name();
    /// cluster_desc.stream.id = proto_desc.stream().id();

    cluster_desc.shards = proto_desc.shards();
    cluster_desc.replicas = proto_desc.replicas();
    cluster_desc.sql_def = proto_desc.sql_def();
    cluster_desc.flush_ms = proto_desc.flush_ms();
    cluster_desc.flush_messages = proto_desc.flush_messages();
    cluster_desc.retention_ms = proto_desc.retention_ms();
    cluster_desc.retention_bytes = proto_desc.retention_bytes();
    cluster_request.setCompacted(proto_desc.compacted());

    /// Convert Placements
    const auto & proto_placements = proto_request.request_data().desc().placements();

    cluster::Placements newClusterPlacements;

    for (const auto & entry : proto_placements.node_shard_replicas())
    {
        const auto & protoReplicas = entry.second.replicas();

        std::vector<cluster::ShardReplica> shardReplicas;
        shardReplicas.reserve(protoReplicas.size());

        for (const auto & protoShardReplica : protoReplicas)
            shardReplicas.push_back({protoShardReplica.shard(), protoShardReplica.replica()});

        newClusterPlacements.node_shard_replicas[entry.first] = std::move(shardReplicas);
    }

    cluster_request.setPlacements(std::move(newClusterPlacements));
}

std::string serialize(const cluster::CreateStreamRequest & request)
{
    CreateStreamRequest proto_request;
    convertToProto(request, proto_request);
    std::string serialized_data{};
    proto_request.SerializeToString(&serialized_data);
    return serialized_data;
}

void deserialize(const std::string & serialized_data, cluster::CreateStreamRequest & request)
{
    CreateStreamRequest proto_request;

    if (!proto_request.ParseFromString(serialized_data))
        UNREACHABLE();

    convertFromProto(proto_request, request);
}

void validate([[maybe_unused]] const auto & old_request, [[maybe_unused]] const auto & new_request)
{
    assert(old_request.opCode() == new_request.opCode());

    [[maybe_unused]] const auto & old_data = old_request.data();
    [[maybe_unused]] const auto & new_data = new_request.data();

    assert(old_data.sql_ddl == new_data.sql_ddl);
    assert(old_data.desc == new_data.desc);
}

}

void protobufSerDeserExample()
{
    /// step 1 new 2 requests
    cluster::CreateStreamRequest old_request(
        "default",
        "test_stream",
        DB::UUIDHelpers::generateV4(),
        10,
        3,
        "ATTACH STREAM test(i int);",
        "CREATE STREAM test(i int)",
        /*request_version_=*/1);

    cluster::Placements placements;
    placements.node_shard_replicas.emplace(1, std::vector{cluster::ShardReplica{0, 1}, cluster::ShardReplica{1, 1}});

    auto & data = old_request.data();
    /// Init old_request to make it valid
    data.setPlacements(std::move(placements));
    data.setFlushMessages(1000);
    data.setFlushInterval(5000);


    cluster::CreateStreamRequest new_request(
        "_",
        "_",
        DB::UUIDHelpers::generateV4(),
        10,
        3,
        "_",
        "_",
        /*request_version_=*/1);

    /// step 2 Serialize
    std::string serialized_data = serialize(old_request);

    /// step 3 Deserialize
    deserialize(serialized_data, new_request);

    /// step 4 validate the result
    validate(old_request, new_request);

    std::cout << "verify the protobuf works well\n";
}

void protobufStressWriteBench()
{
    /// step 1 new 2 requests
    cluster::CreateStreamRequest old_request(
        "default",
        "test_stream",
        DB::UUIDHelpers::generateV4(),
        10,
        3,
        "ATTACH STREAM test(i int);",
        "CREATE STREAM test(i int)",
        /*request_version_=*/1);

    cluster::Placements placements;
    placements.node_shard_replicas.emplace(1, std::vector{cluster::ShardReplica{0, 1}, cluster::ShardReplica{1, 1}});

    auto & data = old_request.data();
    /// Init old_request to make it valid
    data.setPlacements(std::move(placements));
    data.setFlushMessages(1000);
    data.setFlushInterval(5000);


    cluster::CreateStreamRequest new_request(
        "_",
        "_",
        DB::UUIDHelpers::generateV4(),
        10,
        3,
        "_",
        "_",
        /*request_version_=*/1);

    /// serialize
    double sum_duration{};

    constexpr int32_t avg_loop_count = 10, loop_count = 1000'000;

    for (int32_t k = 0; k < avg_loop_count; ++k)
    {
        /// current the codec inside requestdata has not been used, thus modify the codec is meaningless
        old_request.setCodec(DB::CompressionMethodByte::NONE);

        Stopwatch watch;

        for (int32_t i = 0; i < loop_count; i++)
            serialize(old_request);

        sum_duration += loop_count * 1e9 / watch.elapsedNanoseconds();
    }

    std::cout << "protobuf write uncompressed: " << static_cast<uint64_t>(sum_duration / avg_loop_count) << " eps\n";
}

void protobufStressReadBench()
{
    /// step 1 new 2 requests
    cluster::CreateStreamRequest old_request(
        "default",
        "test_stream",
        DB::UUIDHelpers::generateV4(),
        10,
        3,
        "ATTACH STREAM test(i int);",
        "CREATE STREAM test(i int)",
        /*request_version_=*/1);

    cluster::Placements placements;
    placements.node_shard_replicas.emplace(1, std::vector{cluster::ShardReplica{0, 1}, cluster::ShardReplica{1, 1}});

    auto & data = old_request.data();
    /// Init old_request to make it valid
    data.setPlacements(std::move(placements));
    data.setFlushMessages(1000);
    data.setFlushInterval(5000);


    cluster::CreateStreamRequest new_request(
        "_",
        "_",
        DB::UUIDHelpers::generateV4(),
        10,
        3,
        "_",
        "_",
        /*request_version_=*/1);

    /// serialize
    std::string serialized_data = serialize(old_request);

    /// bench read
    double sum_duration{};

    constexpr int32_t avg_loop_count = 10, loop_count = 1000'000;

    for (int32_t k = 0; k < avg_loop_count; ++k)
    {

        Stopwatch watch;

        for (int32_t i = 0; i < loop_count; ++i)
            deserialize(serialized_data, new_request);

        sum_duration += loop_count * 1e9 / watch.elapsedNanoseconds();
    }

    std::cout << "protobuf read uncompressed: " << static_cast<uint64_t>(sum_duration / avg_loop_count) << " eps\n";
}

int main()
{
    createStreamRequest();
    stressWriteBench();
    stressReadBench();

    protobufSerDeserExample();
    protobufStressWriteBench();
    protobufStressReadBench();
}
