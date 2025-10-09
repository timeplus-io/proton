#include <Cluster/Common/EncodeMetaKey.h>
#include <Cluster/Common/serde.h>
#include <Cluster/MetaStore/MultipleVersionMergeOperator.h>
#include <Cluster/Protocol/StreamDescriptor.h>

#include <Poco/Logger.h>

#include <gtest/gtest.h>

namespace
{
cluster::protocol::StreamDescriptor getStreamDescriptor(uint32_t version)
{
    cluster::protocol::StreamDescriptor desc;
    desc.data_version = version;
    return desc;
}

/// \param operand_total_versions_to_merge total versions to merge in operands excluding the existing_value
/// \param all_merged all merged versions
void doTest(
    uint8_t keep_versions,
    bool has_existing_value,
    uint8_t num_merged_versions_of_existing_value,
    uint8_t operand_total_versions_to_merge,
    std::vector<uint32_t> all_merged = {})
{
    ASSERT_GE(keep_versions, 3);
    ASSERT_GE(operand_total_versions_to_merge, 1);

    auto key = cluster::encodeMetaStreamKey("test_sn", "test");
    rocksdb::Slice key_slice{key};

    uint32_t next_stream_version = 2;
    std::string existing_value;
    uint8_t total_versions_to_merge = operand_total_versions_to_merge;

    if (has_existing_value)
    {
        if (num_merged_versions_of_existing_value > 0)
        {
            DB::WriteBufferFromString wb(existing_value);
            cluster::meta::MultipleVersionEncoder::encodeHeader(wb, num_merged_versions_of_existing_value);

            for (uint32_t i = 0; i < num_merged_versions_of_existing_value; ++i)
            {
                auto desc = getStreamDescriptor(static_cast<uint32_t>(i + 1));
                desc.serialize(wb, cluster::Nulls::NullVersion);
            }

            next_stream_version = num_merged_versions_of_existing_value + 1;
            total_versions_to_merge += num_merged_versions_of_existing_value;
        }
        else
        {
            auto desc = getStreamDescriptor(1);
            DB::WriteBufferFromString wb(existing_value);
            desc.serialize(wb, cluster::Nulls::NullVersion);

            total_versions_to_merge += 1;
        }
    }
    rocksdb::Slice existing_value_slice{existing_value};

    std::vector<std::string> operands;

    uint32_t all_merged_versions = 0;
    for (auto merged : all_merged)
    {
        /// For merged operation, it has header
        operands.emplace_back();
        DB::WriteBufferFromString wb(operands.back());
        cluster::meta::MultipleVersionEncoder::encodeHeader(wb, merged);

        /// And followed by N merged operations
        for (uint32_t i = 0; i < merged; ++i)
        {
            auto desc = getStreamDescriptor(next_stream_version + i);
            desc.serialize(wb, cluster::Nulls::NullVersion);
        }

        next_stream_version += merged;
        all_merged_versions += merged;
    }

    ASSERT_GE(operand_total_versions_to_merge, all_merged_versions);

    auto remaining_single_merges = operand_total_versions_to_merge - all_merged_versions;
    for (uint32_t i = 0; i < remaining_single_merges; ++i)
    {
        auto desc = getStreamDescriptor(next_stream_version + i);

        operands.emplace_back();
        DB::WriteBufferFromString wb(operands.back());
        desc.serialize(wb, cluster::Nulls::NullVersion);
    }

    next_stream_version += remaining_single_merges;

    std::vector<rocksdb::Slice> operand_list;
    operand_list.reserve(operands.size());
    for (const auto & operand : operands)
        operand_list.emplace_back(operand);

    rocksdb::MergeOperator::MergeOperationInput merge_in(
        key_slice, has_existing_value ? &existing_value_slice : nullptr, operand_list, nullptr);

    std::string new_value;
    rocksdb::Slice existing_operand;
    rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);

    cluster::meta::MultipleVersionMergeOperator merge(keep_versions, getLogger("MultiVersionMergeOperator"));
    auto merged = merge.FullMergeV2(merge_in, &merge_out);
    ASSERT_TRUE(merged);
    ASSERT_FALSE(new_value.empty());

    {
        DB::ReadBufferFromString rb(new_value);
        ASSERT_TRUE(cluster::meta::MultipleVersionEncoder::likelyMultiVersionEncoded(new_value));

        uint8_t number_versions = 0;
        cluster::meta::MultipleVersionEncoder::decodeHeader(rb, number_versions);
        if (total_versions_to_merge >= keep_versions)
            ASSERT_EQ(number_versions, keep_versions);
        else
            ASSERT_EQ(number_versions, total_versions_to_merge);

        for (uint32_t i = 0; i < number_versions; ++i)
        {
            cluster::protocol::StreamDescriptor desc;
            desc.deserialize(rb, cluster::Nulls::NullVersion);
            EXPECT_EQ(desc.data_version, next_stream_version - number_versions + i);
        }

        /// Request more than number_versions of versions
        {
            auto descs = cluster::meta::MultipleVersionEncoder::decode<cluster::protocol::StreamDescriptor>(
                cluster::MetaKeySpace::Stream, key, new_value, /*versions_requested=*/number_versions + 1, cluster::Nulls::NullVersion);
            ASSERT_EQ(descs.size(), number_versions);
            ASSERT_EQ(descs.front()->data_version, next_stream_version - number_versions);
            ASSERT_EQ(descs.back()->data_version, next_stream_version - 1);
        }

        /// Request exactly same number_versions of versions
        {
            auto descs = cluster::meta::MultipleVersionEncoder::decode<cluster::protocol::StreamDescriptor>(
                cluster::MetaKeySpace::Stream, key, new_value, /*versions_requested=*/number_versions, cluster::Nulls::NullVersion);
            ASSERT_EQ(descs.size(), number_versions);
            ASSERT_EQ(descs.front()->data_version, next_stream_version - number_versions);
            ASSERT_EQ(descs.back()->data_version, next_stream_version - 1);
        }

        /// Request less than number_versions of versions
        {
            if (number_versions > 2)
            {
                auto versions_requested = number_versions - 2;
                auto descs = cluster::meta::MultipleVersionEncoder::decode<cluster::protocol::StreamDescriptor>(
                    cluster::MetaKeySpace::Stream, key, new_value, versions_requested, cluster::Nulls::NullVersion);
                ASSERT_EQ(descs.size(), versions_requested);
                ASSERT_EQ(descs.front()->data_version, next_stream_version - versions_requested);
                ASSERT_EQ(descs.back()->data_version, next_stream_version - 1);
            }
        }

        /// Request latest version
        {
            auto desc = cluster::meta::MultipleVersionEncoder::decodeLatest<cluster::protocol::StreamDescriptor>(
                cluster::MetaKeySpace::Stream, key, new_value, cluster::Nulls::NullVersion);
            ASSERT_EQ(desc->data_version, next_stream_version - 1);
        }
    }
}

void doTestInterleaved(
    uint8_t keep_versions,
    bool has_existing_value,
    uint8_t num_merged_versions_of_existing_value,
    std::vector<std::pair<uint8_t, bool>> interleaved_operands)
{
    cluster::meta::MultipleVersionMergeOperator merge(keep_versions, getLogger("MultiVersionMergeOperator"));

    auto key = cluster::encodeMetaStreamKey("test_sn", "test");
    rocksdb::Slice key_slice{key};

    uint32_t next_stream_version = 2;
    std::string existing_value;
    uint8_t total_versions_to_merge = 0;

    if (has_existing_value)
    {
        if (num_merged_versions_of_existing_value > 0)
        {
            DB::WriteBufferFromString wb(existing_value);
            cluster::meta::MultipleVersionEncoder::encodeHeader(wb, num_merged_versions_of_existing_value);

            for (uint32_t i = 0; i < num_merged_versions_of_existing_value; ++i)
            {
                auto desc = getStreamDescriptor(static_cast<uint32_t>(i + 1));
                desc.serialize(wb, cluster::Nulls::NullVersion);
            }

            next_stream_version = num_merged_versions_of_existing_value + 1;
            total_versions_to_merge += num_merged_versions_of_existing_value;
        }
        else
        {
            auto desc = getStreamDescriptor(1);
            DB::WriteBufferFromString wb(existing_value);
            desc.serialize(wb, cluster::Nulls::NullVersion);
            total_versions_to_merge += 1;
        }
    }
    rocksdb::Slice existing_value_slice{existing_value};

    std::vector<std::string> operands;

    for (const auto & [merged_versions, merged_operand] : interleaved_operands)
    {
        if (merged_operand)
        {
            total_versions_to_merge += merged_versions;

            ASSERT_GE(merged_versions, 1);

            /// For merged operation, it has header
            operands.emplace_back();
            DB::WriteBufferFromString wb(operands.back());

            cluster::meta::MultipleVersionEncoder::encodeHeader(wb, merged_versions);

            /// And followed by N merged operations
            for (uint8_t i = 0; i < merged_versions; ++i)
            {
                auto desc = getStreamDescriptor(next_stream_version + i);
                desc.serialize(wb, cluster::Nulls::NullVersion);
            }
            next_stream_version += merged_versions;
        }
        else
        {
            ASSERT_EQ(merged_versions, 0);

            auto desc = getStreamDescriptor(next_stream_version);

            operands.emplace_back();
            DB::WriteBufferFromString wb(operands.back());
            desc.serialize(wb, cluster::Nulls::NullVersion);

            ++next_stream_version;
            ++total_versions_to_merge;
        }
    }

    std::vector<rocksdb::Slice> operand_list;
    operand_list.reserve(operands.size());
    for (const auto & operand : operands)
        operand_list.emplace_back(operand);

    rocksdb::MergeOperator::MergeOperationInput merge_in(key_slice, &existing_value_slice, operand_list, nullptr);

    std::string new_value;
    rocksdb::Slice existing_operand;
    rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);

    auto merged = merge.FullMergeV2(merge_in, &merge_out);
    ASSERT_TRUE(merged);
    ASSERT_FALSE(new_value.empty());

    {
        DB::ReadBufferFromString rb(new_value);
        ASSERT_TRUE(cluster::meta::MultipleVersionEncoder::likelyMultiVersionEncoded(new_value));

        uint8_t number_versions = 0;
        cluster::meta::MultipleVersionEncoder::decodeHeader(rb, number_versions);
        if (total_versions_to_merge >= keep_versions)
            ASSERT_EQ(number_versions, keep_versions);
        else
            ASSERT_EQ(number_versions, total_versions_to_merge);

        for (uint32_t i = 0; i < number_versions; ++i)
        {
            cluster::protocol::StreamDescriptor desc;
            desc.deserialize(rb, cluster::Nulls::NullVersion);
            EXPECT_EQ(desc.data_version, next_stream_version - number_versions + i);
        }
    }
}

}

TEST(MultipleVersionMergeOperator, FullMergeV2SingleMergeWithinMaxVersion)
{
    /// [existing_value=nullptr, MergeOp]
    doTest(
        /*keep_versions=*/3,
        /*has_existing_value=*/false,
        /*num_merged_versions_of_existing_value=*/0,
        /*operand_total_versions_to_merge=*/1);
}

TEST(MultipleVersionMergeOperator, FullMergeV2PutAndMergeWithinMaxVersion)
{
    /// [existing_value!=nullptr, MergeOp]
    doTest(
        /*keep_versions=*/3,
        /*has_existing_value=*/true,
        /*num_merged_versions_of_existing_value=*/0,
        /*operand_total_versions_to_merge=*/1);

    /// [existing_value!=nullptr(1 merged), MergeOp]
    doTest(
        /*keep_versions=*/3,
        /*has_existing_value=*/true,
        /*num_merged_versions_of_existing_value=*/1,
        /*operand_total_versions_to_merge=*/1);
}

TEST(MultipleVersionMergeOperator, FullMergeV2PutAndMergeExceedingMaxVersion)
{
    /// [existing_value!=nullptr, MergeOp, MergeOp, MergeOp, MergeOp, MergeOp, MergeOp]
    doTest(
        /*keep_versions=*/3,
        /*has_existing_value=*/true,
        /*num_merged_versions_of_existing_value=*/0,
        /*operand_total_versions_to_merge=*/6);

    /// [existing_value!=nullptr(1 merged), MergeOp, MergeOp, MergeOp, MergeOp, MergeOp, MergeOp]
    doTest(
        /*keep_versions=*/3,
        /*has_existing_value=*/true,
        /*num_merged_versions_of_existing_value=*/1,
        /*operand_total_versions_to_merge=*/6);
}

TEST(MultipleVersionMergeOperator, FullMergeV2MergeExceedingMaxVersion)
{
    /// [existing_value=nullptr, MergeOp, MergeOp, MergeOp, MergeOp, MergeOp, MergeOp]
    doTest(
        /*keep_versions=*/3,
        /*has_existing_value=*/false,
        /*num_merged_versions_of_existing_value=*/0,
        /*operand_total_versions_to_merge=*/6);
}

TEST(MultipleVersionMergeOperator, FullMergeV2MergedWithinMaxVersion)
{
    /// [existing_value=nullptr, Merged MergeOp, MergeOp]
    doTest(3, /*has_existing_value=*/false, /*num_merged_versions_of_existing_value=*/0, /*operand_total_versions_to_merge=*/2, {1});
    /// [existing_value=nullptr, Merged MergeOp, Merged MergeOp, MergeOp]
    doTest(3, /*has_existing_value=*/false, /*num_merged_versions_of_existing_value=*/0, /*operand_total_versions_to_merge=*/3, {1, 1});
    /// [existing_value=nullptr, Merged MergeOp(2), Merged MergeOp]
    doTest(3, /*has_existing_value=*/false, /*num_merged_versions_of_existing_value=*/0, /*operand_total_versions_to_merge=*/3, {2, 1});
    /// [existing_value=nullptr, Merged MergeOp, Merged MergeOp(2)]
    doTest(3, /*has_existing_value=*/false, /*num_merged_versions_of_existing_value=*/0, /*operand_total_versions_to_merge=*/3, {1, 2});
}

TEST(MultipleVersionMergeOperator, FullMergeV2PutAndMergedWithinMaxVersion)
{
    /// [existing_value!=nullptr, Merged MergeOp, MergeOp]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/0, /*operand_total_versions_to_merge=*/2, {1});
    /// [existing_value!=nullptr, Merged MergeOp, Merged MergeOp]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/0, /*operand_total_versions_to_merge=*/2, {1, 1});
    /// [existing_value!=nullptr, Merged MergeOp(2)]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/0, /*operand_total_versions_to_merge=*/2, {2});
}

TEST(MultipleVersionMergeOperator, FullMergeV2MergedAndMergedWithinMaxVersion)
{
    /// [existing_value!=nullptr(merged), Merged MergeOp, MergeOp]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/1, /*operand_total_versions_to_merge=*/2, {1});
    /// [existing_value!=nullptr(merged), Merged MergeOp, Merged MergeOp]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/1, /*operand_total_versions_to_merge=*/2, {1, 1});
    /// [existing_value!=nullptr(merged), Merged MergeOp(2)]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/1, /*operand_total_versions_to_merge=*/2, {2});

    /// [existing_value!=nullptr(2 merged), MergeOp]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/2, /*operand_total_versions_to_merge=*/1, {});
    /// [existing_value!=nullptr(2 merged), Merged MergeOp]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/2, /*operand_total_versions_to_merge=*/1, {1});
}

TEST(MultipleVersionMergeOperator, FullMergeV2MergedExceedingMaxVersion)
{
    /// [existing_value=nullptr, Merged MergeOp, MergeOp, MergeOp, MergeOp, MergeOp, MergeOp]
    doTest(3, /*has_existing_value=*/false, /*num_merged_versions_of_existing_value=*/0, /*operand_total_versions_to_merge=*/6, {1});
    /// [existing_value=nullptr, Merged MergeOp, Merged MergeOp, MergeOp, MergeOp, MergeOp, MergeOp]
    doTest(3, /*has_existing_value=*/false, /*num_merged_versions_of_existing_value=*/0, /*operand_total_versions_to_merge=*/6, {1, 1});
    /// [existing_value=nullptr, Merged MergeOp(2), Merged MergeOp(2), MergeOp, MergeOp]
    doTest(3, /*has_existing_value=*/false, /*num_merged_versions_of_existing_value=*/0, /*operand_total_versions_to_merge=*/6, {2, 2});
    /// [existing_value=nullptr, Merged MergeOp(3), Merged MergeOp(2), MergeOp]
    doTest(3, /*has_existing_value=*/false, /*num_merged_versions_of_existing_value=*/0, /*operand_total_versions_to_merge=*/6, {3, 2});
    /// [existing_value=nullptr, Merged MergeOp(3), Merged MergeOp(3)]
    doTest(3, /*has_existing_value=*/false, /*num_merged_versions_of_existing_value=*/0, /*operand_total_versions_to_merge=*/6, {3, 3});
}

TEST(MultipleVersionMergeOperator, FullMergeV2PutAndMergedExceedingMaxVersion)
{
    /// [existing_value!=nullptr(Merge), Merged MergeOp, MergeOp, MergeOp, MergeOp, MergeOp, MergeOp]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/0, /*operand_total_versions_to_merge=*/6, {1});
    /// [existing_value!=nullptr(Merge), Merged MergeOp, Merged MergeOp, MergeOp, MergeOp, MergeOp, MergeOp]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/0, /*operand_total_versions_to_merge=*/6, {1, 1});
    /// [existing_value!=nullptr(Merge), Merged MergeOp(2), Merged MergeOp(2), MergeOp, MergeOp]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/0, /*operand_total_versions_to_merge=*/6, {2, 2});
    /// [existing_value!=nullptr(Merge), Merged MergeOp(3), Merged MergeOp(2), MergeOp]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/0, /*operand_total_versions_to_merge=*/6, {3, 2});
    /// [existing_value!=nullptr(Merge), Merged MergeOp(3), Merged MergeOp(3)]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/0, /*operand_total_versions_to_merge=*/6, {3, 3});
}

TEST(MultipleVersionMergeOperator, FullMergeV2MergedAndMergedExceedingMaxVersion)
{
    /// [existing_value!=nullptr(Merged), Merged MergeOp, MergeOp, MergeOp, MergeOp, MergeOp, MergeOp]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/1, /*operand_total_versions_to_merge=*/6, {1});
    /// [existing_value!=nullptr(Merged), Merged MergeOp, Merged MergeOp, MergeOp, MergeOp, MergeOp, MergeOp]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/1, /*operand_total_versions_to_merge=*/6, {1, 1});
    /// [existing_value!=nullptr(Merged), Merged MergeOp(2), Merged MergeOp(2), MergeOp, MergeOp]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/1, /*operand_total_versions_to_merge=*/6, {2, 2});
    /// [existing_value!=nullptr(Merged), Merged MergeOp(3), Merged MergeOp(2), MergeOp]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/1, /*operand_total_versions_to_merge=*/6, {3, 2});
    /// [existing_value!=nullptr(Merged), Merged MergeOp(3), Merged MergeOp(3)]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/1, /*operand_total_versions_to_merge=*/6, {3, 3});

    /// [existing_value!=nullptr(2 Merged), Merged MergeOp, MergeOp, MergeOp, MergeOp, MergeOp, MergeOp]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/2, /*operand_total_versions_to_merge=*/6, {1});
    /// [existing_value!=nullptr(2 Merged), Merged MergeOp, Merged MergeOp, MergeOp, MergeOp, MergeOp, MergeOp]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/2, /*operand_total_versions_to_merge=*/6, {1, 1});
    /// [existing_value!=nullptr(2 Merged), Merged MergeOp(2), Merged MergeOp(2), MergeOp, MergeOp]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/2, /*operand_total_versions_to_merge=*/6, {2, 2});
    /// [existing_value!=nullptr(2 Merged), Merged MergeOp(3), Merged MergeOp(2), MergeOp]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/2, /*operand_total_versions_to_merge=*/6, {3, 2});
    /// [existing_value!=nullptr(2 Merged), Merged MergeOp(3), Merged MergeOp(3)]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/2, /*operand_total_versions_to_merge=*/6, {3, 3});

    /// [existing_value!=nullptr(3 Merged), Merged MergeOp, MergeOp, MergeOp, MergeOp, MergeOp, MergeOp]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/3, /*operand_total_versions_to_merge=*/6, {1});
    /// [existing_value!=nullptr(3 Merged), Merged MergeOp, Merged MergeOp, MergeOp, MergeOp, MergeOp, MergeOp]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/3, /*operand_total_versions_to_merge=*/6, {1, 1});
    /// [existing_value!=nullptr(3 Merged), Merged MergeOp(2), Merged MergeOp(2), MergeOp, MergeOp]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/3, /*operand_total_versions_to_merge=*/6, {2, 2});
    /// [existing_value!=nullptr(3 Merged), Merged MergeOp(3), Merged MergeOp(2), MergeOp]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/3, /*operand_total_versions_to_merge=*/6, {3, 2});
    /// [existing_value!=nullptr(3 Merged), Merged MergeOp(3), Merged MergeOp(3)]
    doTest(3, /*has_existing_value=*/true, /*num_merged_versions_of_existing_value=*/3, /*operand_total_versions_to_merge=*/6, {3, 3});
}

namespace
{
void initialPutInterleaved(uint8_t keep_versions)
{
    /// [existing_value!=nullptr, Merged MergeOp, MergeOp, Merged MergeOp, MergeOp, Merged MergeOp]
    doTestInterleaved(
        /*keep_versions=*/keep_versions,
        /*has_existing_value=*/true,
        /*num_merged_versions_of_existing_value=*/0,
        {{1, true}, {0, false}, {1, true}, {0, false}, {1, true}});

    /// [existing_value!=nullptr, MergeOp, Merged MergeOp, MergeOp, Merged MergeOp, Merged MergeOp]
    doTestInterleaved(
        /*keep_versions=*/keep_versions,
        /*has_existing_value=*/true,
        /*num_merged_versions_of_existing_value=*/0,
        {{0, false}, {1, true}, {0, false}, {1, true}, {1, true}});

    /// [existing_value!=nullptr, MergeOp, Merged MergeOp, Merged MergeOp, Merged MergeOp, MergeOp]
    doTestInterleaved(
        /*keep_versions=*/keep_versions,
        /*has_existing_value=*/true,
        /*num_merged_versions_of_existing_value=*/0,
        {{0, false}, {1, true}, {1, true}, {1, true}, {0, false}});

    /// [existing_value!=nullptr, Merged MergeOp, MergeOp, Merged MergeOp, Merged MergeOp, MergeOp]
    doTestInterleaved(
        /*keep_versions=*/keep_versions,
        /*has_existing_value=*/true,
        /*num_merged_versions_of_existing_value=*/0,
        {{1, true}, {0, false}, {1, true}, {1, true}, {0, false}});

    /// [existing_value!=nullptr, Merged MergeOp, MergeOp, Merged MergeOp(3)]
    doTestInterleaved(
        /*keep_versions=*/keep_versions,
        /*has_existing_value=*/true,
        /*num_merged_versions_of_existing_value=*/0,
        {{1, true}, {0, false}, {3, true}});

    /// [existing_value!=nullptr, Merged MergeOp(2), MergeOp, Merged MergeOp(2)]
    doTestInterleaved(
        /*keep_versions=*/keep_versions,
        /*has_existing_value=*/true,
        /*num_merged_versions_of_existing_value=*/0,
        {{2, true}, {0, false}, {2, true}});
}

}

TEST(MultipleVersionMergeOperator, FullMergeV2PutAndMergedExceedingMaxVersionInterleaved)
{
    initialPutInterleaved(/*keep_versions=*/5);
}

TEST(MultipleVersionMergeOperator, FullMergeV2PutAndMergedWithinMaxVersionInterleaved)
{
    initialPutInterleaved(/*keep_versions=*/6);
}

namespace
{
void initialMergedInterleaved(uint8_t keep_versions)
{
    /// [existing_value!=nullptr, Merged MergeOp, MergeOp, Merged MergeOp, MergeOp, Merged MergeOp]
    doTestInterleaved(
        /*keep_versions=*/keep_versions,
        /*has_existing_value=*/true,
        /*num_merged_versions_of_existing_value=*/1,
        {{1, true}, {0, false}, {1, true}, {0, false}, {1, true}});

    /// [existing_value!=nullptr, MergeOp, Merged MergeOp, MergeOp, Merged MergeOp, Merged MergeOp]
    doTestInterleaved(
        /*keep_versions=*/keep_versions,
        /*has_existing_value=*/true,
        /*num_merged_versions_of_existing_value=*/1,
        {{0, false}, {1, true}, {0, false}, {1, true}, {1, true}});

    /// [existing_value!=nullptr, MergeOp, Merged MergeOp, Merged MergeOp, Merged MergeOp, MergeOp]
    doTestInterleaved(
        /*keep_versions=*/keep_versions,
        /*has_existing_value=*/true,
        /*num_merged_versions_of_existing_value=*/1,
        {{0, false}, {1, true}, {1, true}, {1, true}, {0, false}});

    /// [existing_value!=nullptr, Merged MergeOp, MergeOp, Merged MergeOp, Merged MergeOp, MergeOp]
    doTestInterleaved(
        /*keep_versions=*/keep_versions,
        /*has_existing_value=*/true,
        /*num_merged_versions_of_existing_value=*/1,
        {{1, true}, {0, false}, {1, true}, {1, true}, {0, false}});

    /// [existing_value!=nullptr, Merged MergeOp, MergeOp, Merged MergeOp(4)]
    doTestInterleaved(
        /*keep_versions=*/keep_versions,
        /*has_existing_value=*/true,
        /*num_merged_versions_of_existing_value=*/2,
        {{1, true}, {0, false}, {2, true}});

    /// [existing_value!=nullptr, Merged MergeOp(2), MergeOp, Merged MergeOp(3)]
    doTestInterleaved(
        /*keep_versions=*/keep_versions,
        /*has_existing_value=*/true,
        /*num_merged_versions_of_existing_value=*/2,
        {{2, true}, {0, false}, {1, true}});

    /// [existing_value!=nullptr, Merged MergeOp, MergeOp, Merged MergeOp, MergeOp, Merged MergeOp]
    doTestInterleaved(
        /*keep_versions=*/keep_versions,
        /*has_existing_value=*/true,
        /*num_merged_versions_of_existing_value=*/2,
        {{1, true}, {0, false}, {0, false}, {1, true}});

    /// [existing_value!=nullptr, MergeOp, Merged MergeOp, MergeOp, Merged MergeOp, Merged MergeOp]
    doTestInterleaved(
        /*keep_versions=*/keep_versions,
        /*has_existing_value=*/true,
        /*num_merged_versions_of_existing_value=*/2,
        {{0, false}, {1, true}, {0, false}, {1, true}});

    /// [existing_value!=nullptr, MergeOp, Merged MergeOp, Merged MergeOp, Merged MergeOp, MergeOp]
    doTestInterleaved(
        /*keep_versions=*/keep_versions,
        /*has_existing_value=*/true,
        /*num_merged_versions_of_existing_value=*/2,
        {{0, false}, {1, true}, {1, true}, {0, false}});

    /// [existing_value!=nullptr, Merged MergeOp, MergeOp, Merged MergeOp, Merged MergeOp, MergeOp]
    doTestInterleaved(
        /*keep_versions=*/keep_versions,
        /*has_existing_value=*/true,
        /*num_merged_versions_of_existing_value=*/2,
        {{1, true}, {0, false}, {1, true}, {0, false}});

    /// [existing_value!=nullptr, Merged MergeOp, MergeOp, Merged MergeOp(4)]
    doTestInterleaved(
        /*keep_versions=*/keep_versions,
        /*has_existing_value=*/true,
        /*num_merged_versions_of_existing_value=*/2,
        {{1, true}, {0, false}, {2, true}});

    /// [existing_value!=nullptr, Merged MergeOp(2), MergeOp, Merged MergeOp(3)]
    doTestInterleaved(
        /*keep_versions=*/keep_versions,
        /*has_existing_value=*/true,
        /*num_merged_versions_of_existing_value=*/2,
        {{2, true}, {0, false}, {1, true}});
}

}

TEST(MultipleVersionMergeOperator, FullMergeV2MergedAndMergedExceedingMaxVersionInterleaved)
{
    initialMergedInterleaved(/*keep_versions=*/5);
}

TEST(MultipleVersionMergeOperator, FullMergeV2MergedAndMergedWithinMaxVersionInterleaved)
{
    initialMergedInterleaved(/*keep_versions=*/6);
}
