#include <Cluster/Common/serde.h>
#include <Cluster/MetaStore/MultipleVersionMergeOperator.h>
#include <Cluster/Protocol/AccessEntityDescription.h>

#include <gtest/gtest.h>

namespace
{
cluster::protocol::AccessEntityDescription getAccessDescriptor(uint32_t version)
{
    cluster::protocol::AccessEntityDescription desc;
    desc.data_version = version;

    return desc;
}

TEST(MultipleVersionMergeOperator, UnhandledTruncation)
{
    std::string data;
    uint32_t current_version = 0;
    {
        DB::WriteBufferFromString wb(data);
        cluster::meta::MultipleVersionEncoder::encodeHeader(wb, 2);

        for (uint32_t i = 0; i < 2; ++i)
        {
            auto desc = getAccessDescriptor(i + 1);
            desc.serialize(wb, cluster::Nulls::NullVersion);
            current_version = i + 1;
        }

        wb.finalize();
    }

    rocksdb::Slice existing_value_slice{data};

    std::vector<std::string> operands;
    operands.reserve(3);

    for (uint32_t i = 0; i < 3; ++i)
    {
        operands.emplace_back();
        DB::WriteBufferFromString wb(operands.back());

        auto desc = getAccessDescriptor(current_version + i + 1);
        desc.serialize(wb, cluster::Nulls::NullVersion);
    }

    std::vector<rocksdb::Slice> operand_list;
    operand_list.reserve(operands.size());
    for (const auto & operand : operands)
        operand_list.emplace_back(operand);

    auto key = cluster::encodeMetaAccessEntityKey("access_id");
    rocksdb::Slice key_slice{key};

    rocksdb::MergeOperator::MergeOperationInput merge_in(key_slice, &existing_value_slice, operand_list, nullptr);

    std::string new_value;
    rocksdb::Slice existing_operand;
    rocksdb::MergeOperator::MergeOperationOutput merge_out(new_value, existing_operand);

    cluster::meta::MultipleVersionMergeOperator merge(/*keep_versions=*/4, getLogger("MultiVersionMergeOperator"));
    auto merged = merge.FullMergeV2(merge_in, &merge_out);
    ASSERT_TRUE(merged);
    ASSERT_FALSE(new_value.empty());

    {
        DB::ReadBufferFromString rb(new_value);
        ASSERT_TRUE(cluster::meta::MultipleVersionEncoder::likelyMultiVersionEncoded(new_value));

        uint8_t number_versions = 0;
        cluster::meta::MultipleVersionEncoder::decodeHeader(rb, number_versions);
        ASSERT_EQ(number_versions, 3);

        for (uint32_t i = 0; i < number_versions; ++i)
        {
            cluster::protocol::AccessEntityDescription desc;
            desc.deserialize(rb, cluster::Nulls::NullVersion);
            EXPECT_EQ(desc.data_version, i + 3);
        }
    }
}

}
