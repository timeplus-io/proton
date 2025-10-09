#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/serde.h>
#include <Cluster/Protocol/StreamDescriptor.h>

#include <Core/UUID.h>
#include <base/ClockUtils.h>

#include <gtest/gtest.h>

#include <re2/re2.h>

namespace
{
int getMemoryWeightPartialMatch(const std::string & str)
{
    uint32_t memory_weight;

    RE2 pattern(R"(memory_weight\s*=\s*(\d+))");

    if (RE2::PartialMatch(str, pattern, &memory_weight))
        return memory_weight;
    return -1;
}

int getMemoryWeightFullMatch(const std::string & str)
{
    uint32_t memory_weight;

    RE2 pattern(R"(memory_weight\s*=\s*(\d+))");

    if (RE2::FullMatch(str, pattern, &memory_weight))
        return memory_weight;
    return -1;
}

}

TEST(StreamDescription, Serde)
{
    cluster::protocol::StreamDescriptor desc(
        "test",
        "stream",
        DB::UUIDHelpers::generateV4(),
        /*shards_*/ 3,
        /*sql_def_=*/"ATTACH 0000-0000-0000-0000 STREAM t(i int) SETTINGS shards=3",
        /*data_version_=*/ 11);
    desc.inmemory = true;
    desc.codec = DB::CompressionMethodByte::LZ4;
    desc.flush_ms = 12345;
    desc.flush_messages = 67890;
    desc.retention_ms = 3456;
    desc.retention_bytes = 7890;
    desc.create_timestamp_ms = DB::UTCMilliseconds::now();
    desc.last_modify_timestamp_ms = DB::UTCMilliseconds::now();
    auto data = cluster::serialize<std::string>(desc, cluster::Nulls::NullVersion);
    cluster::protocol::StreamDescriptor der_desc;
    cluster::deserialize(der_desc, data, cluster::Nulls::NullVersion);

    EXPECT_EQ(desc, der_desc);
}

TEST(StreamDescription, Regex)
{
    cluster::protocol::StreamDescriptor desc(
        "test",
        "stream",
        DB::UUIDHelpers::generateV4(),
        /*shards_*/ 3,
        /*sql_def_=*/R"(ATTACH MATERIALIZED VIEW _ UUID '22b6b211-a3df-4190-9ed3-7f6fd564dc7e' INTO default.tar
(
  `id` int32,
  `_tp_time` datetime64(3, 'UTC'),
  INDEX _tp_time_index _tp_time TYPE minmax GRANULARITY 256
) AS
SELECT
  *
FROM
  default.v
SETTINGS
  seek_to = 'earliest', memory_weight = 37, keep_versions = 4
)",
        /*data_version_=*/ 11);


    EXPECT_EQ(getMemoryWeightPartialMatch(desc.sql_def), 37);
    EXPECT_EQ(getMemoryWeightFullMatch(desc.sql_def), -1);
}
