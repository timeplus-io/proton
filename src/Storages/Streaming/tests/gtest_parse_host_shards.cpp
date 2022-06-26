#include <gtest/gtest.h>

#include <Storages/Streaming/parseHostShards.h>
#include <Common/Exception.h>

#include <regex>

namespace
{

/// host_shards = "''", shards = 1
struct Case
{
    Int32 total_shards;
    String host_shards;
    std::vector<Int32> expected;
    bool throws;
};

std::vector<Case> cases = {
    {1, "", {0}, false},
    {1, " ", {}, true},
    {1, "0", {0}, false},
    {1, "0 ", {0}, false},
    {1, " 0", {0}, false},
    {1, " 0 ", {0}, false},
    {1, " 0 ", {0}, false},
    {2, "", {0, 1}, false},
    {2, "0,1", {0, 1}, false},
    {2, " 0,1", {0, 1}, false},
    {2, " 0 ,1", {0, 1}, false},
    {2, " 0 , 1", {0, 1}, false},
    {2, " 0 , 1 ", {0, 1}, false},
    {2, "0,2", {0, 1}, true},
    {2, "0,,1", {}, true},
    {2, "0, ,1", {}, true},
    {2, ",0,1", {}, true},
    {2, "0,1,", {}, true},
    {2, "0,1,,", {}, true},
    {2, ",,", {}, true},
    {2, ",,,", {}, true},
};

}

TEST(ParseHostShards, ParseHostShards)
{
    for (const auto & c : cases)
    {
        for (const auto & host_shards : {c.host_shards, "'" + c.host_shards + "'"})
        {
            if (c.throws)
            {
                EXPECT_THROW(DB::parseHostShards(host_shards, c.total_shards), DB::Exception);
            }
            else
            {
                auto shards = DB::parseHostShards(host_shards, c.total_shards);
                ASSERT_EQ(shards.size(), c.expected.size());
                for (size_t i = 0; auto shard : shards)
                    EXPECT_EQ(shard, c.expected[i++]);
            }
        }
    }
}

TEST(ParseHostShards, ParseHostShardsRegex)
{
    std::regex parse_host_shards_regex{"host_shards\\s*=\\s*'([,|\\s|\\d]*)'"};

    for (const auto & c : cases)
    {
        std::smatch pattern_match;
        std::string host_shards = "host_shards = '" + c.host_shards + "'";
        auto m = std::regex_search(host_shards, pattern_match, parse_host_shards_regex);
        ASSERT_TRUE(m);
        EXPECT_EQ(pattern_match.str(1), c.host_shards);
    }
}
