#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Streaming/SequencedChunkSplitter.h>

#include <gtest/gtest.h>


using namespace DB;
using namespace DB::Streaming;

namespace
{
/// Header: value(String) + SN(Int64)
Chunk prepareChunk(const std::vector<std::pair<std::string_view, Int64>> & data)
{
    Columns columns;
    auto col = ColumnString::create();
    auto sn_col = ColumnInt64::create();

    for (const auto & [value, sn] : data)
    {
        col->insert(value);
        sn_col->insert(sn);
    }

    columns.emplace_back(std::move(col));
    columns.emplace_back(std::move(sn_col));

    return Chunk(std::move(columns), data.size());
}

void checkChunk(const Chunk & chunk, std::vector<std::string_view> data, std::optional<Int64> sn = std::nullopt)
{
    ASSERT_EQ(chunk.rows(), data.size());
    if (sn.has_value())
    {
        ASSERT_EQ(chunk.getNumColumns(), 2);
        const auto & col = chunk.getColumns()[0];
        const auto & sn_col = chunk.getColumns()[1];
        auto sn_val = sn.value();
        for (size_t i = 0; i < data.size(); ++i)
        {
            ASSERT_EQ(col->getDataAt(i).toView(), data[i]);
            ASSERT_EQ(sn_col->getInt(i), sn_val);
        }
    }
    else
    {
        ASSERT_EQ(chunk.getNumColumns(), 1);
        const auto & col = chunk.getColumns()[0];
        for (size_t i = 0; i < data.size(); ++i)
        {
            ASSERT_EQ(col->getDataAt(i).toView(), data[i]);
        }
    }
}
}

TEST(SequencedChunkSplitter, Basic)
{
    SequencedChunkSplitter splitter(1, /*remove_sn_col=*/false);

    /// The chunks may be like this:
    ///  Chunk-1         Chunk-2         Chunk-3         Chunk-4         Chunk-5
    /// +---------+     +---------+     +---------+     +---------+     +---------+
    /// |(SN=1)a1 |     |(SN=1)c1 |     |(SN=2)a2 |     |(SN=2)c2 |     |(SN=3)b3 |
    /// |(SN=1)b1 |     |(SN=1)d1 |     |(SN=2)b2 |     |(SN=3)a3 |     |(SN=4)a4 |
    /// +_________+     +_________+     +_________+     +_________+     |(SN=4)b4 |
    ///                                                                 |(SN=5)a5 |
    ///                                                                 +_________+
    auto result = splitter.splitOrConcat(prepareChunk({
        {"a1", 1},
        {"b1", 1},
    }));
    ASSERT_EQ(result.size(), 0);
    ASSERT_EQ(splitter.currentSN(), 1);

    result = splitter.splitOrConcat(prepareChunk({
        {"c1", 1},
        {"d1", 1},
    }));
    ASSERT_EQ(result.size(), 0);
    ASSERT_EQ(splitter.currentSN(), 1);

    result = splitter.splitOrConcat(prepareChunk({
        {"a2", 2},
        {"b2", 2},
    }));
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result.front().sn, 1);
    checkChunk(result.front().chunk, {"a1", "b1", "c1", "d1"}, 1);
    ASSERT_EQ(splitter.currentSN(), 2);

    result = splitter.splitOrConcat(prepareChunk({
        {"c2", 2},
        {"a3", 3},
    }));
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result.front().sn, 2);
    checkChunk(result.front().chunk, {"a2", "b2", "c2"}, 2);
    ASSERT_EQ(splitter.currentSN(), 3);

    result = splitter.splitOrConcat(prepareChunk({
        {"b3", 3},
        {"a4", 4},
        {"b4", 4},
        {"a5", 5},
    }));
    ASSERT_EQ(result.size(), 2);
    ASSERT_EQ(result.front().sn, 3);
    checkChunk(result.front().chunk, {"a3", "b3"}, 3);
    ASSERT_EQ(result.back().sn, 4);
    checkChunk(result.back().chunk, {"a4", "b4"}, 4);
    ASSERT_EQ(splitter.currentSN(), 5);

    auto finalized_res = splitter.finalize();
    ASSERT_EQ(finalized_res.sn, 5);
    checkChunk(finalized_res.chunk, {"a5"}, 5);
    ASSERT_EQ(splitter.currentSN(), -1);
}

TEST(SequencedChunkSplitter, BasicAndRemoveSN)
{
    SequencedChunkSplitter splitter(1, /*remove_sn_col=*/true);

    /// The chunks may be like this:
    ///  Chunk-1         Chunk-2         Chunk-3         Chunk-4         Chunk-5
    /// +---------+     +---------+     +---------+     +---------+     +---------+
    /// |(SN=1)a1 |     |(SN=1)c1 |     |(SN=2)a2 |     |(SN=2)c2 |     |(SN=3)b3 |
    /// |(SN=1)b1 |     |(SN=1)d1 |     |(SN=2)b2 |     |(SN=3)a3 |     |(SN=4)a4 |
    /// +_________+     +_________+     +_________+     +_________+     |(SN=4)b4 |
    ///                                                                 |(SN=5)a5 |
    ///                                                                 +_________+
    auto result = splitter.splitOrConcat(prepareChunk({
        {"a1", 1},
        {"b1", 1},
    }));
    ASSERT_EQ(result.size(), 0);
    ASSERT_EQ(splitter.currentSN(), 1);

    result = splitter.splitOrConcat(prepareChunk({
        {"c1", 1},
        {"d1", 1},
    }));
    ASSERT_EQ(result.size(), 0);
    ASSERT_EQ(splitter.currentSN(), 1);

    result = splitter.splitOrConcat(prepareChunk({
        {"a2", 2},
        {"b2", 2},
    }));
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result.front().sn, 1);
    checkChunk(result.front().chunk, {"a1", "b1", "c1", "d1"});
    ASSERT_EQ(splitter.currentSN(), 2);

    result = splitter.splitOrConcat(prepareChunk({
        {"c2", 2},
        {"a3", 3},
    }));
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result.front().sn, 2);
    checkChunk(result.front().chunk, {"a2", "b2", "c2"});
    ASSERT_EQ(splitter.currentSN(), 3);

    result = splitter.splitOrConcat(prepareChunk({
        {"b3", 3},
        {"a4", 4},
        {"b4", 4},
        {"a5", 5},
    }));
    ASSERT_EQ(result.size(), 2);
    ASSERT_EQ(result.front().sn, 3);
    checkChunk(result.front().chunk, {"a3", "b3"});
    ASSERT_EQ(result.back().sn, 4);
    checkChunk(result.back().chunk, {"a4", "b4"});
    ASSERT_EQ(splitter.currentSN(), 5);

    auto finalized_res = splitter.finalize();
    ASSERT_EQ(finalized_res.sn, 5);
    checkChunk(finalized_res.chunk, {"a5"});
    ASSERT_EQ(splitter.currentSN(), -1);
}
