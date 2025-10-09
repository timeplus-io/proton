#include <Processors/Streaming/SequencedChunkSplitter.h>

#include <Columns/ColumnsNumber.h>
#include <Common/PODArray.h>

#include <numeric>

namespace DB::Streaming
{
std::list<SequenceChunk> SequencedChunkSplitter::splitOrConcat(Chunk && chunk)
{
    chassert(chunk.hasRows());

    std::list<SequenceChunk> result;

    auto sn_column = chunk.getColumns()[sn_col_pos];
    const auto & sns = assert_cast<const ColumnInt64 &>(*sn_column).getData();
    if (remove_sn_col)
        chunk.erase(sn_col_pos);

    auto first_sn = sns.front();
    auto last_sn = sns.back();

    /// Initialize buffered_sn with the first SN in the chunk
    if (buffered_sn == -1)
        buffered_sn = first_sn;

    /// The result of historical query may has multiple sn data or partial sn data in one chunk, we need to re-generate corresponding records based on SNs
    /// The chunks may be like this:
    ///  Chunk-1         Chunk-2         Chunk-3         Chunk-4
    /// +---------+     +---------+     +---------+     +---------+
    /// |(SN=1) 1 |     |(SN=1) 3 |     |(SN=2) 1 |     |(SN=2) 3 |
    /// |(SN=1) 2 |     |(SN=1) 4 |     |(SN=2) 2 |     |(SN=3) 1 |
    /// |_________+     |_________+     |_________+     |_________+
    ///
    /// - Data (SN=1) is placed in Chunk-1 and Chunk-2
    /// - Data (SN=2) is placed in Chunk-3 and Chunk-4
    /// - Data (SN=3) is placed in Chunk-4
    /// As the example above, there are three cases need to handle:
    /// 1) For Chunk-1 and Chunk-2, they contain same SN=1 data and is the same as current chunk, just append to \buffered_chunk.
    /// Now: \buffered_chunk is [1, 2, 3, 4] (SN=1)
    if (buffered_sn == last_sn)
    {
        buffered_chunk.append(std::move(chunk));
    }
    /// 2) For Chunk-3, the chunk contains same SN data but is different with current chunk, so we can output the \buffered_chunk (SN=1) first
    /// Output: chunk is [1,2,3,4] (SN=1)   Now: \buffered_chunk is [1, 2] (SN=2)
    else if (first_sn == last_sn)
    {
        result.emplace_back(buffered_sn, std::move(buffered_chunk));
        buffered_sn = last_sn;
        buffered_chunk.swap(chunk);
    }
    /// 3) For Chunk-4, the chunk contains different SN data, we need to split the chunk into several chunks by SN, and output prev sn chunk when saw a new SN.
    /// Output: chunk is [1, 2, 3] (SN=2)   Now: \buffered_chunk = [1] (SN=3)
    else
    {
        std::vector<Int64> chunk_sns;
        IColumn::Selector selector(chunk.rows(), 0);
        size_t chunk_idx = 0;
        /// Skipping the first batch of \buffered_sn (selector is always 0)
        auto curr_batch_sn = first_sn;
        chunk_sns.emplace_back(curr_batch_sn);
        auto it = std::ranges::find_if(sns, [curr_batch_sn](const auto & sn) { return sn != curr_batch_sn; });
        chassert(it != sns.end());
        for (size_t i = std::ranges::distance(sns.begin(), it); it != sns.end(); ++it, ++i)
        {
            auto & sn = *it;
            if (sn != curr_batch_sn)
            {
                chassert(sn > curr_batch_sn);
                ++chunk_idx;
                curr_batch_sn = sn;
                chunk_sns.emplace_back(sn);
            }
            selector[i] = chunk_idx;
        }

        auto chunk_num = chunk_idx + 1;
        chassert(chunk_num == chunk_sns.size());

        std::vector<MutableColumns> columns_of_chunks(chunk_num);
        for (const auto & column : chunk.getColumns())
        {
            auto columns = column->scatter(chunk_num, selector);
            for (size_t chunk_i = 0; auto & col : columns)
                columns_of_chunks[chunk_i++].emplace_back(std::move(col));
        }

        for (size_t i = 0; auto & columns : columns_of_chunks)
        {
            auto & chunk_sn = chunk_sns[i++];
            if (buffered_sn != chunk_sn)
            {
                result.emplace_back(buffered_sn, std::move(buffered_chunk));
                buffered_sn = chunk_sn;
            }

            auto chunk_rows = columns.front()->size();
            buffered_chunk.append(Chunk{std::move(columns), chunk_rows});
        }
    }

    return result;
}

SequenceChunk SequencedChunkSplitter::finalize()
{
    auto sn = buffered_sn;
    buffered_sn = -1;
    return {sn, std::move(buffered_chunk)};
}
}
