#include <Interpreters/DirectCrossJoin.h>

#include <base/scope_guard.h>
#include <Common/logger_useful.h>

#include <numeric>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{
struct NotProcessedDirectCrossJoin : public ExtraBlock
{
    size_t left_row_start;

    ChunkList right_chunks;
    size_t right_row_start;
};

std::pair<Names, std::vector<size_t>> getOriginalRequiredColumns(const Block & right_sample_block, const TableJoin & table_join)
{
    Names right_names;
    right_names.reserve(right_sample_block.columns());

    std::vector<size_t> right_positions;
    right_positions.reserve(right_sample_block.columns());

    for (size_t i = 0; const auto & col : right_sample_block)
    {
        auto col_name = table_join.getOriginalName(table_join.restoreRightColumnNameForAlias(col.name));
        if (!std::ranges::contains(right_names, col_name))
        {
            right_names.emplace_back(std::move(col_name));
            right_positions.push_back(i);
        }
        ++i;
    }
    return {std::move(right_names), std::move(right_positions)};
}
}

DirectCrossJoin::DirectCrossJoin(
    std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block_, std::shared_ptr<IStorage> storage_, bool cache_enabled_, Int64 default_cache_expire_sec_)
    : table_join(std::move(table_join_))
    , storage(storage_)
    , right_sample_block(right_sample_block_)
    , cache_enabled(cache_enabled_)
    , default_cache_expire_sec(default_cache_expire_sec_)
    , logger(getLogger(fmt::format("Direct{}CrossJoin", storage->getName())))
{
    JoinCommon::createMissedColumns(right_sample_block);
    std::tie(required_right_names, required_right_positions) = getOriginalRequiredColumns(right_sample_block, *table_join);
    LOG_INFO(
        logger,
        "Use direct cross join with cache_enabled={}, default_cache_expire_sec={}, required right names: {}, required right positions: {}",
        cache_enabled,
        default_cache_expire_sec,
        fmt::join(required_right_names, ", "),
        fmt::join(required_right_positions, ", "));
}

bool DirectCrossJoin::addJoinedBlock(const Block &, bool)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unreachable code reached in DirectCrossJoin::addJoinedBlock");
}

void DirectCrossJoin::checkTypesOfKeys(const Block &) const
{
    if (!table_join->getClauses().empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DirectCrossJoin does not support join on clauses");
}

void DirectCrossJoin::joinBlock(Block & block, std::shared_ptr<ExtraBlock> & not_processed)
{
    if (!empty_result_block)
    {
        auto & empty_res = empty_result_block.emplace(block.cloneEmpty());

        for (const auto & pos : required_right_positions)
            empty_res.insert(right_sample_block.getByPosition(pos));
    }

    size_t num_rows = block.rows();
    if (num_rows == 0)
    {
        block = *empty_result_block;
        return;
    }

    ChunkList right_chunks;
    size_t start_left_row = 0;
    size_t start_right_row = 0;

    /// If there is a continuation of not processed rows, we need to continue from where we left off.
    if (not_processed)
    {
        auto & continuation = static_cast<NotProcessedDirectCrossJoin &>(*not_processed);
        start_left_row = continuation.left_row_start;
        right_chunks = std::move(continuation.right_chunks);
        start_right_row = continuation.right_row_start;
        not_processed.reset();
    }
    else if (cache_enabled)
    {
        /// Update the cached right data if it has expired.
        if (!cached_right_data.snapshot_expired || cached_right_data.snapshot_expired())
            cached_right_data = storage->readSnapshot(required_right_names, default_cache_expire_sec);

        for (const auto & chunk : cached_right_data.chunks)
            right_chunks.emplace_back(chunk.clone()); /// No data is copied here
    }
    else
    {
        right_chunks = storage->readSnapshot(required_right_names, default_cache_expire_sec).chunks;
    }

    auto result_columns = empty_result_block->cloneEmptyColumns();

    SCOPE_EXIT({ block = empty_result_block->cloneWithColumns(std::move(result_columns)); });

    auto left_rows_to_join = num_rows - start_left_row;
    auto right_rows_to_join
        = std::accumulate(right_chunks.begin(), right_chunks.end(), 0, [](size_t sum, const Chunk & chunk) { return sum + chunk.rows(); })
        - start_right_row;

    auto expected_rows = left_rows_to_join * right_rows_to_join;
    if (expected_rows == 0)
        return;

    /// The result of a cross join is `left_rows_to_join * right_rows_to_join` rows.
    /// To avoid generating overly large result blocks, we cap the output at `max_joined_block_rows`.
    /// If the full result exceeds this limit, the remaining rows are saved in \not_processed
    /// for processing in subsequent calls.
    size_t max_joined_block_rows = table_join->maxJoinedBlockRows();
    auto num_columns_left = block.columns();

    /// Fast path: Speeds up the single-row streaming cross join
    /// If there is only one left row, all right rows fit in the block, we can directly reuse right_chunks columns without copying.
    if (left_rows_to_join == 1 && right_rows_to_join < max_joined_block_rows && right_chunks.size() == 1 && start_right_row == 0)
    {
        auto right_columns = right_chunks.front().detachColumns();
        for (size_t col_num = 0; auto & res_col : result_columns)
        {
            if (col_num < num_columns_left)
            {
                /// Replicate the single left row for each right row
                res_col->reserve(right_rows_to_join);
                res_col->insertManyFrom(*block.getByPosition(col_num).column, start_left_row, right_rows_to_join);
            }
            else
            {
                /// Move right chunk columns directly to the result
                res_col = right_columns[col_num - num_columns_left]->assumeMutable();
            }
            ++col_num;
        }
        return;
    }

    expected_rows = max_joined_block_rows != 0 ? std::min(expected_rows, max_joined_block_rows) : expected_rows;
    for (auto & res_col : result_columns)
        res_col->reserve(expected_rows);

    for (size_t left_row = start_left_row; left_row < num_rows; ++left_row)
    {
        for (auto right_chunk_it = right_chunks.begin(); right_chunk_it != right_chunks.end(); ++right_chunk_it)
        {
            auto & right_chunk = *right_chunk_it;
            size_t rows_in_right_chunk = right_chunk.rows() - start_right_row;
            bool reached_max_block_rows = rows_in_right_chunk > expected_rows;
            auto joined_rows = reached_max_block_rows ? expected_rows : rows_in_right_chunk;

            for (size_t col_num = 0; auto & res_col : result_columns)
            {
                if (col_num < num_columns_left)
                    /// Replicate the left row for each row in the right chunk
                    res_col->insertManyFrom(*block.getByPosition(col_num).column, left_row, joined_rows);
                else
                    /// Fill the right chunk columns
                    res_col->insertRangeFrom(*right_chunk.getColumns()[col_num - num_columns_left], start_right_row, joined_rows);

                ++col_num;
            }

            /// Reached max joined block rows, store the remaining rows in \not_processed
            if (reached_max_block_rows) [[unlikely]]
            {
                chassert(!not_processed || not_processed->empty());
                not_processed = std::make_shared<NotProcessedDirectCrossJoin>(NotProcessedDirectCrossJoin{
                    {block.cloneEmpty()},
                    left_row,
                    ChunkList{std::make_move_iterator(right_chunk_it), std::make_move_iterator(right_chunks.end())},
                    start_right_row + joined_rows});
                not_processed->block.swap(block);
                return;
            }

            expected_rows -= joined_rows;
            start_right_row = 0;
        }
    }
}
}
