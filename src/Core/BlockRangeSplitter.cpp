#include "BlockRangeSplitter.h"

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnVector.h>

namespace DB
{
namespace
{
    template <typename IntegralColumnType>
    struct BlockRangeSplitter : public BlockRangeSplitterInf
    {
        BlockRangeSplitter(size_t split_column_pos_, UInt64 range_, bool calc_min_max_)
            : split_column_pos(split_column_pos_), range(range_), calc_min_max(calc_min_max_)
        {
            assert(range);
        }

        std::vector<std::pair<UInt64, Block>> split(Block block) const override
        {
            assert(block.rows());

            const auto & col_with_type = block.getByPosition(split_column_pos);

            auto * col = typeid_cast<IntegralColumnType *>(col_with_type.column->assumeMutable().get());
            assert(col);

            if (block.rows() == 1)
            {
                /// fast path
                calculateMinMax(block);
                auto time_bucket = bucket(col->getData());
                return std::vector<std::pair<UInt64, Block>>{1, std::pair<UInt64, Block>(time_bucket, std::move(block))};
            }

            std::unordered_map<UInt64, UInt64> bucket_to_column_num;
            IColumn::Selector selector(block.rows(), 0);

            calculateSelector(col->getData(), bucket_to_column_num, selector);

            if (bucket_to_column_num.size() == 1)
            {
                calculateMinMax(block);
                return std::vector<std::pair<UInt64, Block>>{
                    1, std::pair<UInt64, Block>(bucket_to_column_num.begin()->first, std::move(block))};
            }

            std::unordered_map<UInt64, UInt64> column_num_to_bucket;
            column_num_to_bucket.reserve(bucket_to_column_num.size());
            for (const auto & p : bucket_to_column_num)
                column_num_to_bucket[p.second] = p.first;

            std::vector<std::pair<UInt64, Block>> split_blocks(
                bucket_to_column_num.size(), std::pair<UInt64, Block>(0, block.cloneEmpty()));

            for (size_t col_pos = 0; auto & col_with_name_type : block)
            {
                auto split_cols{col_with_name_type.column->scatter(bucket_to_column_num.size(), selector)};
                assert(split_cols.size() == split_blocks.size());

                for (size_t col_num = 0; auto & split_col : split_cols)
                {
                    split_blocks[col_num].first = column_num_to_bucket.at(col_num);
                    split_blocks[col_num].second.getByPosition(col_pos).column = std::move(split_col);
                    ++col_num;
                }
                ++col_pos;
            }

            if (calc_min_max)
            {
                for (auto & bucket_block : split_blocks)
                    calculateMinMax(bucket_block.second);
            }

            return split_blocks;
        }

    private:
        void calculateMinMax(Block & block) const
        {
            if (!calc_min_max)
                return;

            const auto & split_col = block.getByPosition(split_column_pos);
            auto col = typeid_cast<IntegralColumnType *>(split_col.column->assumeMutable().get());

            assert(col);

            auto result{std::minmax_element(col->getData().begin(), col->getData().end())};
            if constexpr (std::is_same_v<IntegralColumnType, DB::ColumnDecimal<DB::DateTime64>>)
            {
                block.info.watermark_lower_bound = result.first->value;
                block.info.watermark = result.second->value;
            }
            else
            {
                block.info.watermark_lower_bound = *result.first;
                block.info.watermark = *result.second;
            }
        }

        void ALWAYS_INLINE calculateSelectorOneRow(
            UInt64 value, size_t row_num, std::unordered_map<UInt64, UInt64> & bucket_to_column_num, IColumn::Selector & selector) const
        {
            auto bucket = (value / range) * range;
            auto iter = bucket_to_column_num.find(bucket);
            if (iter != bucket_to_column_num.end())
            {
                selector[row_num] = iter->second;
            }
            else
            {
                selector[row_num] = bucket_to_column_num.size();
                bucket_to_column_num.emplace(bucket, selector[row_num]);
            }
        }

        template <typename Container>
        void calculateSelector(
            const Container & data_container, std::unordered_map<UInt64, UInt64> & bucket_to_column_num, IColumn::Selector & selector) const
        {
            for (size_t row_num = 0; auto v : data_container)
            {
                calculateSelectorOneRow(v, row_num, bucket_to_column_num, selector);
                ++row_num;
            }
        }

        void calculateSelector(
            const DB::ColumnDecimal<DB::DateTime64>::Container & data_container,
            std::unordered_map<UInt64, UInt64> & bucket_to_column_num,
            IColumn::Selector & selector) const
        {
            for (size_t row_num = 0; auto v : data_container)
            {
                calculateSelectorOneRow(v, row_num, bucket_to_column_num, selector);
                ++row_num;
            }
        }

        /// bucket for one row block
        template <typename Container>
        UInt64 bucket(const Container & data_container) const
        {
            return data_container[0] / range * range;
        }

        UInt64 bucket(const DB::ColumnDecimal<DB::DateTime64>::Container & data_container) const
        {
            return data_container[0].value / range * range;
        }

    private:
        size_t split_column_pos;
        UInt64 range;
        bool calc_min_max;
    };
}

std::unique_ptr<BlockRangeSplitterInf>
createBlockRangeSplitter(TypeIndex type_index, size_t split_column_pos, UInt64 range, bool calc_min_max)
{
    switch (type_index)
    {
        case TypeIndex::DateTime64:
            return std::make_unique<BlockRangeSplitter<DB::ColumnDecimal<DB::DateTime64>>>(split_column_pos, range, calc_min_max);

        case TypeIndex::DateTime:
        case TypeIndex::UInt32:
            return std::make_unique<BlockRangeSplitter<DB::ColumnVector<UInt32>>>(split_column_pos, range, calc_min_max);

        case TypeIndex::Date:
        case TypeIndex::UInt16:
            return std::make_unique<BlockRangeSplitter<DB::ColumnVector<UInt16>>>(split_column_pos, range, calc_min_max);

        case TypeIndex::Date32:
        case TypeIndex::Int32:
            return std::make_unique<BlockRangeSplitter<DB::ColumnVector<Int32>>>(split_column_pos, range, calc_min_max);

        case TypeIndex::Int8:
            return std::make_unique<BlockRangeSplitter<DB::ColumnVector<Int8>>>(split_column_pos, range, calc_min_max);

        case TypeIndex::Int16:
            return std::make_unique<BlockRangeSplitter<DB::ColumnVector<Int16>>>(split_column_pos, range, calc_min_max);

        case TypeIndex::Int64:
            return std::make_unique<BlockRangeSplitter<DB::ColumnVector<Int64>>>(split_column_pos, range, calc_min_max);

        case TypeIndex::UInt8:
            return std::make_unique<BlockRangeSplitter<DB::ColumnVector<UInt8>>>(split_column_pos, range, calc_min_max);

        case TypeIndex::UInt64:
            return std::make_unique<BlockRangeSplitter<DB::ColumnVector<UInt64>>>(split_column_pos, range, calc_min_max);

        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Range split only supports splitting on integral column");
    }
}
}
