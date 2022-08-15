#pragma once

#include <fmt/format.h>

namespace DB
{
namespace Streaming
{
struct JoinMetrics
{
    size_t current_total_blocks = 0;
    size_t current_total_bytes = 0;
    size_t total_blocks = 0;
    size_t total_bytes = 0;
    size_t gced_blocks = 0;

    std::string string() const
    {
        return fmt::format(
            "total_bytes={} total_blocks={} current_total_bytes={} current_total_blocks={} gced_blocks={}",
            total_bytes,
            total_blocks,
            current_total_bytes,
            current_total_blocks,
            gced_blocks);
    }
};

struct JoinGlobalMetrics
{
    size_t total_join = 0;
    size_t no_new_data_skip = 0;
    size_t time_bucket_no_new_data_skip = 0;
    size_t time_bucket_no_intersection_skip = 0;
    size_t left_block_and_right_time_bucket_no_intersection_skip = 0;
    size_t only_join_new_data = 0;

    std::string string() const
    {
        return fmt::format(
            "total_join={} no_new_data_skip={} time_bucket_no_new_data_skip={} time_bucket_no_intersection_skip={}  "
            "left_block_and_right_time_bucket_no_intersection_skip={} only_join_new_data={}",
            total_join,
            no_new_data_skip,
            time_bucket_no_new_data_skip,
            time_bucket_no_intersection_skip,
            left_block_and_right_time_bucket_no_intersection_skip,
            only_join_new_data);
    }
};

}
}
