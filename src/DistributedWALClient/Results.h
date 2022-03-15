#pragma once

#include "Record.h"
#include "SchemaProvider.h"

namespace DWAL
{
struct AppendResult
{
    /// 0 if success, otherwise non-zero
    int32_t err = 0;

    RecordSN sn = -1;

    /// Other context
    int32_t partition = -1;
};

struct ConsumeResult
{
    int32_t err = 0;
    RecordPtrs records;
};

using AppendCallback = void (*)(const AppendResult & result, void * data);

using ConsumeCallbackData = SchemaProvider;

using ConsumeCallback = void (*)(RecordPtrs records, ConsumeCallbackData * data);

using ConsumeRawCallback = void (*)(void * payload, size_t payload_len, size_t total_count, int64_t sn, int32_t partition, int64_t append_time, void * data);

struct DescribeResult
{
    int32_t err = 0;

    int32_t partitions;
};
}
