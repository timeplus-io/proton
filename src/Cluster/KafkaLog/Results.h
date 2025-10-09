#pragma once

#include <Cluster/SchemaRecord/SchemaProvider.h>
#include <Cluster/SchemaRecord/SchemaRecord.h>

namespace cluster::klog
{
struct AppendResult
{
    /// 0 if success, otherwise non-zero
    int32_t err = 0;

    int64_t sn = -1;

    /// Other context
    int32_t partition = -1;
};

struct ConsumeResult
{
    int32_t err = 0;
    SchemaRecordPtrs records;
};

using CallbackData = std::shared_ptr<void>;

using AppendCallback = void (*)(const AppendResult & result, const CallbackData & data);

using ConsumeCallbackData = SchemaProvider;

using ConsumeCallback = void (*)(SchemaRecordPtrs records, ConsumeCallbackData * data);

using ConsumeRawCallback = void (*)(void * kmessage, size_t total_count, void * data);

struct DescribeResult
{
    int32_t err = 0;

    int32_t partitions = 0;
};
}
