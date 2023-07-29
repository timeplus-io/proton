#pragma once

#include <NativeLog/Record/Record.h>
#include <NativeLog/Record/SchemaProvider.h>

namespace klog
{
struct AppendResult
{
    /// 0 if success, otherwise non-zero
    int32_t err = 0;

    nlog::RecordSN sn = -1;

    /// Other context
    int32_t partition = -1;
};

struct ConsumeResult
{
    int32_t err = 0;
    nlog::RecordPtrs records;
};

using CallbackData = std::shared_ptr<void>;

using AppendCallback = void (*)(const AppendResult & result, const CallbackData & data);

using ConsumeCallbackData = nlog::SchemaProvider;

using ConsumeCallback = void (*)(nlog::RecordPtrs records, ConsumeCallbackData * data);

using ConsumeRawCallback = void (*)(void * kmessage, size_t total_count, void * data);

struct DescribeResult
{
    int32_t err = 0;

    int32_t partitions;
};
}
