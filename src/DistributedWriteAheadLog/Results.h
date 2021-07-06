#pragma once

#include "Record.h"

#include <any>

namespace DWAL
{
struct AppendResult
{
    RecordSN sn = -1;

    /// 0 if success, otherwise non-zero
    Int32 err = 0;

    /// Other context
    std::any ctx;
};

struct ConsumeResult
{
    Int32 err = 0;
    RecordPtrs records;
};

using AppendCallback = void (*)(const AppendResult & result, void * data);
using ConsumeCallback = void (*)(RecordPtrs records, void * data);
}
