#pragma once

namespace DB
{
namespace Streaming
{
struct TimestampFunctionDescription;
using TimestampFunctionDescriptionMutablePtr = std::shared_ptr<TimestampFunctionDescription>;
using TimestampFunctionDescriptionPtr = std::shared_ptr<const TimestampFunctionDescription>;
using TimestampFunctionDescriptionPtrs = std::vector<TimestampFunctionDescriptionPtr>;

}
}
