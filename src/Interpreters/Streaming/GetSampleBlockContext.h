#pragma once

#include <Core/Streaming/DataStreamSemantic.h>

namespace DB
{
namespace Streaming
{
struct GetSampleBlockContext
{
    DataStreamSemanticEx output_data_stream_semantic = DataStreamSemantic::Append;
    bool is_streaming_output = false;
};

}
}