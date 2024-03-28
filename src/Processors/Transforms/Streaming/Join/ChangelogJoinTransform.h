#pragma once

#include <Processors/Transforms/Streaming/JoinTransform.h>

namespace DB
{
namespace Streaming
{
/// Streaming join rows from left stream to right stream
/// It has 2 inputs, the first one is left stream and the second one is right stream.
/// These 2 input streams will be pulled concurrently
/// left stream -> ... ->
///                      \
///                      ChangelogJoinTransform
///                      /
/// right stream -> ... ->
class ChangelogJoinTransform final : public JoinTransform
{
public:
    using JoinTransform::JoinTransform;
    String getName() const override { return "StreamingChangelogJoinTransform"; }
};
}
}
