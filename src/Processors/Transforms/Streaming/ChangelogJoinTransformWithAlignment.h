#pragma once

#include <Processors/Transforms/Streaming/JoinTransformWithAlignment.h>

namespace DB::Streaming
{

/// Streaming join rows from left stream to right stream
/// It has 2 inputs, the first one is left stream and the second one is right stream.
/// These 2 input streams will be pulled concurrently and have watermark / timestamp
/// alignment for temporal join scenarios.
/// left stream -> ... ->
///                      \
///                      ChangelogJoinTransformWithAlignment
///                      /
/// right stream -> ... ->
class ChangelogJoinTransformWithAlignment final : public JoinTransformWithAlignment
{
public:
    using JoinTransformWithAlignment::JoinTransformWithAlignment;
    String getName() const override { return "StreamingChangelogJoinTransformWithAlignment"; }
};
}
