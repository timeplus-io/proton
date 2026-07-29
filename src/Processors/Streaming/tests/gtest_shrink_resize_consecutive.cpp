/// Regression: ShrinkResizeProcessor (M inputs -> 1 output) must not
/// interleave a chunk from another input between a consecutive (-1) chunk and its
/// partner (+1) on the same input. Otherwise a downstream EMIT ON UPDATE substream
/// watermark transform finalizes the half-applied retract and leaks a spurious row.

#include <Processors/Streaming/ResizeProcessor.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Port.h>

#include <gtest/gtest.h>

namespace DB::Streaming
{
namespace
{

Block makeHeader()
{
    Block h;
    h.insert({ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "v"});
    return h;
}

/// Streaming source whose single output port the test pushes into by hand.
class HandSource final : public IProcessor
{
public:
    explicit HandSource(Block header) : IProcessor(InputPorts{}, OutputPorts{std::move(header)}, ProcessorID::NullSourceID)
    {
        setStreaming(true);
    }
    String getName() const override { return "HandSource"; }
    Status prepare() override { return Status::PortFull; }
    OutputPort & out() { return outputs.front(); }
};

/// Sink whose single input port the test pulls from by hand.
class HandSink final : public IProcessor
{
public:
    explicit HandSink(Block header) : IProcessor(InputPorts{std::move(header)}, OutputPorts{}, ProcessorID::NullSinkToStorageID) { }
    String getName() const override { return "HandSink"; }
    Status prepare() override { return Status::NeedData; }
    InputPort & in() { return inputs.front(); }
};

Chunk dataChunk(UInt64 tag, bool consecutive)
{
    auto col = ColumnUInt64::create();
    col->insert(tag);
    Columns cols;
    cols.emplace_back(std::move(col));
    Chunk c(std::move(cols), 1);
    if (consecutive)
        c.setConsecutiveDataFlag();
    return c;
}

Chunk heartbeatChunk()
{
    /// One column, zero rows: a "no rows" heartbeat chunk.
    auto col = ColumnUInt64::create();
    Columns cols;
    cols.emplace_back(std::move(col));
    return Chunk(std::move(cols), 0);
}

/// Row-0 tag, or -1 sentinel for an empty (heartbeat) chunk.
Int64 tagOf(const Chunk & c)
{
    if (c.rows() == 0)
        return -1;
    return static_cast<Int64>(typeid_cast<const ColumnUInt64 &>(*c.getColumns()[0]).getUInt(0));
}

/// Two sources -> ShrinkResizeProcessor(2->1) -> sink, wired and initialized.
struct Harness
{
    std::shared_ptr<HandSource> src0;
    std::shared_ptr<HandSource> src1;
    std::shared_ptr<ShrinkResizeProcessor> resize;
    std::shared_ptr<HandSink> sink;
};

Harness makeHarness(const Block & header)
{
    Harness h{
        std::make_shared<HandSource>(header),
        std::make_shared<HandSource>(header),
        std::make_shared<ShrinkResizeProcessor>(header, 2),
        std::make_shared<HandSink>(header)};

    auto in_it = h.resize->getInputs().begin();
    connect(h.src0->out(), *in_it++);
    connect(h.src1->out(), *in_it);
    connect(h.resize->getOutputs().front(), h.sink->in());

    h.sink->in().setNeeded();
    h.resize->prepare({}, {}); /// init
    return h;
}

/// Pull one buffered chunk if present, appending its tag to `got` when given.
void drainSink(HandSink & sink, std::vector<Int64> * got = nullptr)
{
    if (sink.in().hasData())
    {
        auto c = sink.in().pull();
        if (got)
            got->push_back(tagOf(c));
        sink.in().setNeeded();
    }
}

using PN = IProcessor::PortNumbers;

} // namespace

TEST(ShrinkResizeConsecutive, PairNotSplitByForeignHeartbeat)
{
    auto h = makeHarness(makeHeader());
    std::vector<Int64> got;

    /// input0 emits the leading -1 of a pair.
    h.src0->out().push(dataChunk(100, /*consecutive=*/true));
    h.resize->prepare(PN{0}, PN{});
    drainSink(*h.sink, &got);

    /// input1 (another shard) emits a heartbeat while the pair is still open.
    h.src1->out().push(heartbeatChunk());
    auto st = h.resize->prepare(PN{1}, PN{});
    drainSink(*h.sink, &got);
    EXPECT_EQ(st, IProcessor::Status::NeedData) << "must wait for the +1 partner, not service input1";
    ASSERT_EQ(got.size(), 1u) << "foreign heartbeat leaked between -1 and +1";

    /// input0 emits the trailing +1; it is pulled before input1's heartbeat.
    h.src0->out().push(dataChunk(200, /*consecutive=*/false));
    h.resize->prepare(PN{0}, PN{});
    drainSink(*h.sink, &got);

    /// The held-back heartbeat is delivered only now.
    h.resize->prepare(PN{}, PN{});
    drainSink(*h.sink, &got);

    ASSERT_EQ(got.size(), 3u);
    EXPECT_EQ(got[0], 100); /// -1
    EXPECT_EQ(got[1], 200); /// +1 immediately follows its -1
    EXPECT_EQ(got[2], -1); /// heartbeat delivered after the pair
}

/// If the pinned input finishes mid-pair (delivers finish instead of its +1),
/// the pin must be released and other inputs holding buffered data still serviced
/// in the same cycle — no stall.
TEST(ShrinkResizeConsecutive, PinReleasedWhenInputFinishesMidPair)
{
    auto h = makeHarness(makeHeader());
    std::vector<Int64> got;

    /// input0 emits the leading -1, opening a pair (pins input0).
    h.src0->out().push(dataChunk(100, /*consecutive=*/true));
    h.resize->prepare(PN{0}, PN{});
    drainSink(*h.sink, &got);
    ASSERT_EQ(got.size(), 1u);

    /// input1 buffers a chunk; input0 finishes without ever sending its +1.
    h.src1->out().push(dataChunk(300, /*consecutive=*/false));
    h.src0->out().finish();
    h.resize->prepare(PN{0, 1}, PN{});
    drainSink(*h.sink, &got);

    /// input1's buffered chunk must still be delivered despite the pin on the now-finished input0.
    ASSERT_EQ(got.size(), 2u) << "pinned-input finish stalled a ready sibling input";
    EXPECT_EQ(got[1], 300);
}

/// A hot input streaming (-1, +1) pairs must not accumulate stale inputs_with_data
/// entries: the exclusive-path pull bypasses the queue, so the pinned input must not
/// be queued in the first place.
TEST(ShrinkResizeConsecutive, QueueDoesNotLeakOnHotConsecutivePairs)
{
    auto h = makeHarness(makeHeader());

    for (int p = 0; p < 50; ++p)
    {
        h.src0->out().push(dataChunk(2 * p, /*consecutive=*/true)); /// -1
        h.resize->prepare(PN{0}, PN{});
        drainSink(*h.sink);
        h.src0->out().push(dataChunk(2 * p + 1, /*consecutive=*/false)); /// +1
        h.resize->prepare(PN{0}, PN{});
        drainSink(*h.sink);
    }

    /// Bounded (oscillates ~0-1), not ~50.
    EXPECT_LE(h.resize->numQueuedInputsForTest(), 2u) << "inputs_with_data leaked stale entries";
}

} // namespace DB::Streaming
