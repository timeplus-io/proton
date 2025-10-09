#include <Processors/Executors/ExecutingGraph.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Processors/PlaceholdProcessor.h>
#include <Processors/Sinks/EmptySink.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Streaming/ISource.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>
#include <Common/scope_guard_safe.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <ranges>

namespace CurrentMetrics
{
extern const Metric LocalThread;
extern const Metric LocalThreadActive;
}

namespace DB
{

namespace ErrorCodes
{
extern const int RECOVER_CHECKPOINT_FAILED;
}

void ExecutingGraph::serialize(WriteBuffer & wb) const
{
    /// Graph layout
    /// [num_processors][processor][processor]...
    UInt16 num_processors_to_serde = processors_indices_to_serde.size();
    DB::writeIntBinary(num_processors_to_serde, wb);
    for (auto index : processors_indices_to_serde)
        processors->at(index)->marshal(wb);
}

void ExecutingGraph::deserialize(ReadBuffer & rb) const
{
    /// Graph layout
    /// [num_processors][processor][processor]...
    UInt16 num_processors_to_serde = 0;
    DB::readIntBinary(num_processors_to_serde, rb);
    chassert(num_processors_to_serde > 0);

    Processors recovered_processors;
    recovered_processors.reserve(num_processors_to_serde);
    for (UInt16 i = 0; i < num_processors_to_serde; ++i)
    {
        auto processor = std::make_shared<PlaceholdProcessor>();
        processor->setName(processor->unmarshal(rb));
        recovered_processors.push_back(std::move(processor));
    }

    LoggerPtr logger = getLogger("ExecutingGraph");

    /// Validate the recovered processors and new planned stateful processors
    if (recovered_processors.size() != processors_indices_to_serde.size())
        LOG_WARNING(
            logger,
            "Processor graph mismatch: checkpointed={}, current={}. This may indicate an old format",
            recovered_processors.size(),
            processors_indices_to_serde.size());

    auto stateful_processors
        = processors_indices_to_serde | std::views::transform([&](auto i) -> ProcessorPtr & { return processors->at(i); });
    auto stateful_processor_it = stateful_processors.begin();
    for (UInt32 logic_id = 0; const auto & recovered_processor : recovered_processors)
    {
        /// Skip recovered stateless processors
        if (stateful_processor_it == stateful_processors.end() || (*stateful_processor_it)->getID() != recovered_processor->getID())
        {
            LOG_WARNING(
                logger,
                "Skipped mismatched processor ({}), likely a stateless processor from an older format",
                recovered_processor->getName());
            ++logic_id;
            continue;
        }

        /// NOTE: If there are old stateless processors, we need to reset the logic id for compatibility
        if ((*stateful_processor_it)->getLogicID() != logic_id)
            (*stateful_processor_it)->setLogicID(logic_id);

        ++stateful_processor_it;
        ++logic_id;
    }

    /// If there are still some stateful processors is not validated in the new plan, it means that the checkpointed processors are not matched
    if (stateful_processor_it != stateful_processors.end())
        throw Exception(
            ErrorCodes::RECOVER_CHECKPOINT_FAILED,
            "Found unmatched stateful processor '{}': checkpointed=[{}], current=[{}]",
            (*stateful_processor_it)->getName(),
            fmt::format("{}", fmt::join(recovered_processors | std::views::transform([](const auto & p) { return p->getName(); }), ",")),
            fmt::format(
                "{}",
                fmt::join(processors_indices_to_serde | std::views::transform([&](auto i) { return processors->at(i)->getName(); }), ",")));

    /// FIXME, check the graph shape
}

void ExecutingGraph::recover(CheckpointContextPtr ckpt_ctx)
{
    /// Recover processors in parallel
    ThreadPool pool{CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive};
    for (auto & processor : *processors)
    {
        if (processor->isStreaming() && processor->hasState())
            pool.scheduleOrThrowOnError([&, thread_group = CurrentThread::getGroup()]() {
                SCOPE_EXIT_SAFE(if (thread_group) CurrentThread::detachFromGroupIfNotDetached(););
                if (thread_group)
                    CurrentThread::attachToGroup(thread_group);
                processor->recover(ckpt_ctx);
            });
    }

    pool.wait();
}

void ExecutingGraph::initCheckpointNodes()
{
    if (!checkpoint_ack_nodes.empty())
        return;

    processors_indices_to_serde.clear();
    processors_indices_to_serde.reserve(processors->size());
    UInt32 logic_id = 0;
    for (UInt16 i = 0; auto & node : nodes)
    {
        if (node->processor->isStreaming() && node->processor->hasState())
        {
            node->processor->setLogicID(logic_id++);
            processors_indices_to_serde.emplace_back(i);
        }

        if (node->back_edges.empty())
        {
            assert(node->processor->isSource());
            if (node->processor->isStreaming())
                checkpoint_trigger_nodes.push_back(node.get());
        }

        /// There are two sink cases:
        /// 1) Common case: ... -> SinkToStorage (e.g. StreamSink/KafkaSink ...) -> EmptySink
        /// 2) Other case:  ... -> Sink (ExternalTableDataSink)
        if (dynamic_cast<SinkToStorage *>(node->processor))
        {
            checkpoint_ack_nodes.push_back(node.get());
        }

        if (node->direct_edges.empty())
        {
            assert(node->processor->isSink());
            if (dynamic_cast<EmptySink *>(node->processor) == nullptr)
                checkpoint_ack_nodes.push_back(node.get());
        }

        ++i;
    }

    /// Also sets the logical ids of checkpoint_ack_nodes, which are used to track ack checkpointed.
    /// logicial processors contain: 1) stateful processors and 2) ack processors
    chassert(!checkpoint_trigger_nodes.empty() && !checkpoint_ack_nodes.empty());
    for (auto * node : checkpoint_ack_nodes)
    {
        if (node->processor->getLogicID() == std::numeric_limits<UInt32>::max())
            node->processor->setLogicID(1000 + logic_id++); /// 1000 is a magic number to avoid conflict with stateless processors
    }
}

bool ExecutingGraph::hasProcessedNewDataSinceLastCheckpoint() const noexcept
{
    for (const auto * node : checkpoint_trigger_nodes)
    {
        const auto * streaming_source = dynamic_cast<const Streaming::ISource *>(node->processor);
        if (streaming_source && streaming_source->hasProcessedNewDataSinceLastCheckpoint())
            return true;
    }
    return false;
}

void ExecutingGraph::triggerCheckpoint(CheckpointContextPtr ckpt_ctx)
{
    for (auto * node : checkpoint_trigger_nodes)
        node->processor->checkpoint(ckpt_ctx);
}

String ExecutingGraph::getStats() const
{
    Poco::JSON::Object status;
    Poco::JSON::Array node_list;
    Poco::JSON::Array edges;

    for (const auto & node : nodes)
    {
        Poco::JSON::Object n;

        n.set("name", node->processor->getName());
        n.set("id", node->processors_id);
        n.set("status", IProcessor::statusToName(node->last_processor_status));

        for (auto eg = node->direct_edges.begin(); eg != node->direct_edges.end(); ++eg)
        {
            Poco::JSON::Object edge;
            edge.set("from", node->processors_id);
            edge.set("to", eg->to);
            edges.add(edge);
        }

        Poco::JSON::Object metrices;
        auto metric = node->processor->getMetrics();

        metrices.set("processed_time_ns", metric.processed_time_ns);
        metrices.set("processed_bytes", metric.processed_bytes);
        metrices.set("processed_rows", metric.processed_rows);

        if (metric.last_processed_sn_range.has_value())
        {
            metrices.set("last_processed_sn_start", metric.last_processed_sn_range->first);
            metrices.set("last_processed_sn_end", metric.last_processed_sn_range->second);
        }

        if (!metric.last_error_message.empty())
            metrices.set("last_error_message", metric.last_error_message);

        n.set("metric", metrices);
        node_list.add(n);
    }
    status.set("nodes", node_list);
    status.set("edges", edges);

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    status.stringify(oss);
    return oss.str();
}

std::vector<ExecutingGraph::NodeDescription> ExecutingGraph::statefulNodeDescriptions() const
{
    std::vector<NodeDescription> descs;
    descs.reserve(processors_indices_to_serde.size());
    for (UInt32 logic_id = 0; auto idx : processors_indices_to_serde)
        descs.emplace_back(logic_id++, nodes[idx]->processor->getName(), nodes[idx]->processor->getDescription());

    return descs;
}

std::vector<ExecutingGraph::NodeDescription> ExecutingGraph::statefulNodeDescriptions(const Processors & processors_)
{
    std::vector<NodeDescription> descs;
    descs.reserve(processors_.size());
    for (UInt32 logic_id = 0; const auto & processor : processors_)
    {
        if (processor->isStreaming() && processor->hasState())
            descs.emplace_back(logic_id++, processor->getName(), processor->getDescription());
    }
    return descs;
}

std::vector<ExecutingGraph::NodeDescription> ExecutingGraph::checkpointAckNodeDescriptions() const
{
    std::vector<NodeDescription> descs;
    descs.reserve(checkpoint_ack_nodes.size());
    for (const auto * node : checkpoint_ack_nodes)
        descs.emplace_back(node->processor->getLogicID(), node->processor->getName(), node->processor->getDescription());

    return descs;
}

String formatNodeDescriptions(const std::vector<ExecutingGraph::NodeDescription> & node_descs)
{
    return fmt::format("{}", fmt::join(node_descs | std::views::transform([](const auto & desc) { return desc.string(); }), ", "));
}

}
