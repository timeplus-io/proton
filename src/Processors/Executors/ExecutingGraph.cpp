#include <Processors/Executors/ExecutingGraph.h>
#include <stack>
#include <Common/Stopwatch.h>

/// proton: starts.
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Processors/PlaceholdProcessor.h>
#include <Processors/Streaming/ISource.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
/// proton: ends.

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int RECOVER_CHECKPOINT_FAILED;
}

ExecutingGraph::ExecutingGraph(Processors & processors_, bool profile_processors_)
    : processors(processors_)
    , profile_processors(profile_processors_)
{
    uint64_t num_processors = processors.size();
    nodes.reserve(num_processors);

    /// Create nodes.
    for (uint64_t node = 0; node < num_processors; ++node)
    {
        IProcessor * proc = processors[node].get();
        processors_map[proc] = node;
        nodes.emplace_back(std::make_unique<Node>(proc, node));
    }

    /// Create edges.
    for (uint64_t node = 0; node < num_processors; ++node)
        addEdges(node);
}

ExecutingGraph::Edge & ExecutingGraph::addEdge(Edges & edges, Edge edge, const IProcessor * from, const IProcessor * to)
{
    auto it = processors_map.find(to);
    if (it == processors_map.end())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Processor {} was found as {} for processor {}, but not found in list of processors",
            to->getName(),
            edge.backward ? "input" : "output",
            from->getName());

    edge.to = it->second;
    auto & added_edge = edges.emplace_back(std::move(edge));
    added_edge.update_info.id = &added_edge;
    return added_edge;
}

bool ExecutingGraph::addEdges(uint64_t node)
{
    IProcessor * from = nodes[node]->processor;

    bool was_edge_added = false;

    /// Add backward edges from input ports.
    auto & inputs = from->getInputs();
    auto from_input = nodes[node]->back_edges.size();

    if (from_input < inputs.size())
    {
        was_edge_added = true;

        for (auto it = std::next(inputs.begin(), from_input); it != inputs.end(); ++it, ++from_input)
        {
            const IProcessor * to = &it->getOutputPort().getProcessor();
            auto output_port_number = to->getOutputPortNumber(&it->getOutputPort());
            Edge edge(0, true, from_input, output_port_number, &nodes[node]->post_updated_input_ports);
            auto & added_edge = addEdge(nodes[node]->back_edges, std::move(edge), from, to);
            it->setUpdateInfo(&added_edge.update_info);
        }
    }

    /// Add direct edges form output ports.
    auto & outputs = from->getOutputs();
    auto from_output = nodes[node]->direct_edges.size();

    if (from_output < outputs.size())
    {
        was_edge_added = true;

        for (auto it = std::next(outputs.begin(), from_output); it != outputs.end(); ++it, ++from_output)
        {
            const IProcessor * to = &it->getInputPort().getProcessor();
            auto input_port_number = to->getInputPortNumber(&it->getInputPort());
            Edge edge(0, false, input_port_number, from_output, &nodes[node]->post_updated_output_ports);
            auto & added_edge = addEdge(nodes[node]->direct_edges, std::move(edge), from, to);
            it->setUpdateInfo(&added_edge.update_info);
        }
    }

    return was_edge_added;
}

bool ExecutingGraph::expandPipeline(std::stack<uint64_t> & stack, uint64_t pid)
{
    auto & cur_node = *nodes[pid];
    Processors new_processors;

    try
    {
        new_processors = cur_node.processor->expandPipeline();
    }
    catch (...)
    {
        cur_node.exception = std::current_exception();
        return false;
    }

    {
        std::lock_guard guard(processors_mutex);
        processors.insert(processors.end(), new_processors.begin(), new_processors.end());
    }

    uint64_t num_processors = processors.size();
    std::vector<uint64_t> back_edges_sizes(num_processors, 0);
    std::vector<uint64_t> direct_edge_sizes(num_processors, 0);

    for (uint64_t node = 0; node < nodes.size(); ++node)
    {
        direct_edge_sizes[node] = nodes[node]->direct_edges.size();
        back_edges_sizes[node] = nodes[node]->back_edges.size();
    }

    nodes.reserve(num_processors);

    while (nodes.size() < num_processors)
    {
        auto * processor = processors[nodes.size()].get();
        if (processors_map.contains(processor))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Processor {} was already added to pipeline", processor->getName());

        processors_map[processor] = nodes.size();
        nodes.emplace_back(std::make_unique<Node>(processor, nodes.size()));
    }

    std::vector<uint64_t> updated_nodes;

    for (uint64_t node = 0; node < num_processors; ++node)
    {
        if (addEdges(node))
            updated_nodes.push_back(node);
    }

    for (auto updated_node : updated_nodes)
    {
        auto & node = *nodes[updated_node];

        size_t num_direct_edges = node.direct_edges.size();
        size_t num_back_edges = node.back_edges.size();

        std::lock_guard guard(node.status_mutex);

        for (uint64_t edge = back_edges_sizes[updated_node]; edge < num_back_edges; ++edge)
            node.updated_input_ports.emplace_back(edge);

        for (uint64_t edge = direct_edge_sizes[updated_node]; edge < num_direct_edges; ++edge)
            node.updated_output_ports.emplace_back(edge);

        if (node.status == ExecutingGraph::ExecStatus::Idle)
        {
            node.status = ExecutingGraph::ExecStatus::Preparing;
            stack.push(updated_node);
        }
    }

    return true;
}

void ExecutingGraph::initializeExecution(Queue & queue)
{
    std::stack<uint64_t> stack;

    /// Add childless processors to stack.
    uint64_t num_processors = nodes.size();
    for (uint64_t proc = 0; proc < num_processors; ++proc)
    {
        if (nodes[proc]->direct_edges.empty())
        {
            stack.push(proc);
            /// do not lock mutex, as this function is executed in single thread
            nodes[proc]->status = ExecutingGraph::ExecStatus::Preparing;
        }
    }

    Queue async_queue;

    while (!stack.empty())
    {
        uint64_t proc = stack.top();
        stack.pop();

        updateNode(proc, queue, async_queue);

        if (!async_queue.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Async is only possible after work() call. Processor {}",
                            async_queue.front()->processor->getName());
    }
}


bool ExecutingGraph::updateNode(uint64_t pid, Queue & queue, Queue & async_queue)
{
    std::stack<Edge *> updated_edges;
    std::stack<uint64_t> updated_processors;
    updated_processors.push(pid);

    UpgradableMutex::ReadGuard read_lock(nodes_mutex);

    while (!updated_processors.empty() || !updated_edges.empty())
    {
        std::optional<std::unique_lock<std::mutex>> stack_top_lock;

        if (updated_processors.empty())
        {
            auto * edge = updated_edges.top();
            updated_edges.pop();

            /// Here we have ownership on edge, but node can be concurrently accessed.

            auto & node = *nodes[edge->to];

            std::unique_lock lock(node.status_mutex);

            ExecutingGraph::ExecStatus status = node.status;

            if (status != ExecutingGraph::ExecStatus::Finished)
            {
                if (edge->backward)
                    node.updated_output_ports.push_back(edge->output_port_number);
                else
                    node.updated_input_ports.push_back(edge->input_port_number);

                if (status == ExecutingGraph::ExecStatus::Idle)
                {
                    node.status = ExecutingGraph::ExecStatus::Preparing;
                    updated_processors.push(edge->to);
                    stack_top_lock = std::move(lock);
                }
                else
                    nodes[edge->to]->processor->onUpdatePorts();
            }
        }

        if (!updated_processors.empty())
        {
            pid = updated_processors.top();
            updated_processors.pop();

            /// In this method we have ownership on node.
            auto & node = *nodes[pid];

            bool need_expand_pipeline = false;

            if (!stack_top_lock)
                stack_top_lock.emplace(node.status_mutex);

            {
#ifndef NDEBUG
                Stopwatch watch;
#endif

                std::unique_lock<std::mutex> lock(std::move(*stack_top_lock));

                try
                {
                    auto & processor = *node.processor;
                    IProcessor::Status last_status = node.last_processor_status;
                    IProcessor::Status status = processor.prepare(node.updated_input_ports, node.updated_output_ports);
                    node.last_processor_status = status;

                    if (profile_processors)
                    {
                        /// NeedData
                        if (last_status != IProcessor::Status::NeedData && status == IProcessor::Status::NeedData)
                        {
                            processor.input_wait_watch.restart();
                        }
                        else if (last_status == IProcessor::Status::NeedData && status != IProcessor::Status::NeedData)
                        {
                            processor.input_wait_elapsed_us += processor.input_wait_watch.elapsedMicroseconds();
                        }

                        /// PortFull
                        if (last_status != IProcessor::Status::PortFull && status == IProcessor::Status::PortFull)
                        {
                            processor.output_wait_watch.restart();
                        }
                        else if (last_status == IProcessor::Status::PortFull && status != IProcessor::Status::PortFull)
                        {
                            processor.output_wait_elapsed_us += processor.output_wait_watch.elapsedMicroseconds();
                        }
                    }
                }
                catch (...)
                {
                    node.exception = std::current_exception();
                    return false;
                }

#ifndef NDEBUG
                node.preparation_time_ns += watch.elapsed();
#endif

                node.updated_input_ports.clear();
                node.updated_output_ports.clear();

                switch (node.last_processor_status)
                {
                    case IProcessor::Status::NeedData:
                    case IProcessor::Status::PortFull:
                    {
                        node.status = ExecutingGraph::ExecStatus::Idle;
                        break;
                    }
                    case IProcessor::Status::Finished:
                    {
                        node.status = ExecutingGraph::ExecStatus::Finished;
                        break;
                    }
                    case IProcessor::Status::Ready:
                    {
                        node.status = ExecutingGraph::ExecStatus::Executing;
                        queue.push(&node);
                        break;
                    }
                    case IProcessor::Status::Async:
                    {
                        node.status = ExecutingGraph::ExecStatus::Executing;
                        async_queue.push(&node);
                        break;
                    }
                    case IProcessor::Status::ExpandPipeline:
                    {
                        need_expand_pipeline = true;
                        break;
                    }
                }

                if (!need_expand_pipeline)
                {
                    /// If you wonder why edges are pushed in reverse order,
                    /// it is because updated_edges is a stack, and we prefer to get from stack
                    /// input ports firstly, and then outputs, both in-order.
                    ///
                    /// Actually, there should be no difference in which order we process edges.
                    /// However, some tests are sensitive to it (e.g. something like SELECT 1 UNION ALL 2).
                    /// Let's not break this behaviour so far.

                    for (auto it = node.post_updated_output_ports.rbegin(); it != node.post_updated_output_ports.rend(); ++it)
                    {
                        auto * edge = static_cast<ExecutingGraph::Edge *>(*it);
                        updated_edges.push(edge);
                        edge->update_info.trigger();
                    }

                    for (auto it = node.post_updated_input_ports.rbegin(); it != node.post_updated_input_ports.rend(); ++it)
                    {
                        auto * edge = static_cast<ExecutingGraph::Edge *>(*it);
                        updated_edges.push(edge);
                        edge->update_info.trigger();
                    }

                    node.post_updated_input_ports.clear();
                    node.post_updated_output_ports.clear();
                }
            }

            if (need_expand_pipeline)
            {
                {
                    UpgradableMutex::WriteGuard lock(read_lock);
                    if (!expandPipeline(updated_processors, pid))
                        return false;
                }

                /// Add itself back to be prepared again.
                updated_processors.push(pid);
            }
        }
    }

    return true;
}

void ExecutingGraph::cancel()
{
    std::lock_guard guard(processors_mutex);
    for (auto & processor : processors)
        processor->cancel();
}

/// proton: starts.
void ExecutingGraph::serialize(WriteBuffer & wb) const
{
    /// Graph layout
    /// [num_processors][processor][processor]...
    UInt16 num_processors_to_serde = processors_indices_to_serde.size();
    DB::writeIntBinary(num_processors_to_serde, wb);
    for (auto index : processors_indices_to_serde)
        processors[index]->marshal(wb);
}

void ExecutingGraph::deserialize(ReadBuffer & rb) const
{
    /// Graph layout
    /// [num_processors][processor][processor]...
    UInt16 num_processors_to_serde = 0;
    DB::readIntBinary(num_processors_to_serde, rb);

    if (num_processors_to_serde != processors_indices_to_serde.size())
        throw Exception(
            ErrorCodes::RECOVER_CHECKPOINT_FAILED,
            "Checkpointed number of streaming processors doesn't match: checkpointed={}, current={}",
            num_processors_to_serde,
            processors_indices_to_serde.size());

    Processors recovered_processors;
    recovered_processors.reserve(num_processors_to_serde);
    for (UInt16 i = 0; i < num_processors_to_serde; ++i)
    {
        auto processor = std::make_shared<PlaceholdProcessor>();
        processor->setName(processor->unmarshal(rb));
        recovered_processors.push_back(std::move(processor));
    }

    /// Validate the recovered processors and new-planed processors
    for (size_t i = 0; const auto & recovered_processor : recovered_processors)
    {
        auto new_planned_processor = processors[processors_indices_to_serde[i]];
        if (recovered_processor->getID() != new_planned_processor->getID())
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Recovered streaming processor logic_id={} is not the same type as the new planned processor: checkpointed={}, current={}",
                recovered_processor->getLogicID(),
                recovered_processor->getName(),
                new_planned_processor->getName());

        auto compare_ports = [&](const auto & recovered_ports, const auto & new_ports) {
            if (recovered_ports.size() != new_ports.size())
                throw Exception(
                    ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                    "Recovered streaming processor logic_id={} name={} doesn't have same number of inputs as the new planned processor: checkpointed={}, "
                    "current={} current_name={}",
                    recovered_processor->getLogicID(),
                    recovered_processor->getName(),
                    recovered_ports.size(),
                    new_ports.size(),
                    new_planned_processor->getName());

            auto recovered_ports_iter = recovered_ports.begin();
            auto new_ports_iter = new_ports.begin();

            for (; recovered_ports_iter != recovered_ports.end();)
            {
                /// Use `isCompatibleHeaderWithoutComparingColumnNames` instead of `blocksHaveEqualStructure`,
                /// After deserialization from disk, the header name may be different than the current one in-memory
                /// (for instance constant column names) for query state checkpoint. So skip column name comparision
                /// when validting the head structure
                if (!isCompatibleHeaderWithoutComparingColumnNames(new_ports_iter->getHeader(), recovered_ports_iter->getHeader()))
                    throw Exception(
                        ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                        "Recovered streaming processor logic_id={} name={} doesn't have same input structure as the new planned processor. expected "
                        "structure: \"{}\", but recovered structure: \"{}\"",
                        recovered_processor->getLogicID(),
                        recovered_processor->getName(),
                        new_ports_iter->getHeader().dumpStructure(),
                        recovered_ports_iter->getHeader().dumpStructure());

                ++recovered_ports_iter;
                ++new_ports_iter;
            }
        };

        /// Compare inputs
        compare_ports(recovered_processor->getInputs(), new_planned_processor->getInputs());
        /// Compare outputs
        compare_ports(recovered_processor->getOutputs(), new_planned_processor->getOutputs());

        ++i;
    }

    /// FIXME, check the graph shape
}

void ExecutingGraph::recover(CheckpointContextPtr ckpt_ctx)
{
    for (auto & processor : processors)
        processor->recover(ckpt_ctx);
}

void ExecutingGraph::initCheckpointNodes()
{
    if (!checkpoint_ack_nodes.empty())
        return;

    /// NOTE: If there are some historical source, the number of processors is volatile for each recovering,
    /// since there are new parts of MergeTree or parts merging of MergeTree.
    /// So far, we only support following streaming queries:
    /// 1) fillback from historical data (With ConcatProcessor)
    /// 2) stream join table (With JoiningTransform)
    /// 3) join with versioned_kv (With Streaming::JoinTransform + ConcatProcessor)
    ///
    /// A case: checkpointed when there is no historical data, but recovering when there is historical data, which
    /// will use ConcatProcessor to link historical and streaming data.
    /// So we only serialize/deserialize streaming processors (Except ConcatProcessor).
    processors_indices_to_serde.clear();
    processors_indices_to_serde.reserve(processors.size());
    for (UInt16 i = 0; auto & node : nodes)
    {
        if (node->processor->isStreaming() && node->processor->getID() != ProcessorID::ConcatProcessorID)
        {
            node->processor->setLogicID(static_cast<UInt32>(processors_indices_to_serde.size()));
            processors_indices_to_serde.emplace_back(i);
        }

        if (node->back_edges.empty())
        {
            assert(node->processor->isSource());
            if (node->processor->isStreaming())
                checkpoint_trigger_nodes.push_back(node.get());
        }

        if (node->direct_edges.empty())
        {
            checkpoint_ack_nodes.push_back(node.get());
            assert(node->processor->isSink());
        }

        ++i;
    }

    assert(!checkpoint_trigger_nodes.empty() && !checkpoint_ack_nodes.empty());
}

bool ExecutingGraph::hasProcessedToCheckpoint() const
{
    for (auto * node : checkpoint_trigger_nodes)
    {
        const auto * streaming_source = dynamic_cast<const Streaming::ISource *>(node->processor);
        if (streaming_source->lastProcessedSN() > streaming_source->lastCheckpointedSN())
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

        metrices.set("processing_time_ns", metric.processing_time_ns);
        metrices.set("processed_bytes", metric.processed_bytes);

        n.set("metric", metrices);
        node_list.add(n);
    }
    status.set("nodes", node_list);
    status.set("edges", edges);

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    status.stringify(oss);
    return oss.str();
}
/// proton: ends.

}
