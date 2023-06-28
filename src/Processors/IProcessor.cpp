#include <iostream>
#include <Processors/IProcessor.h>

/// proton : starts
#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/VersionRevision.h>
/// proton : ends


namespace DB
{

void IProcessor::dump() const
{
    std::cerr << getName() << "\n";

    std::cerr << "inputs:\n";
    for (const auto & port : inputs)
        std::cerr << "\t" << port.hasData() << " " << port.isFinished() << "\n";

    std::cerr << "outputs:\n";
    for (const auto & port : outputs)
        std::cerr << "\t" << port.hasData() << " " << port.isNeeded() << "\n";
}


std::string IProcessor::statusToName(Status status)
{
    switch (status)
    {
        case Status::NeedData:
            return "NeedData";
        case Status::PortFull:
            return "PortFull";
        case Status::Finished:
            return "Finished";
        case Status::Ready:
            return "Ready";
        case Status::Async:
            return "Async";
        case Status::ExpandPipeline:
            return "ExpandPipeline";
    }

    __builtin_unreachable();
}

/// proton : starts
/// Layout :
/// [pid][version][is_streaming][stream_num][query_plan_step_group][name][processor_description]
/// [input_port_size][input_port_addresses][input_ports]
/// [output_port_size][output_port_address][output_ports]
void IProcessor::marshal(WriteBuffer & wb) const
{
    assert(pid != ProcessorID::InvalidID);
    writeIntBinary(static_cast<UInt32>(pid), wb);

    writeIntBinary(getVersion(), wb);
    writeIntBinary(is_streaming, wb);
    writeIntBinary(stream_number, wb);
    writeIntBinary(query_plan_step_group, wb);
    writeStringBinary(getName(), wb);
    writeStringBinary(processor_description, wb);

    auto serialize = [&](const auto & ports) {
        UInt16 port_size = ports.size();
        writeIntBinary(port_size, wb);

        for (const auto & port : ports)
            port.marshal(wb);
    };

    serialize(inputs);
    serialize(outputs);
}

String IProcessor::unmarshal(ReadBuffer & rb)
{
    assert(inputs.empty());
    assert(outputs.empty());

    UInt32 pid_i = 0;
    readIntBinary(pid_i, rb);
    pid = ProcessorID(pid_i);
    assert(pid != ProcessorID::InvalidID);

    version = 0;
    readIntBinary(*version, rb);
    readIntBinary(is_streaming, rb);
    readIntBinary(stream_number, rb);
    readIntBinary(query_plan_step_group, rb);

    String name;
    readStringBinary(name, rb);
    readStringBinary(processor_description, rb);

    auto deserialize = [&](auto & ports) {
        UInt16 port_size = 0;
        readIntBinary(port_size, rb);

        for (size_t i = 0; i < port_size; ++i)
        {
            ports.emplace_back(Block{});
            ports.back().unmarshal(rb);
        }
    };

    deserialize(inputs);
    deserialize(outputs);

    return name;
}

void IProcessor::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    /// By default, we just notify checkpoint coordinator the processor has seen
    /// seen the checkpoint epic since most processor don't have `state` to checkpoint
    ckpt_ctx->coordinator->checkpointed(getVersion(), logic_pid, ckpt_ctx);
}


VersionType IProcessor::getVersionFromRevision(UInt64 revision) const
{
    if (version)
        return *version;

    return static_cast<VersionType>(revision);
}

VersionType IProcessor::getVersion() const
{
    auto ver = getVersionFromRevision(ProtonRevision::getVersionRevision());

    if (!version)
        version = ver;

    return ver;
}

/// proton : ends

}

