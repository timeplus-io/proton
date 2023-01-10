#include <Processors/Port.h>
#include <Processors/IProcessor.h>

/// proton : starts
#include <Formats/SimpleNativeReader.h>
#include <Formats/SimpleNativeWriter.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/VersionRevision.h>
/// proton : ends

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void connect(OutputPort & output, InputPort & input)
{
    if (input.state || output.state)
        throw Exception("Port is already connected", ErrorCodes::LOGICAL_ERROR);

    auto out_name = output.getProcessor().getName();
    auto in_name = input.getProcessor().getName();

    assertCompatibleHeader(output.getHeader(), input.getHeader(), " function connect between " + out_name + " and " + in_name);

    input.output_port = &output;
    output.input_port = &input;
    input.state = std::make_shared<Port::State>();
    output.state = input.state;
}

/// proton : starts
void Port::doMarshal(DB::WriteBuffer & wb) const
{
    /// Layout : [version][processor_addr][header]
    writeIntBinary(getVersion(), wb);
    writeIntBinary(reinterpret_cast<uintptr_t>(processor), wb);

    SimpleNativeWriter writer(wb, 0);
    writer.write(header);
    writer.flush();
}

void Port::doUnmarshal(DB::ReadBuffer & rb)
{
    version = 0;
    readIntBinary(*version, rb);

    uintptr_t processor_addr = 0;
    readIntBinary(processor_addr, rb);

    /// We will fix this later by finding the correct
    /// new processor addr
    processor = reinterpret_cast<IProcessor*>(processor_addr);

    SimpleNativeReader reader(rb, 0);
    header = reader.read();
}

void InputPort::marshal(DB::WriteBuffer & wb) const
{
    doMarshal(wb);
    writeIntBinary(reinterpret_cast<uintptr_t>(output_port), wb);
}

void InputPort::unmarshal(DB::ReadBuffer & rb)
{
    doUnmarshal(rb);

    uintptr_t output_port_addr = 0;
    readIntBinary(output_port_addr, rb);
    /// Please note that the unmarshalled output_port cannot be de-referenced,
    /// It is just used to figure the topology of the processor graph during recover.
    /// We will need fix its address later
    output_port = reinterpret_cast<OutputPort*>(output_port_addr);
}

void OutputPort::marshal(DB::WriteBuffer & wb) const
{
    doMarshal(wb);
    writeIntBinary(reinterpret_cast<uintptr_t>(input_port), wb);
}

void OutputPort::unmarshal(DB::ReadBuffer & rb)
{
    doUnmarshal(rb);
    uintptr_t input_port_addr = 0;
    readIntBinary(input_port_addr, rb);

    /// Please note that the unmarshalled output_port cannot be de-referenced,
    /// It is just used to figure the topology of the processor graph during recover.
    /// We will need fix its address later
    input_port = reinterpret_cast<InputPort*>(input_port_addr);
}

VersionType Port::getVersionFromRevision(UInt64 revision) const
{
    if (version)
        return *version;

    return static_cast<VersionType>(revision);
}

VersionType Port::getVersion() const
{
    auto ver = getVersionFromRevision(ProtonRevision::getVersionRevision());

    if (!version)
        version = ver;

    return ver;
}

/// proton : ends

}
