#include <Storages/ExternalStream/Pulsar/Pulsar.h>
#include <Storages/ExternalStream/Pulsar/PulsarSource.h>

namespace DB{

PulsarSource::PulsarSource(
    Pulsar * pulsar_,
    Block header_,
    ContextPtr query_context_,
    size_t max_block_size_,
    Poco::Logger * log_)
    : ISource(header_, true, ProcessorID::FileLogSourceID)
    , pulsar(pulsar_)
    , query_context(query_context_)
    , max_block_size(max_block_size_)
    , log(log_)
{
    is_streaming = true;
}

PulsarSource::~PulsarSource() = default;

Chunk PulsarSource::generate()
{
    auto col = column_type->createColumn();
    col->insertData("kartik", 6);
    return Chunk(Columns(1, std::move(col)), 1);
}
}
