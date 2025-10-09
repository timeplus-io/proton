#include <Storages/ExternalStream/BatchConfiguration.h>

#include <Interpreters/Context.h>

namespace DB
{

namespace ExternalStream
{

void BatchConfiguration::loadFromContext(const ContextPtr & context)
{
    const auto & settings = context->getSettingsRef();
    bytes_threshold = settings.max_insert_block_size;
    count_threshold = settings.max_insert_block_bytes;
    timeout_ms = settings.insert_block_timeout_ms.value < 0 ? std::numeric_limits<UInt64>::max() : settings.insert_block_timeout_ms.value;
}

BatchConfiguration BatchConfiguration::copyAndUpdateFromContext(const ContextPtr & context) const
{
    auto new_config = *this;
    const auto & settings = context->getSettingsRef();

    if (settings.max_insert_block_size.changed)
        new_config.count_threshold = settings.max_insert_block_size;

    if (settings.max_insert_block_bytes.changed)
        new_config.bytes_threshold = settings.max_insert_block_bytes;

    if (auto timeout_ms_ = settings.insert_block_timeout_ms; timeout_ms_.changed)
        new_config.timeout_ms = timeout_ms_.value < 0 ? std::numeric_limits<UInt64>::max() : timeout_ms_;

    return new_config;
}

}

}
