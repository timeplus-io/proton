#include <Interpreters/CollectCreateTelemetry.h>
#include <Interpreters/TelemetryCollector.h>
#include <Interpreters/TelemetryElement.h>
#include <Storages/ExternalStream/Kafka/Kafka.h>
#include <Storages/ExternalStream/Pulsar/Pulsar.h>
#include <Storages/ExternalStream/StorageExternalStream.h>
#include <Storages/ExternalStream/Timeplus/Timeplus.h>
#include <Storages/ExternalTable/StorageExternalTable.h>
#include <Storages/Streaming/StorageMaterializedView.h>
#include <Storages/Streaming/StorageRandom.h>
#include <Storages/Streaming/StorageStream.h>

namespace DB
{
void collectCreateTelemetry(const StoragePtr & storage, ContextPtr context)
{
    auto & collector = TelemetryCollector::instance(context);

    if (auto * stream = storage->as<StorageStream>())
    {
        collector.add<CreateStreamTelemetryElementBuilder>([&](auto & builder) {
            builder.withShards(stream->getShards())
                .withRepliactionFactor(stream->getReplicationFactor())
                .withStorageType(stream->getStorageType())
                .withMode(stream->getEngineMode());
        });
    }
    else if (auto * external_stream = storage->as<StorageExternalStream>())
    {
        collector.add<CreateExternalStreamTelemetryElementBuilder>([&](auto & builder) {
            builder.withType(external_stream->getType());

            auto nested_storage = external_stream->getNested();

            if (auto * kafka = nested_storage->as<Kafka>())
            {
                builder.withSecurityProtocol(kafka->secureProtocol())
                    .withSaslMechanism(kafka->saslMechanism())
                    .withDataFormat(kafka->dataFormat())
                    .withKafkaSchemaRegistryUrl(kafka->hasSchemaRegistryUrl())
                    .skipSslCertCheck(kafka->skipSslCertCheck())
                    .hasSslCaCertFile(kafka->hasSslCaCertFile())
                    .hasSslCaPem(kafka->hasSslCaPem());
            }
            else if (auto * timeplusd = nested_storage->as<ExternalStream::Timeplus>())
            {
                builder.isSecure(timeplusd->isSecure());
            }
#if USE_PULSAR
            else if (auto * plusar = nested_storage->as<ExternalStream::Pulsar>())
            {
                builder.withDataFormat(plusar->dataFormat())
                    .withFormatSchema(plusar->formatSchema())
                    .isOneMessagePerRow(plusar->isOneMessagePerRow())
                    .skipServerCertCheck(plusar->skipServerCertCheck())
                    .validateHostname(plusar->validateHostname());
            }
#endif
        });
    }
    else if (auto * external_table = storage->as<StorageExternalTable>())
    {
        collector.add<CreateExternalTableTelemetryElementBuilder>([&](auto & builder) { builder.withType(external_table->getType()); });
    }
    else if (auto materialized_view = storage->as<StorageMaterializedView>())
    {
        collector.add<CreateMaterializedViewTelemetryElementBuilder>([&](auto & builder) {
            builder.isExternalTarget(!materialized_view->getExternalTargetTableID().empty());

            if (auto target = materialized_view->tryGetTargetTable())
                builder.withTargetStorageType(target->getName());
            else
                builder.withTargetStorageType("Stream");
        });
    }
    else if (auto random_stream = storage->as<StorageRandom>())
    {
        collector.add<CreateRandomStreamTelelemtryElementBuilder>(
            [&](auto & builder) { builder.withEventsPerSecond(random_stream->getEventsPerSecond()); });
    }
}
}
