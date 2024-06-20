#pragma once

#include <Storages/IStorage.h>
#include <Common/SettingsChanges.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Storages/StorageProxy.h>

#include <base/shared_ptr_helper.h>

namespace DB
{
struct ExternalStreamSettings;

namespace ErrorCodes
{
extern const int INCOMPATIBLE_COLUMNS;
}

/// StorageExternalStream acts like a routing storage engine which proxy the requests to the underlying specific
/// external streaming storage like Kafka, Redpanda etc.
class StorageExternalStream final : public shared_ptr_helper<StorageExternalStream>, public StorageProxy, public WithContext
{
    friend struct shared_ptr_helper<StorageExternalStream>;

public:
    ExternalStreamCounterPtr getExternalStreamCounter() { return external_stream_counter; }

    StoragePtr getNested() const override { return external_stream; }

    String getName() const override { return "ExternalStream"; }

    bool supportsSubcolumns() const override { return external_stream->supportsSubcolumns(); }
    bool supportsStreamingQuery() const override { return external_stream->supportsStreamingQuery(); }
    bool supportsAccurateSeekTo() const noexcept override { return external_stream->supportsAccurateSeekTo(); }
    bool squashInsert() const noexcept override { return external_stream->squashInsert(); }

    friend class KafkaSource;

protected:
    StorageExternalStream(
        const ASTs & engine_args,
        const StorageID & table_id_,
        ContextPtr context_,
        const ColumnsDescription & columns_,
        std::unique_ptr<ExternalStreamSettings> external_stream_settings_,
        const String & comment,
        bool attach);

private:
    ExternalStreamCounterPtr external_stream_counter;
    StoragePtr external_stream;
};

}
