#include <Interpreters/Apply/MetadataUpdater.h>

#include <Cluster/MetaStore/MetaStore.h>

#include <Formats/FormatSchemaFactory.h>
#include <Formats/ProtobufSchemas.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int OK;
extern const int UNKNOWN_EXCEPTION;
extern const int AMBIGUOUS_FORMAT_SCHEMA;
extern const int FORMAT_SCHEMA_ALREADY_EXISTS;
extern const int UNKNOWN_FORMAT_SCHEMA;
extern const int UNKNOWN_FORMAT_SCHEMA_TYPE;
extern const int INVALID_DATA;
extern const int METADATA_VERSION_CHANGED;
}

void MetadataUpdater::handleCreateFormatSchema(
    const cluster::CreateFormatSchemaRequest & request,
    const cluster::RequestHeader & request_header,
    uint64_t sn) const
{
    const auto & request_data = request.data();
    const auto & desc = request_data.desc;

    LOG_INFO(logger, "Start CreateFormatSchema, schema_name={} format={}", desc.name, desc.format);
    Stopwatch stopwatch;
    SCOPE_EXIT({
        LOG_INFO(
            logger, "End CreateFormatSchema, schema_name={} format={}, took={}ms", desc.name, desc.format, stopwatch.elapsedMilliseconds());
    });

    uint32_t current_version{cluster::Nulls::NullVersion};
    uint32_t next_version{1};

    auto schema_desc = loadFormatSchemaDescriptor(desc.name, desc.format);
    if (schema_desc.hasError())
    {
        if (!schema_desc.err.isNotFound())
        {
            LOG_ERROR(
                logger,
                "Failed to load format schema description for creating format schema {} of type {}, error_code={} error={}",
                desc.name,
                desc.format,
                schema_desc.err.error_code,
                schema_desc.errorString());

            meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
            meta_store->ackProposal(
                request_header.correlationID(), sn, schema_desc.err.error_code, std::move(schema_desc.err.error_message));
            return;
        }
    }
    else
    {
        current_version = schema_desc.result->data_version;
    }

    if (current_version == cluster::Nulls::NullVersion)
    {
        if (desc.data_version != 1)
        {
            auto error = fmt::format(
                "Failed to create schema due to invalid data version: name={} format={} expected_version=1 actual_version={}",
                desc.name,
                desc.format,
                desc.data_version);

            LOG_ERROR(logger, "{}", error);

            meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
            meta_store->ackProposal(
                request_header.correlationID(), sn, ErrorCodes::METADATA_VERSION_CHANGED, std::move(error));

            return;
        }
    }
    else if (request_data.exists_op == cluster::protocol::ExistsOperation::Throw)
    {
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(
            request_header.correlationID(),
            sn,
            ErrorCodes::FORMAT_SCHEMA_ALREADY_EXISTS,
            fmt::format("Format schema {} of type already exists version={}", desc.name, desc.format, current_version));

        return;
    }
    else if (request_data.exists_op == cluster::protocol::ExistsOperation::Ignore)
    {
        meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");
        return;
    }
    else
    {
        if (current_version != desc.data_version)
        {
            auto error = fmt::format(
                "Failed to replace format schema {} of type {} due to version conflict: expected_version={} current_version={}",
                desc.name,
                desc.format,
                desc.data_version,
                current_version);

            LOG_ERROR(logger, "{}", error);

            meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
            meta_store->ackProposal(
                request_header.correlationID(), sn, ErrorCodes::METADATA_VERSION_CHANGED, std::move(error));

            return;
        }

        next_version = current_version + 1;
    }

    try
    {
        auto const_global_context = std::const_pointer_cast<const Context>(global_context);
        FormatSchemaFactory::instance().registerSchema(
            desc.name, desc.format, desc.schema_body, request_data.exists_op, const_global_context);
    }
    catch (const Exception & e)
    {
        LOG_ERROR(logger, "Failed to create format schema, schema_name={} format={}, error={}", desc.name, desc.format, e.message());
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(request_header.correlationID(), sn, e.code(), std::string{e.message()});
        return;
    }
    catch (...)
    {
        tryLogCurrentException(logger, fmt::format("Failed to create format schema schema_name={} format={}", desc.name, desc.format));
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::UNKNOWN_EXCEPTION, /*error_message=*/"");
        return;
    }

    auto updated_desc{desc};
    updated_desc.data_version = next_version;
    updated_desc.last_modify_timestamp_ms = request_data.requested_ts;

    if (updated_desc.data_version == 1)
    {
        updated_desc.create_timestamp_ms = request_data.requested_ts;
        updated_desc.created_by = request_data.requested_by;
    }
    else
    {
        updated_desc.create_timestamp_ms = schema_desc.result->create_timestamp_ms;
        updated_desc.created_by = schema_desc.result->created_by;
        updated_desc.last_modified_by = request_data.requested_by;
    }

    auto res = executeWithRetry([this, &updated_desc, &sn, &desc]() {
        auto err = meta_store->getMetaDB().saveFormatSchema(updated_desc, cluster::AppliedSequence(sn));
        if (err.hasError())
            LOG_ERROR(
                logger, "Failed to commit create format schema: schema_name={} format={} err_msg={}", desc.name, desc.format, err.string());
        return err.error_code;
    });

    if (res != DB::ErrorCodes::OK)
    {
        handleFailedRequest(
            request_header.correlationID(),
            sn,
            res,
            fmt::format("Failed to commit create format schema: schema_name={} format={}", desc.name, desc.format));
        return;
    }

    meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");
}

void MetadataUpdater::handleDeleteFormatSchema(
    const cluster::DeleteFormatSchemaRequest & request,
    const cluster::RequestHeader & request_header,
    uint64_t sn) const
{
    const auto & request_data = request.data();

    LOG_INFO(logger, "Start DeleteFormatSchema, schema_name={} format={}", request_data.schema_name, request_data.format);
    Stopwatch stopwatch;
    SCOPE_EXIT({
        LOG_INFO(
            logger,
            "End DeleteFormatSchema, schema_name={} format={}, took={}ms",
            request_data.schema_name,
            request_data.format,
            stopwatch.elapsedMilliseconds());
    });

    String schema_format;
    try
    {
        schema_format = FormatSchemaFactory::instance().unregisterSchema(
            request_data.schema_name, request_data.format, !request_data.if_exists, global_context);
    }
    catch (const Exception & e)
    {
        LOG_ERROR(
            logger,
            "Failed to delete format schema schema_name={} format={}, error={}",
            request_data.schema_name,
            request_data.format,
            e.message());

        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));

        meta_store->ackProposal(request_header.correlationID(), sn, e.code(), std::string{e.message()});
        return;
    }
    catch (...)
    {
        tryLogCurrentException(
            logger, fmt::format("Failed to delete format schema schema_name={} format={}", request_data.schema_name, request_data.format));

        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));

        meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::UNKNOWN_EXCEPTION, /*error_message=*/"");
        return;
    }

    auto res = executeWithRetry([this, &schema_format, &request_data, &sn]() {
        if (schema_format.empty())
            return DB::ErrorCodes::OK;

        auto err = meta_store->getMetaDB().deleteFormatSchema(request_data.schema_name, schema_format, cluster::AppliedSequence(sn));
        if (err.hasError())
            LOG_WARNING(
                logger,
                "Failed to commit delete format schema: schema_name={} format={} err_msg={}",
                request_data.schema_name,
                request_data.format,
                err.string());

        return err.error_code;
    });

    if (res != DB::ErrorCodes::OK)
    {
        handleFailedRequest(
            request_header.correlationID(),
            sn,
            res,
            fmt::format("Failed to commit delete format schema: name={} format={}", request_data.schema_name, request_data.format));
        return;
    }

    meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");
}

cluster::CallResultV<cluster::protocol::FormatSchemaDescriptorPtr>
MetadataUpdater::loadFormatSchemaDescriptor(const std::string & name, const std::string & format) const
{
    cluster::CallResultV<cluster::protocol::FormatSchemaDescriptorPtr> format_schema_desc;
    auto _ = executeWithRetry([this, &format_schema_desc, &name, &format]() {
        format_schema_desc = meta_store->getMetaDB().getFormatSchema(name, format);
        return format_schema_desc.err.error_code;
    });

    if (format_schema_desc.hasError())
        LOG_ERROR(
            logger,
            "Failed to load format schema description when updating format schema {} of type {}, error={}",
            name,
            format,
            format_schema_desc.err.error_message);

    return format_schema_desc;
}

void MetadataUpdater::handleDropFormatSchemaCache(
    const cluster::DropFormatSchemaCacheRequest & request,
    const cluster::RequestHeader & request_header,
    uint64_t sn) const
{
    const auto & data = request.data();

    LOG_INFO(logger, "Start DropFormatSchemaCache, {}", data.doString());
    Stopwatch stopwatch;
    SCOPE_EXIT({ LOG_INFO(logger, "End DropFormatSchemaCache, {} took={}ms", data.string(), stopwatch.elapsedMilliseconds()); });

#if USE_PROTOBUF
    try
    {
        if (data.caches_to_drop.contains("Protobuf"))
            ProtobufSchemas::instance().clear();
    }
    catch (...)
    {
        tryLogCurrentException(logger, fmt::format("Failed to drop format schema cache {}", data.doString()));
        meta_store->getMetaDB().saveAppliedSequence(cluster::AppliedSequence(sn));
        meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::UNKNOWN_EXCEPTION, /*error_message=*/"");
        return;
    }
#endif

    meta_store->ackProposal(request_header.correlationID(), sn, DB::ErrorCodes::OK, /*error_message=*/"");
}

}
