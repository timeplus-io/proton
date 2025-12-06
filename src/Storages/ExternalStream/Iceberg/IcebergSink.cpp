#include <Storages/ExternalStream/Iceberg/IcebergSink.h>

#if USE_AWS_S3 && USE_AVRO && USE_PARQUET

#include <Core/UUID.h>
#include <Formats/Avro/DataFileWriter.h>
#include <Formats/Avro/OutputStreamWriteBufferAdapter.h>
#include <Formats/FormatFactory.h>
#include <IO/IOThreadPool.h>
#include <IO/S3/getObjectInfo.h>
#include <IO/WriteBufferFromS3.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/Impl/ParquetBlockOutputFormat.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/ExternalStream/Iceberg/IcebergS3Configuration.h>
#include <Storages/Iceberg/AvroSchemas.h>
#include <Storages/Iceberg/Manifest.h>
#include <Storages/Iceberg/ManifestList.h>
#include <Storages/Iceberg/Requirement.h>
#include <Storages/Iceberg/Update.h>
#include <Common/CurrentMetrics.h>
#include <Common/logger_useful.h>

#include <parquet/arrow/writer.h>
#include <parquet/metadata.h>
#include <Compiler.hh>
#include <ValidSchema.hh>

namespace DB
{

namespace
{
/// Adds current timestamp to the base_key (before the file extension if it has one).
String genKey(const String & prefix)
{
    static size_t count = 0;

    /// 00000-0-ad7b1fd7-6834-4ef0-956e-b8a6dc7f07e1-0-00001.parquet
    auto result = std::filesystem::path(prefix) / "data"
        / fmt::format("{:05}-{}-{}-0-00001.parquet", /*partition_id=*/0, count, toString(UUIDHelpers::generateV4()));

    ++count;

    return result;
}

}

namespace ExternalStream
{

IcebergSink::IcebergSink(
    const StorageID & storage_id_,
    const String & format_,
    const Block & sample_block_,
    std::optional<FormatSettings> format_settings_,
    const IcebergS3Configuration & s3_configuration_,
    UInt64 min_upload_file_size_,
    UInt64 max_upload_idle_seconds_,
    Apache::Iceberg::TableMetadata metadata_,
    std::list<Apache::Iceberg::ManifestList> current_manifest_lists,
    const Apache::Iceberg::CatalogPtr & catalog_,
    ContextPtr context_)
    : SinkToStorage(sample_block_, ProcessorID::IcebergSinkID)
    , storage_id(storage_id_)
    , format(format_)
    , sample_block(sample_block_)
    , catalog(catalog_)
    , metadata(std::move(metadata_))
    , manifest_lists(std::move(current_manifest_lists))
    , s3_configuration(s3_configuration_)
    , bucket(s3_configuration.url.bucket)
    , format_settings(format_settings_)
    , min_upload_file_size(min_upload_file_size_)
    , max_upload_idle_seconds(max_upload_idle_seconds_)
    , context(context_)
    , logger(getLogger("IcebergSink"))
{
    /// The timer should only start when data start flowing.
    upload_idle_timer.stop();
}

IcebergSink::~IcebergSink()
{
    try
    {
        onFinish();
    }
    catch (...)
    {
        stopped = true;
        tryLogCurrentException(logger, "Failed to finish");
    }
}

void IcebergSink::consume(Chunk chunk)
{
    if (!chunk.hasRows())
    {
        if (max_upload_idle_seconds > 0 && upload_idle_timer.elapsedSeconds() >= max_upload_idle_seconds)
        {
            std::lock_guard lock(cancel_mutex);
            finalize();
        }
        return;
    }

    std::lock_guard lock(cancel_mutex);
    if (cancelled)
        return;

    if (writer && write_buf->count() >= min_upload_file_size && write_buf->count() > 0)
        /// Properly finalize the format writer and complete the current upload.
        finalize();

    if (!writer)
    {
        std::string object_key;
        do
        {
            current_data_file_uri = genKey(metadata.getLocation(false));
            object_key = Poco::URI(current_data_file_uri).getPath();
        } while (DB::S3::objectExists(*s3_configuration.client, bucket, object_key, s3_configuration.url.version_id));

        BlobStorageLogWriterPtr blob_log = nullptr;
        if (auto blob_storage_log = context->getBlobStorageLog())
        {
            blob_log = std::make_shared<BlobStorageLogWriter>(std::move(blob_storage_log));
            blob_log->query_id = context->getCurrentQueryId();
        }

        write_buf = std::make_unique<WriteBufferFromS3>(
            s3_configuration.client,
            bucket,
            object_key,
            DBMS_DEFAULT_BUFFER_SIZE,
            s3_configuration.request_settings,
            std::move(blob_log),
            std::nullopt,
            threadPoolCallbackRunner<void>(IOThreadPool::get(), "S3ParallelWrite"),
            context->getWriteSettings());
        writer = FormatFactory::instance().getOutputFormatParallelIfPossible(format, *write_buf, sample_block, context, format_settings);
        writer->setAutoFlush();
    }

    current_record_count += chunk.getNumRows();
    writer->write(getHeader().cloneWithColumns(chunk.detachColumns()));

    /// Restart the timer when there are new data, because we are calculating the idle time.
    upload_idle_timer.start();
}

void IcebergSink::onCancel() noexcept
{
    std::lock_guard lock(cancel_mutex);

    try
    {
        finalize();
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Failed to finalize");
    }

    cancelled = true;
    stopped = true;
}

void IcebergSink::onException(std::exception_ptr exception)
{
    std::lock_guard lock(cancel_mutex);
    try
    {
        std::rethrow_exception(exception);
    }
    catch (...)
    {
        /// An exception context is needed to proper delete write buffers without finalization
        release();
    }
}

void IcebergSink::onFinish()
{
    std::lock_guard lock(cancel_mutex);
    finalize();
    stopped = true;
}

void IcebergSink::finalize()
{
    if (!writer)
        return;

    /// Stop ParallelFormattingOutputFormat correctly.
    SCOPE_EXIT(writer.reset());

    writer->finalize();
    writer->flush();
    write_buf->finalize();

    commit();

    current_record_count = 0;
    upload_idle_timer.reset();
}

void IcebergSink::release()
{
    writer.reset();
    write_buf.reset();
}

namespace
{
int64_t genSnapshotID()
{
    auto uuid = UUIDHelpers::generateV4();
    const auto & high_bits = UUIDHelpers::getHighBytes(uuid);
    const auto & low_bits = UUIDHelpers::getLowBytes(uuid);
    return (high_bits ^ low_bits) & LLONG_MAX;
}

auto genManifestListPath(const Apache::Iceberg::TableMetadata & metadata, int64_t snapshot_id, size_t attempts, const UUID & commit_uuid)
{
    ///  Mimics the behavior in Java:
    ///  https://github.com/apache/iceberg/blob/c862b9177af8e2d83122220764a056f3b96fd00c/core/src/main/java/org/apache/iceberg/SnapshotProducer.java#L491
    return fmt::format("{}/metadata/snap-{}-{}-{}.avro", metadata.getLocation(false), snapshot_id, attempts, toString(commit_uuid));
}

auto genManifestPath(const Apache::Iceberg::TableMetadata & metadata, const UUID & commit_uuid, size_t num)
{
    return fmt::format("{}/metadata/{}-m{}.avro", metadata.getLocation(false), toString(commit_uuid), num);
}
}

void IcebergSink::commit()
{
    /// TODO make sure that snapshot_id is not being used.
    auto snapshot_id = genSnapshotID();
    auto commit_uuid = UUIDHelpers::generateV4();
    auto sequence_number = metadata.getSequenceNumber() + 1;
    size_t attempts = 0;

    /// The manifest files and manifest list must be generated before calling commitTable on the catalog.
    /// Otherwise, if it fails to write those files after it calls commitTable, the table will be in a bad state.
    generateManifest(commit_uuid);
    writeManifest();

    /// TODO if commitTable fails later due to conflict (409), we should re-generate the manifest list (increase attempts).
    generateManifestList(commit_uuid, snapshot_id, sequence_number, attempts);
    writeManifestList();

    const auto & current_manifest_list = manifest_lists.front();

    auto total_data_files = current_manifest_list.added_files_count;
    auto total_delete_files = current_manifest_list.deleted_files_count;
    auto total_records = current_manifest_list.added_rows_count;
    auto total_files_size = current_manifest.data_file.file_size_in_bytes;
    auto total_position_deletes = 0;
    auto total_equality_deletes = 0;

    const auto & current_snapshot = metadata.getCurrentSnapshot();

    if (current_snapshot.isValid())
    {
        total_data_files += std::stol(current_snapshot.summary.total_data_files);
        total_delete_files += std::stol(current_snapshot.summary.total_delete_files);
        total_records += std::stoll(current_snapshot.summary.total_records);
        total_files_size += std::stoll(current_snapshot.summary.total_files_size);
        total_position_deletes += std::stoll(current_snapshot.summary.total_position_deletes);
        total_equality_deletes += std::stoll(current_snapshot.summary.total_equality_deletes);
    }

    Apache::Iceberg::Snapshot snapshot{
        .snapshot_id = snapshot_id,
        .parent_snapshot_id = metadata.getSnapshotID() < 0 ? std::nullopt : std::optional<uint64_t>(metadata.getSnapshotID()),
        .sequence_number = sequence_number,
        .timestamp_ms = UTCMilliseconds::now(),
        .manifest_list = current_manifest_list_uri,
        .summary = {
            /// TODO make this a enum
            .operation = "append",
            .added_files_size = fmt::format("{}", current_manifest.data_file.file_size_in_bytes),
            .added_data_files = "1",
            .added_records = fmt::format("{}", current_manifest_list.added_rows_count),
            /// FIXME all total_ fields
            .total_data_files = std::to_string(total_data_files),
            .total_delete_files = std::to_string(total_delete_files),
            .total_records = std::to_string(total_records),
            .total_files_size = std::to_string(total_files_size),
            .total_position_deletes = std::to_string(total_position_deletes),
            .total_equality_deletes = std::to_string(total_equality_deletes),
        },
        .schema_id = metadata.getCurrentSchemaID(),
    };

    Apache::Iceberg::Updates updates;
    updates.reserve(2);
    updates.push_back(std::make_unique<Apache::Iceberg::AddSnopshotUpdate>(snapshot));
    updates.push_back(std::make_unique<Apache::Iceberg::SetSnapshotRefUpdate>(Apache::Iceberg::SnapshotReference{
        .name = "main",
        .type = "branch",
        .snapshot_id = snapshot_id,
    }));

    Apache::Iceberg::Requirements requirements;
    requirements.reserve(2);
    requirements.push_back(std::make_unique<Apache::Iceberg::AssertRefSnapshotID>("main", metadata.getSnapshotID()));
    requirements.push_back(std::make_unique<Apache::Iceberg::AssertTableUUID>(metadata.getTableUUID()));

    /// TODO
    /// Retry on failure. When retry, should re-generate the manifest list path (increase attempts).
    catalog->commitTable(storage_id.getDatabaseName(), storage_id.getTableName(), requirements, updates, metadata);
}

void IcebergSink::writeManifestList() const
{
    const auto & current_manifest_list = manifest_lists.back();

    BlobStorageLogWriterPtr blob_log = nullptr;
    if (auto blob_storage_log = context->getBlobStorageLog())
    {
        blob_log = std::make_shared<BlobStorageLogWriter>(std::move(blob_storage_log));
        blob_log->query_id = context->getCurrentQueryId();
    }

    WriteBufferFromS3 buf{
        s3_configuration.client,
        bucket,
        Poco::URI(current_manifest_list_uri).getPath(),
        DBMS_DEFAULT_BUFFER_SIZE,
        s3_configuration.request_settings,
        std::move(blob_log),
        /*object_metadata_=*/std::nullopt,
        threadPoolCallbackRunner<void>(IOThreadPool::get(), "S3ParallelRead"),
        context->getWriteSettings()};

    std::unordered_map<String, String> file_metadata;
    file_metadata.reserve(5);
    file_metadata["format-version"] = fmt::format("{}", metadata.getFormatVersion());
    file_metadata["snapshot-id"] = fmt::format("{}", current_manifest_list.added_snapshot_id);
    file_metadata["parent-snapshot-id"] = "null";
    file_metadata["sequence-number"] = fmt::format("{}", current_manifest_list.sequence_number);
    file_metadata["iceberg.schema"] = Apache::Iceberg::AvroSchemas::MANIFEST_LIST_ICEBERG;

    Avro::DataFileWriterBase file_writer{
        std::make_unique<Avro::OutputStreamWriteBufferAdapter>(buf),
        Apache::Iceberg::AvroSchemas::MANIFEST_LIST,
        file_metadata,
        /*sync_interval=*/16 * 1024, /// FIXME
        avro::Codec::DEFLATE_CODEC};

    file_writer.syncIfNeeded();
    for (const auto & manifest_list : manifest_lists)
    {
        avro::encode(file_writer.encoder(), manifest_list);
        file_writer.incr();
    }
    file_writer.flush();
    file_writer.close();
    buf.finalize();
}

void IcebergSink::generateManifestList(const UUID & commit_uuid, int64_t snapshot_id, uint64_t sequence_number, size_t attempts)
{
    current_manifest_list_uri = genManifestListPath(metadata, snapshot_id, attempts, commit_uuid);
    LOG_INFO(logger, "Generated manifest list path: {}", current_manifest_list_uri);

    Apache::Iceberg::ManifestList manifest_list;
    manifest_list.manifest_path = current_manifest_uri;
    manifest_list.manifest_length = write_buf->count(); /// FIXME
    manifest_list.sequence_number = sequence_number;
    manifest_list.min_sequence_number = sequence_number;
    manifest_list.added_snapshot_id = snapshot_id;
    manifest_list.added_files_count = 1;
    manifest_list.added_rows_count = current_manifest.data_file.record_count;
    manifest_list.existing_files_count = 0;
    manifest_list.deleted_files_count = 0;
    manifest_list.existing_rows_count = 0;
    manifest_list.deleted_rows_count = 0;

    Apache::Iceberg::partitions partitions;
    partitions.set_array({});
    manifest_list.partitions = std::move(partitions);
    // manifest.key_metadata = nullptr;

    manifest_lists.push_front(std::move(manifest_list));
}

void IcebergSink::writeManifest() const
{
    BlobStorageLogWriterPtr blob_log = nullptr;
    if (auto blob_storage_log = context->getBlobStorageLog())
    {
        blob_log = std::make_shared<BlobStorageLogWriter>(std::move(blob_storage_log));
        blob_log->query_id = context->getCurrentQueryId();
    }

    WriteBufferFromS3 buf{
        s3_configuration.client,
        bucket,
        Poco::URI(current_manifest_uri).getPath(),
        DBMS_DEFAULT_BUFFER_SIZE,
        s3_configuration.request_settings,
        std::move(blob_log),
        /*object_metadata_=*/std::nullopt,
        threadPoolCallbackRunner<void>(IOThreadPool::get(), "S3ParallelRead"),
        context->getWriteSettings()};

    std::unordered_map<String, String> file_metadata;
    assert(!metadata.getSchemaJSON().empty());
    file_metadata["schema"] = metadata.getSchemaJSON();
    file_metadata["format-version"] = fmt::format("{}", metadata.getFormatVersion());
    file_metadata["partition-spec-id"] = "0";
    file_metadata["partition-spec"] = "[]";
    file_metadata["content"] = "data";
    file_metadata["iceberg.schema"] = Apache::Iceberg::AvroSchemas::MANIFEST_ICEBERG;

    Avro::DataFileWriterBase file_writer{
        std::make_unique<Avro::OutputStreamWriteBufferAdapter>(buf),
        Apache::Iceberg::AvroSchemas::MANIFEST,
        file_metadata,
        /*sync_interval=*/16 * 1024, /// FIXME?
        avro::Codec::DEFLATE_CODEC};

    file_writer.syncIfNeeded();
    avro::encode(file_writer.encoder(), current_manifest);
    file_writer.incr();
    file_writer.flush();
    file_writer.close();
    buf.finalize();
}

void IcebergSink::generateManifest(const UUID & commit_uuid)
{
    current_manifest_uri = genManifestPath(metadata, commit_uuid, 0);
    LOG_INFO(logger, "Generated manifest key: {}", current_manifest_uri);

    Apache::Iceberg::Manifest manifest;
    /// 0: EXISTING 1: ADDED 2: DELETED
    manifest.status = 1;
    /// manifest.snapshot_id = {}; /// optional, inherited from manifest list
    /// manifest.sequence_number = {}; /// optional, should set to null for add new files
    /// manifest.file_sequence_number = {}; /// optional, should set to null for add new files

    auto parquet_output_format = std::dynamic_pointer_cast<ParquetBlockOutputFormat>(writer);
    if (!parquet_output_format)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected writer should be a ParquetBlockOutputFormat");

    /// All other fields not assigned below in data_file are optional.
    /// TODO set all the optional fields for better query planning.
    manifest.data_file.file_path = current_data_file_uri;
    /// avro, orc, parquet, or puffin
    manifest.data_file.file_format = "PARQUET";
    manifest.data_file.partition = {};
    manifest.data_file.record_count = current_record_count;
    manifest.data_file.file_size_in_bytes = write_buf->count();
    /// 0: DATA, 1: POSITION DELETES, 2: EQUALITY DELETES
    manifest.data_file.content = Apache::Iceberg::DataFileContent::Data;

    current_manifest = std::move(manifest);
}


}
}
#endif
