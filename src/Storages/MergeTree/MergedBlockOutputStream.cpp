#include <memory>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <IO/HashingWriteBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Parsers/queryToString.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


MergedBlockOutputStream::MergedBlockOutputStream(
    const MergeTreeMutableDataPartPtr & data_part,
    const StorageMetadataPtr & metadata_snapshot_,
    const NamesAndTypesList & columns_list_,
    const MergeTreeIndices & skip_indices,
    CompressionCodecPtr default_codec_,
    TransactionID tid,
    bool reset_columns_,
    bool blocks_are_granules_size,
    const WriteSettings & write_settings_,
    const MergeTreeIndexGranularity & computed_index_granularity)
    : IMergedBlockOutputStream(data_part->storage.getSettings(), data_part->getDataPartStoragePtr(), metadata_snapshot_, columns_list_, reset_columns_)
    , columns_list(columns_list_)
    , default_codec(default_codec_)
    , write_settings(write_settings_)
{
    MergeTreeWriterSettings writer_settings(
        data_part->storage.getContext()->getSettingsRef(),
        write_settings,
        storage_settings,
        data_part->index_granularity_info.mark_type.adaptive,
        /* rewrite_primary_key = */ true,
        blocks_are_granules_size);

    /// TODO: looks like isStoredOnDisk() is always true for MergeTreeDataPart
    if (data_part->isStoredOnDisk())
        data_part_storage->createDirectories();

    /// NOTE do not pass context for writing to system.transactions_info_log,
    /// because part may have temporary name (with temporary block numbers). Will write it later.
    data_part->version.setCreationTID(tid, nullptr);
    data_part->storeVersionMetadata();

    writer = createMergeTreeDataPartWriter(data_part, data_part->getType(),    /// proton: updates. Add data_part as argument
            data_part->name, data_part->storage.getLogName(), data_part->getSerializations(),
            data_part_storage, data_part->index_granularity_info,
            storage_settings,
            columns_list, data_part->getColumnPositions(), metadata_snapshot,
            skip_indices, data_part->getMarksFileExtension(), default_codec, writer_settings, computed_index_granularity);
}

/// If data is pre-sorted.
void MergedBlockOutputStream::write(const Block & block)
{
    writeImpl(block, nullptr);
}

void MergedBlockOutputStream::cancel() noexcept
{
    if (writer)
        writer->cancel();
}


/** If the data is not sorted, but we pre-calculated the permutation, after which they will be sorted.
    * This method is used to save RAM, since you do not need to keep two blocks at once - the source and the sorted.
    */
void MergedBlockOutputStream::writeWithPermutation(const Block & block, const IColumn::Permutation * permutation)
{
    writeImpl(block, permutation);
}

struct MergedBlockOutputStream::Finalizer::Impl
{
    IMergeTreeDataPartWriter & writer;
    MergeTreeData::MutableDataPartPtr part;
    NameSet files_to_remove_after_finish;
    std::vector<std::unique_ptr<WriteBufferFromFileBase>> written_files;
    bool sync;

    Impl(IMergeTreeDataPartWriter & writer_, MergeTreeData::MutableDataPartPtr part_, const NameSet & files_to_remove_after_finish_, bool sync_)
        : writer(writer_)
        , part(std::move(part_))
        , files_to_remove_after_finish(files_to_remove_after_finish_)
        , sync(sync_)
    {
    }

    void finish();
    void cancel() noexcept;
};

void MergedBlockOutputStream::Finalizer::finish()
{
    std::unique_ptr<Impl> to_finish = std::move(impl);
    impl.reset();
    if (to_finish)
        to_finish->finish();
}

void MergedBlockOutputStream::Finalizer::cancel() noexcept
{
    std::unique_ptr<Impl> to_cancel = std::move(impl);
    impl.reset();
    if (to_cancel)
        to_cancel->cancel();
}

void MergedBlockOutputStream::Finalizer::Impl::finish()
{
    writer.finish(sync);

    for (auto & file : written_files)
    {
        file->finalize();
        if (sync)
            file->sync();
    }

    /// TODO: this code looks really stupid. It's because DiskTransaction is
    /// unable to see own write operations. When we merge part with column TTL
    /// and column completely outdated we first write empty column and after
    /// remove it. In case of single DiskTransaction it's impossible because
    /// remove operation will not see just written files. That is why we finish
    /// one transaction and start new...
    ///
    /// FIXME: DiskTransaction should see own writes. Column TTL implementation shouldn't be so stupid...
    if (!files_to_remove_after_finish.empty())
    {
        part->getDataPartStorage().commitTransaction();
        part->getDataPartStorage().beginTransaction();
    }

    for (const auto & file_name : files_to_remove_after_finish)
        part->getDataPartStorage().removeFile(file_name);
}

void MergedBlockOutputStream::Finalizer::Impl::cancel() noexcept
{
    writer.cancel();

    for (auto & file : written_files)
    {
        file->cancel();
    }
}

MergedBlockOutputStream::Finalizer::Finalizer(Finalizer &&) noexcept = default;
MergedBlockOutputStream::Finalizer & MergedBlockOutputStream::Finalizer::operator=(Finalizer &&) noexcept = default;
MergedBlockOutputStream::Finalizer::Finalizer(std::unique_ptr<Impl> impl_) : impl(std::move(impl_)) {}

MergedBlockOutputStream::Finalizer::~Finalizer()
{
    try
    {
        if (impl)
            finish();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void MergedBlockOutputStream::finalizePart(
    const MergeTreeMutableDataPartPtr & new_part,
    bool sync,
    const NamesAndTypesList * total_columns_list,
    MergeTreeData::DataPart::Checksums * additional_column_checksums)
{
    finalizePartAsync(new_part, sync, total_columns_list, additional_column_checksums).finish();
}

MergedBlockOutputStream::Finalizer MergedBlockOutputStream::finalizePartAsync(
    const MergeTreeMutableDataPartPtr & new_part,
    bool sync,
    const NamesAndTypesList * total_columns_list,
    MergeTreeData::DataPart::Checksums * additional_column_checksums)
{
    /// Finish write and get checksums.
    MergeTreeData::DataPart::Checksums checksums;
    NameSet checksums_to_remove;

    if (additional_column_checksums)
        checksums = std::move(*additional_column_checksums);

    /// Finish columns serialization.
    writer->fillChecksums(checksums, checksums_to_remove);

    for (const auto & name : checksums_to_remove)
        checksums.files.erase(name);

    LOG_TRACE(getLogger("MergedBlockOutputStream"), "filled checksums {}", new_part->getNameWithState());

    for (const auto & [projection_name, projection_part] : new_part->getProjectionParts())
        checksums.addFile(
            projection_name + ".proj",
            projection_part->checksums.getTotalSizeOnDisk(),
            projection_part->checksums.getTotalChecksumUInt128());

    NameSet files_to_remove_after_sync;
    if (reset_columns)
    {
        auto part_columns = total_columns_list ? *total_columns_list : columns_list;
        auto serialization_infos = new_part->getSerializationInfos();

        serialization_infos.replaceData(new_serialization_infos);
        files_to_remove_after_sync = removeEmptyColumnsFromPart(new_part, part_columns, serialization_infos, checksums);

        new_part->setColumns(part_columns, serialization_infos, metadata_snapshot->getMetadataVersion());
    }

    std::vector<std::unique_ptr<WriteBufferFromFileBase>> written_files;
    if (new_part->isStoredOnDisk())
        written_files = finalizePartOnDisk(new_part, checksums);

    new_part->rows_count = rows_count;
    new_part->modification_time = time(nullptr);
    new_part->index = writer->releaseIndexColumns();
    new_part->checksums = checksums;
    new_part->setBytesOnDisk(checksums.getTotalSizeOnDisk());
    new_part->index_granularity = writer->getIndexGranularity();
    new_part->calculateColumnsAndSecondaryIndicesSizesOnDisk();

    if (default_codec != nullptr)
        new_part->default_codec = default_codec;

    auto finalizer = std::make_unique<Finalizer::Impl>(*writer, new_part, files_to_remove_after_sync, sync);
    finalizer->written_files = std::move(written_files);
    return Finalizer(std::move(finalizer));
}

MergedBlockOutputStream::WrittenFiles MergedBlockOutputStream::finalizePartOnDisk(
    const MergeTreeMutableDataPartPtr & new_part,
    MergeTreeData::DataPart::Checksums & checksums)
{
    WrittenFiles written_files;

    auto write_hashed_file = [&](const auto & filename, auto && writer)
    {
        auto out = new_part->getDataPartStorage().writeFile(filename, 4096, write_settings);
        HashingWriteBuffer out_hashing(*out);
        writer(out_hashing);
        out_hashing.finalize();
        checksums.files[filename].file_size = out_hashing.count();
        checksums.files[filename].file_hash = out_hashing.getHash();
        out->preFinalize();
        written_files.emplace_back(std::move(out));
    };

    auto write_plain_file = [&](const auto & filename, auto && writer)
    {
        auto out = new_part->getDataPartStorage().writeFile(filename, 4096, write_settings);
        writer(*out);
        out->preFinalize();
        written_files.emplace_back(std::move(out));
    };

    if (!new_part->isProjectionPart())
    {
        if (new_part->uuid != UUIDHelpers::Nil)
        {
            write_hashed_file(IMergeTreeDataPart::UUID_FILE_NAME, [&](auto & buffer)
            {
                writeUUIDText(new_part->uuid, buffer);
            });
        }

        if (new_part->storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        {
            if (auto file = new_part->partition.store(metadata_snapshot, new_part->storage.getContext(), new_part->getDataPartStorage(), checksums))
            {
                written_files.emplace_back(std::move(file));
            }

            if (new_part->minmax_idx->initialized)
            {
                auto files = new_part->minmax_idx->store(metadata_snapshot, new_part->getDataPartStorage(), checksums);
                for (auto & file : files)
                    written_files.emplace_back(std::move(file));
            }
            else if (rows_count)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "MinMax index was not initialized for new non-empty part {}", new_part->name);
            }
        }
    }

    write_hashed_file("count.txt", [&](auto & buffer)
    {
        writeIntText(rows_count, buffer);
    });

    if (!new_part->ttl_infos.empty())
    {
        write_hashed_file("ttl.txt", [&](auto & buffer)
        {
            new_part->ttl_infos.write(buffer);
        });
    }

    const auto & serialization_infos = new_part->getSerializationInfos();
    if (!serialization_infos.empty())
    {
        write_hashed_file(IMergeTreeDataPart::SERIALIZATION_FILE_NAME, [&](auto & buffer)
        {
            serialization_infos.writeJSON(buffer);
        });
    }

    write_plain_file("columns.txt", [&](auto & buffer)
    {
        new_part->getColumns().writeText(buffer);
    });

    write_plain_file(IMergeTreeDataPart::METADATA_VERSION_FILE_NAME, [&](auto & buffer)
    {
        writeIntText(new_part->getMetadataVersion(), buffer);
    });

    if (default_codec != nullptr)
    {
        write_plain_file(IMergeTreeDataPart::DEFAULT_COMPRESSION_CODEC_FILE_NAME, [&](auto & buffer)
        {
            writeText(queryToString(default_codec->getFullCodecDesc()), buffer);
        });
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Compression codec have to be specified for part on disk, empty for {}", new_part->name);
    }

    write_plain_file("checksums.txt", [&](auto & buffer)
    {
        checksums.write(buffer);
    });

    /// proton: starts
    if (new_part->seq_info && new_part->seq_info->valid())
    {
        auto out = new_part->getDataPartStorage().writeFile("sn.txt", 4096, write_settings);
        new_part->seq_info->write(*out);
        out->preFinalize();
        written_files.emplace_back(std::move(out));
    }
    /// proton: ends

    return written_files;
}

void MergedBlockOutputStream::writeImpl(const Block & block, const IColumn::Permutation * permutation)
{
    block.checkNumberOfRows();
    size_t rows = block.rows();
    if (!rows)
        return;

    writer->write(block, permutation);
    if (reset_columns)
        new_serialization_infos.add(block);

    rows_count += rows;
}

}
