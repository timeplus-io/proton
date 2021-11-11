#include <Coordination/MetaSnapshotManager.h>

#include <filesystem>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>

#include "rocksdb/utilities/backupable_db.h"

namespace DB
{
std::string SNAPSHOT_META_FILE = "snapshotmeta";

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int UNKNOWN_SNAPSHOT;
    extern const int LOGICAL_ERROR;
    extern const int ROCKSDB_ERROR;
}

namespace
{
    uint64_t getSnapshotPathUpToLogIdx(const String & snapshot_path)
    {
        std::filesystem::path path(snapshot_path);
        std::string filename = path.stem();
        Strings name_parts;
        splitInto<'_'>(name_parts, filename);
        return parse<uint64_t>(name_parts[1]);
    }

    std::string getSnapshotDirName(uint64_t up_to_log_idx) { return std::string{"snapshot_"} + std::to_string(up_to_log_idx); }

    void serializeSnapshotMetadata(const SnapshotMetadataPtr & snapshot_meta, WriteBuffer & out)
    {
        auto buffer = snapshot_meta->serialize();
        writeVarUInt(buffer->size(), out);
        out.write(reinterpret_cast<const char *>(buffer->data_begin()), buffer->size());
    }

    void serializeBackupInfo(const rocksdb::BackupInfo & info, WriteBuffer & out)
    {
        writeIntBinary<uint32_t>(info.backup_id, out);
        writeIntBinary<int64_t>(info.timestamp, out);
        writeIntBinary<uint64_t>(info.size, out);
        writeIntBinary<uint32_t>(info.number_files, out);
        writeStringBinary(info.app_metadata, out);
    }

    void serializeFileList(const std::map<uint64_t, std::string> & files, WriteBuffer & out)
    {
        writeVarUInt(files.size(), out);
        for (const auto & it : files)
        {
            writeIntBinary<uint64_t>(it.first, out);
            writeStringBinary(it.second, out);
        }
    }

    SnapshotMetadataPtr deserializeSnapshotMetadata(ReadBuffer & in)
    {
        size_t data_size;
        readVarUInt(data_size, in);
        auto buffer = nuraft::buffer::alloc(data_size);
        in.readStrict(reinterpret_cast<char *>(buffer->data_begin()), data_size);
        buffer->pos(0);
        return SnapshotMetadata::deserialize(*buffer);
    }

    rocksdb::BackupInfo deserializeBackupInfo(ReadBuffer & in)
    {
        rocksdb::BackupInfo info;
        readIntBinary<uint32_t>(info.backup_id, in);
        readIntBinary<int64_t>(info.timestamp, in);
        readIntBinary<uint64_t>(info.size, in);
        readIntBinary<uint32_t>(info.number_files, in);
        readStringBinary(info.app_metadata, in);

        return info;
    }

    std::map<uint64_t, std::string> deserializeFileList(ReadBuffer & in)
    {
        std::map<uint64_t, std::string> files;
        size_t size;
        readVarUInt(size, in);
        size_t i = 0;
        while (i < size)
        {
            uint64_t idx;
            std::string path;
            readIntBinary<uint64_t>(idx, in);
            readStringBinary(path, in);
            files[idx] = std::move(path);
            i++;
        }

        return files;
    }

    void serializeBufferToDisk(nuraft::buffer & buffer, const std::string & path)
    {
        ReadBufferFromNuraftBuffer reader(buffer);
        WriteBufferFromFile plain_buf(std::filesystem::path{path});
        copyData(reader, plain_buf);
        plain_buf.sync();
    }
}

void MetaSnapshot::serialize(const MetaSnapshot & snapshot, WriteBuffer & out)
{
    writeBinary(static_cast<uint8_t>(snapshot.version), out);
    serializeSnapshotMetadata(snapshot.snapshot_meta, out);
    serializeBackupInfo(snapshot.backup_info, out);
    serializeFileList(snapshot.files, out);
}

MetaSnapshotPtr MetaSnapshot::deserialize(ReadBuffer & in)
{
    uint8_t ver;
    readBinary(ver, in);
    if (static_cast<MetaSnapshotVersion>(ver) > MetaSnapshotVersion::META_V0)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported snapshot version {}", ver);

    SnapshotMetadataPtr result = deserializeSnapshotMetadata(in);
    MetaSnapshotPtr meta_ptr = std::make_shared<MetaSnapshot>(result);

    meta_ptr->version = static_cast<MetaSnapshotVersion>(ver);
    meta_ptr->backup_info = deserializeBackupInfo(in);
    meta_ptr->files = deserializeFileList(in);

    return meta_ptr;
}

MetaSnapshot::MetaSnapshot(uint64_t up_to_log_idx_)
    : snapshot_meta(std::make_shared<SnapshotMetadata>(up_to_log_idx_, 0, std::make_shared<nuraft::cluster_config>()))
{
    //    storage->enableSnapshotMode();
    //    snapshot_container_size = storage->container.snapshotSize();
}

MetaSnapshot::MetaSnapshot(const SnapshotMetadataPtr & snapshot_meta_) : snapshot_meta(snapshot_meta_)
{
}

MetaSnapshot::~MetaSnapshot() = default;

MetaSnapshotManager::MetaSnapshotManager(const std::string & snapshots_path_, size_t snapshots_to_keep_)
    : snapshots_path(snapshots_path_), snapshots_to_keep(snapshots_to_keep_), log(&Poco::Logger::get("MetaSnapshotManager"))
{
    namespace fs = std::filesystem;

    if (!fs::exists(snapshots_path))
        fs::create_directories(snapshots_path);

    for (const auto & p : fs::directory_iterator(snapshots_path))
    {
        if (std::filesystem::is_directory(p.path()))
        {
            if (startsWith(p.path(), "tmp_")) /// Unfinished tmp files
            {
                std::filesystem::remove_all(p);
                continue;
            }
            size_t snapshot_up_to = getSnapshotPathUpToLogIdx(p.path());
            existing_snapshots[snapshot_up_to] = p.path();
            // TODO: load snapshot meta file
        }
    }

    removeOutdatedSnapshotsIfNeeded();
}

nuraft::ptr<nuraft::buffer> MetaSnapshotManager::serializeSnapshotBufferToDisk(rocksdb::DB * storage, MetaSnapshot & meta)
{
    uint64_t up_to_log_idx = meta.snapshot_meta->get_last_log_idx();
    auto snapshot_dir_name = getSnapshotDirName(up_to_log_idx);
    auto tmp_snapshot_dir_name = "tmp_" + snapshot_dir_name;
    std::string tmp_snapshot_path = std::filesystem::path{snapshots_path} / tmp_snapshot_dir_name;
    std::string new_snapshot_path = std::filesystem::path{snapshots_path} / snapshot_dir_name;

    // serialize rocksdb data
    rocksdb::Status s;
    rocksdb::BackupEngine * backup_engine;
    s = rocksdb::BackupEngine::Open(rocksdb::Env::Default(), rocksdb::BackupableDBOptions(tmp_snapshot_path), &backup_engine);
    if (!s.ok())
        throw Exception("RocksDB write error: " + s.ToString(), ErrorCodes::ROCKSDB_ERROR);

    s = backup_engine->CreateNewBackupWithMetadata(storage, std::to_string(up_to_log_idx));
    if (!s.ok())
        throw Exception("RocksDB write error: " + s.ToString(), ErrorCodes::ROCKSDB_ERROR);

    std::vector<rocksdb::BackupInfo> backup_info;
    backup_engine->GetBackupInfo(&backup_info);

    for (const rocksdb::BackupInfo & info : backup_info)
    {
        if (info.app_metadata == std::to_string(up_to_log_idx))
        {
            meta.backup_info.size = info.size;
            meta.backup_info.number_files = info.number_files;
            meta.backup_info.backup_id = info.backup_id;
            meta.backup_info.app_metadata = info.app_metadata;
            meta.backup_info.timestamp = info.timestamp;
            LOG_DEBUG(log, "Created backup {} , {} files , size: [{}]", info.backup_id, info.number_files, info.size);
        }
    }
    delete backup_engine;

    // Get Backup file list
    namespace fs = std::filesystem;

    meta.files[0] = SNAPSHOT_META_FILE;
    uint64_t count = 1;
    for (const auto & p : fs::recursive_directory_iterator(tmp_snapshot_path))
    {
        const auto & file_path = p.path();

        if (!std::filesystem::is_regular_file(file_path) || endsWith(file_path, SNAPSHOT_META_FILE))
            continue;

        meta.files[count] = file_path.string().substr(tmp_snapshot_path.length() + 1);
        LOG_DEBUG(log, "backup 1, {} file: {}", count, meta.files[count]);
        count++;
    }

    // serialize MetaSnapshot
    auto snapshot_buf = serializeSnapshotToBuffer(meta);
    serializeBufferToDisk(*snapshot_buf, std::filesystem::path{tmp_snapshot_path} / SNAPSHOT_META_FILE);


    std::filesystem::rename(tmp_snapshot_path, new_snapshot_path);
    existing_snapshots.emplace(up_to_log_idx, new_snapshot_path);
    removeOutdatedSnapshotsIfNeeded();

    return snapshot_buf;
}

nuraft::ptr<nuraft::buffer> MetaSnapshotManager::deserializeLatestSnapshotBufferFromDisk()
{
    while (!existing_snapshots.empty())
    {
        auto latest_itr = existing_snapshots.rbegin();
        try
        {
            return deserializeSnapshotBufferFromDisk(latest_itr->first);
        }
        catch (const DB::Exception &)
        {
            std::filesystem::remove(latest_itr->second);
            existing_snapshots.erase(latest_itr->first);
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    return nullptr;
}

nuraft::ptr<nuraft::buffer> MetaSnapshotManager::deserializeSnapshotBufferFromDisk(uint64_t up_to_log_idx) const
{
    const std::string & snapshot_path = existing_snapshots.at(up_to_log_idx);
    WriteBufferFromNuraftBuffer writer;
    ReadBufferFromFile reader(std::filesystem::path{snapshot_path} / SNAPSHOT_META_FILE);
    copyData(reader, writer);
    return writer.getBuffer();
}

void MetaSnapshotManager::restoreFromSnapshot(const std::string & rocksdb_dir, uint64_t up_to_log_idx) const
{
    auto snapshot_buf = deserializeSnapshotBufferFromDisk(up_to_log_idx);
    auto snapshot_meta = deserializeSnapshotFromBuffer(snapshot_buf);

    // TODO: rely on the BackupId in SnapshotMeta to restore rocksdb
    const std::string & snapshot_path = existing_snapshots.at(up_to_log_idx);
    rocksdb::BackupEngineReadOnly * backup_engine;
    rocksdb::Status s
        = rocksdb::BackupEngineReadOnly::Open(rocksdb::Env::Default(), rocksdb::BackupableDBOptions(snapshot_path), &backup_engine);
    if (!s.ok())
        throw Exception("RocksDB Backup Open error: " + s.ToString(), ErrorCodes::ROCKSDB_ERROR);

    s = backup_engine->RestoreDBFromBackup(snapshot_meta->backup_info.backup_id, rocksdb_dir, rocksdb_dir);
    if (!s.ok())
        throw Exception("RocksDB Restore error: " + s.ToString(), ErrorCodes::ROCKSDB_ERROR);

    delete backup_engine;
}

nuraft::ptr<nuraft::buffer> MetaSnapshotManager::serializeSnapshotToBuffer(const MetaSnapshot & snapshot)
{
    WriteBufferFromNuraftBuffer writer;
    CompressedWriteBuffer compressed_writer(writer);

    MetaSnapshot::serialize(snapshot, compressed_writer);
    compressed_writer.finalize();
    return writer.getBuffer();
}

MetaSnapshotPtr MetaSnapshotManager::deserializeSnapshotFromBuffer(nuraft::ptr<nuraft::buffer> buffer)
{
    ReadBufferFromNuraftBuffer reader(buffer);
    CompressedReadBuffer compressed_reader(reader);
    return MetaSnapshot::deserialize(compressed_reader);
}

void MetaSnapshotManager::saveBackupFileOfSnapshot(const MetaSnapshot & snapshot, uint64_t obj_id, nuraft::buffer & buffer)
{
    auto idx = snapshot.snapshot_meta->get_last_log_idx();
    auto snapshot_dir_name = getSnapshotDirName(idx);
    auto tmp_snapshot_dir_name = "tmp_" + snapshot_dir_name;
    std::string tmp_snapshot_path = std::filesystem::path{snapshots_path} / tmp_snapshot_dir_name;

    const std::string file_path = std::filesystem::path{tmp_snapshot_path} / snapshot.files.at(obj_id);
    auto path1 = std::filesystem::path{file_path};
    namespace fs = std::filesystem;

    if (!fs::exists(path1.parent_path()))
        fs::create_directories(path1.parent_path());

    serializeBufferToDisk(buffer, file_path);
    LOG_DEBUG(log, "save  #{} of snapshot {} to file: {}", obj_id, idx, file_path);
}

nuraft::ptr<nuraft::buffer> MetaSnapshotManager::loadBackupFileOfSnapshot(const MetaSnapshot & snapshot, uint64_t obj_id) const
{
    auto idx = snapshot.snapshot_meta->get_last_log_idx();
    auto snapshot_dir_name = getSnapshotDirName(idx);
    std::string snapshot_dir = std::filesystem::path{snapshots_path} / snapshot_dir_name;
    const std::string file_path = std::filesystem::path{snapshot_dir} / snapshot.files.at(obj_id);

    WriteBufferFromNuraftBuffer writer;
    ReadBufferFromFile reader(file_path);
    copyData(reader, writer);
    return writer.getBuffer();
}

void MetaSnapshotManager::finalizeSnapshot(uint64_t up_to_log_idx)
{
    namespace fs = std::filesystem;
    auto snapshot_dir_name = getSnapshotDirName(up_to_log_idx);
    auto tmp_snapshot_dir_name = "tmp_" + snapshot_dir_name;
    std::string tmp_snapshot_path = fs::path{snapshots_path} / tmp_snapshot_dir_name;
    std::string new_snapshot_path = fs::path{snapshots_path} / tmp_snapshot_dir_name;

    if (fs::exists(tmp_snapshot_path))
    {
        std::filesystem::rename(tmp_snapshot_path, new_snapshot_path);
        existing_snapshots.emplace(up_to_log_idx, new_snapshot_path);
        removeOutdatedSnapshotsIfNeeded();
    }
    else
        LOG_WARNING(log, "Snapshot {} does not exist in {} directory", up_to_log_idx, tmp_snapshot_path);
}

void MetaSnapshotManager::removeOutdatedSnapshotsIfNeeded()
{
    while (existing_snapshots.size() > snapshots_to_keep)
        removeSnapshot(existing_snapshots.begin()->first);
}

void MetaSnapshotManager::removeSnapshot(uint64_t log_idx)
{
    auto itr = existing_snapshots.find(log_idx);
    if (itr == existing_snapshots.end())
        throw Exception(ErrorCodes::UNKNOWN_SNAPSHOT, "Unknown snapshot with log index {}", log_idx);
    std::filesystem::remove_all(itr->second);
    existing_snapshots.erase(itr);
}
}
