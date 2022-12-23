#include <Storages/RocksDB/StorageEmbeddedRocksDB.h>
#include <Storages/RocksDB/EmbeddedRocksDBSink.h>
/// proton: starts.
#include <Storages/MutationCommands.h>
/// proton:ends.
#include <DataTypes/DataTypesNumber.h>

#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageFactory.h>

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTCreateQuery.h>

#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>

#include <QueryPipeline/Pipe.h>
#include <Processors/Sources/SourceWithProgress.h>

#include <Interpreters/Context.h>
#include <Interpreters/Set.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/convertFieldToType.h>

#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/logger_useful.h>
#include <base/sort.h>

#include <rocksdb/db.h>
#include <rocksdb/table.h>
#include <rocksdb/convenience.h>

#include <filesystem>
#include <shared_mutex>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ROCKSDB_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
}

using FieldVectorPtr = std::shared_ptr<FieldVector>;
using RocksDBOptions = std::unordered_map<std::string, std::string>;


static RocksDBOptions getOptionsFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & path)
{
    RocksDBOptions options;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(path, keys);

    for (const auto & key : keys)
    {
        const String key_path = path + "." + key;
        options[key] = config.getString(key_path);
    }

    return options;
}


// returns keys may be filter by condition
static bool traverseASTFilter(
    const String & primary_key, const DataTypePtr & primary_key_type, const ASTPtr & elem, const PreparedSets & sets, FieldVectorPtr & res)
{
    const auto * function = elem->as<ASTFunction>();
    if (!function)
        return false;

    if (function->name == "and")
    {
        // one child has the key filter condition is ok
        for (const auto & child : function->arguments->children)
            if (traverseASTFilter(primary_key, primary_key_type, child, sets, res))
                return true;
        return false;
    }
    else if (function->name == "or")
    {
        // make sure every child has the key filter condition
        for (const auto & child : function->arguments->children)
            if (!traverseASTFilter(primary_key, primary_key_type, child, sets, res))
                return false;
        return true;
    }
    else if (function->name == "equals" || function->name == "in")
    {
        const auto & args = function->arguments->as<ASTExpressionList &>();
        const ASTIdentifier * ident;
        const IAST * value;

        if (args.children.size() != 2)
            return false;

        if (function->name == "in")
        {
            ident = args.children.at(0)->as<ASTIdentifier>();
            if (!ident)
                return false;

            if (ident->name() != primary_key)
                return false;
            value = args.children.at(1).get();

            PreparedSetKey set_key;
            if ((value->as<ASTSubquery>() || value->as<ASTIdentifier>()))
                set_key = PreparedSetKey::forSubquery(*value);
            else
                set_key = PreparedSetKey::forLiteral(*value, {primary_key_type});

            auto set_it = sets.find(set_key);
            if (set_it == sets.end())
                return false;
            SetPtr prepared_set = set_it->second;

            if (!prepared_set->hasExplicitSetElements())
                return false;

            prepared_set->checkColumnsNumber(1);
            const auto & set_column = *prepared_set->getSetElements()[0];
            for (size_t row = 0; row < set_column.size(); ++row)
                res->push_back(set_column[row]);
            return true;
        }
        else
        {
            if ((ident = args.children.at(0)->as<ASTIdentifier>()))
                value = args.children.at(1).get();
            else if ((ident = args.children.at(1)->as<ASTIdentifier>()))
                value = args.children.at(0).get();
            else
                return false;

            if (ident->name() != primary_key)
                return false;

            /// function->name == "equals"
            if (const auto * literal = value->as<ASTLiteral>())
            {
                auto converted_field = convertFieldToType(literal->value, *primary_key_type);
                if (!converted_field.isNull())
                    res->push_back(converted_field);
                return true;
            }
        }
    }
    return false;
}


/** Retrieve from the query a condition of the form `key = 'key'`, `key in ('xxx_'), from conjunctions in the WHERE clause.
  * TODO support key like search
  */
static std::pair<FieldVectorPtr, bool> getFilterKeys(
    const String & primary_key, const DataTypePtr & primary_key_type, const SelectQueryInfo & query_info)
{
    const auto & select = query_info.query->as<ASTSelectQuery &>();
    if (!select.where())
        return {{}, true};

    FieldVectorPtr res = std::make_shared<FieldVector>();
    auto matched_keys = traverseASTFilter(primary_key, primary_key_type, select.where(), query_info.sets, res);
    return std::make_pair(res, !matched_keys);
}


class EmbeddedRocksDBSource final : public SourceWithProgress
{
public:
    EmbeddedRocksDBSource(
        const StorageEmbeddedRocksDB & storage_,
        const Block & header,
        FieldVectorPtr keys_,
        FieldVector::const_iterator begin_,
        FieldVector::const_iterator end_,
        const size_t max_block_size_)
        : SourceWithProgress(header, ProcessorID::EmbeddedRocksDBSourceID)
        , storage(storage_)
        , primary_key_pos(header.getPositionByName(storage.getPrimaryKey()))
        , keys(keys_)
        , begin(begin_)
        , end(end_)
        , it(begin)
        , max_block_size(max_block_size_)
    {
    }

    EmbeddedRocksDBSource(
        const StorageEmbeddedRocksDB & storage_,
        const Block & header,
        std::unique_ptr<rocksdb::Iterator> iterator_,
        const size_t max_block_size_)
        : SourceWithProgress(header, ProcessorID::EmbeddedRocksDBSourceID)
        , storage(storage_)
        , primary_key_pos(header.getPositionByName(storage.getPrimaryKey()))
        , iterator(std::move(iterator_))
        , max_block_size(max_block_size_)
    {
    }

    String getName() const override { return storage.getName(); }

    Chunk generate() override
    {
        if (keys)
            return generateWithKeys();
        return generateFullScan();
    }

    Chunk generateWithKeys()
    {
        if (it >= end)
            return {};

        size_t num_keys = end - begin;

        std::vector<std::string> serialized_keys(num_keys);
        std::vector<rocksdb::Slice> slices_keys(num_keys);

        const auto & sample_block = getPort().getHeader();

        const auto & key_column_type = sample_block.getByName(storage.getPrimaryKey()).type;

        size_t rows_processed = 0;
        while (it < end && rows_processed < max_block_size)
        {
            WriteBufferFromString wb(serialized_keys[rows_processed]);
            key_column_type->getDefaultSerialization()->serializeBinary(*it, wb, {});
            wb.finalize();
            slices_keys[rows_processed] = std::move(serialized_keys[rows_processed]);

            ++it;
            ++rows_processed;
        }

        MutableColumns columns = sample_block.cloneEmptyColumns();
        std::vector<String> values;
        auto statuses = storage.multiGet(slices_keys, values);
        for (size_t i = 0; i < statuses.size(); ++i)
        {
            if (statuses[i].ok())
            {
                ReadBufferFromString key_buffer(slices_keys[i]);
                ReadBufferFromString value_buffer(values[i]);
                fillColumns(key_buffer, value_buffer, columns);
            }
        }

        UInt64 num_rows = columns.at(0)->size();
        return Chunk(std::move(columns), num_rows);
    }

    Chunk generateFullScan()
    {
        if (!iterator->Valid())
            return {};

        const auto & sample_block = getPort().getHeader();
        MutableColumns columns = sample_block.cloneEmptyColumns();

        for (size_t rows = 0; iterator->Valid() && rows < max_block_size; ++rows, iterator->Next())
        {
            ReadBufferFromString key_buffer(iterator->key());
            ReadBufferFromString value_buffer(iterator->value());
            fillColumns(key_buffer, value_buffer, columns);
        }

        if (!iterator->status().ok())
        {
            throw Exception("Engine " + getName() + " got error while seeking key value data: " + iterator->status().ToString(),
                ErrorCodes::ROCKSDB_ERROR);
        }
        Block block = sample_block.cloneWithColumns(std::move(columns));
        return Chunk(block.getColumns(), block.rows());
    }

    void fillColumns(ReadBufferFromString & key_buffer, ReadBufferFromString & value_buffer, MutableColumns & columns)
    {
        size_t idx = 0;
        for (const auto & elem : getPort().getHeader())
        {
            elem.type->getDefaultSerialization()->deserializeBinary(*columns[idx], idx == primary_key_pos ? key_buffer : value_buffer, {});
            ++idx;
        }
    }

private:
    const StorageEmbeddedRocksDB & storage;

    size_t primary_key_pos;

    /// For key scan
    FieldVectorPtr keys = nullptr;
    FieldVector::const_iterator begin;
    FieldVector::const_iterator end;
    FieldVector::const_iterator it;

    /// For full scan
    std::unique_ptr<rocksdb::Iterator> iterator = nullptr;

    const size_t max_block_size;
};


StorageEmbeddedRocksDB::StorageEmbeddedRocksDB(const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata_,
        bool attach,
        ContextPtr context_,
        const String & primary_key_)
    : IStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , primary_key{primary_key_}
{
    setInMemoryMetadata(metadata_);
    rocksdb_dir = context_->getPath() + relative_data_path_;
    if (!attach)
    {
        fs::create_directories(rocksdb_dir);
    }
    initDB();
}

void StorageEmbeddedRocksDB::truncate(const ASTPtr &, const StorageMetadataPtr & , ContextPtr, TableExclusiveLockHolder &)
{
    std::unique_lock<std::shared_mutex> lock(rocksdb_ptr_mx);
    rocksdb_ptr->Close();
    rocksdb_ptr = nullptr;

    fs::remove_all(rocksdb_dir);
    fs::create_directories(rocksdb_dir);
    initDB();
}

void StorageEmbeddedRocksDB::initDB()
{
    rocksdb::Status status;
    rocksdb::Options base;
    rocksdb::DB * db;

    base.create_if_missing = true;
    base.compression = rocksdb::CompressionType::kZSTD;
    base.statistics = rocksdb::CreateDBStatistics();
    /// It is too verbose by default, and in fact we don't care about rocksdb logs at all.
    base.info_log_level = rocksdb::ERROR_LEVEL;

    rocksdb::Options merged = base;

    const auto & config = getContext()->getConfigRef();
    if (config.has("rocksdb.options"))
    {
        auto config_options = getOptionsFromConfig(config, "rocksdb.options");
        status = rocksdb::GetDBOptionsFromMap(merged, config_options, &merged);
        if (!status.ok())
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to merge rocksdb options from 'rocksdb.options' at: {}: {}",
                rocksdb_dir, status.ToString());
        }
    }
    if (config.has("rocksdb.column_family_options"))
    {
        auto column_family_options = getOptionsFromConfig(config, "rocksdb.column_family_options");
        status = rocksdb::GetColumnFamilyOptionsFromMap(merged, column_family_options, &merged);
        if (!status.ok())
        {
            throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to merge rocksdb options from 'rocksdb.options' at: {}: {}",
                rocksdb_dir, status.ToString());
        }
    }

    if (config.has("rocksdb.tables"))
    {
        auto table_name = getStorageID().getTableName();

        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys("rocksdb.tables", keys);

        for (const auto & key : keys)
        {
            const String key_prefix = "rocksdb.tables." + key;
            if (config.getString(key_prefix + ".name") != table_name)
                continue;

            String config_key = key_prefix + ".options";
            if (config.has(config_key))
            {
                auto table_config_options = getOptionsFromConfig(config, config_key);
                status = rocksdb::GetDBOptionsFromMap(merged, table_config_options, &merged);
                if (!status.ok())
                {
                    throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to merge rocksdb options from '{}' at: {}: {}",
                        config_key, rocksdb_dir, status.ToString());
                }
            }

            config_key = key_prefix + ".column_family_options";
            if (config.has(config_key))
            {
                auto table_column_family_options = getOptionsFromConfig(config, config_key);
                status = rocksdb::GetColumnFamilyOptionsFromMap(merged, table_column_family_options, &merged);
                if (!status.ok())
                {
                    throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to merge rocksdb options from '{}' at: {}: {}",
                        config_key, rocksdb_dir, status.ToString());
                }
            }
        }
    }

    status = rocksdb::DB::Open(merged, rocksdb_dir, &db);

    if (!status.ok())
    {
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to open rocksdb path at: {}: {}",
            rocksdb_dir, status.ToString());
    }
    rocksdb_ptr = std::unique_ptr<rocksdb::DB>(db);
}

Pipe StorageEmbeddedRocksDB::read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t max_block_size,
        unsigned num_streams)
{
    storage_snapshot->check(column_names);

    FieldVectorPtr keys;
    bool all_scan = false;

    Block sample_block = storage_snapshot->metadata->getSampleBlock();
    auto primary_key_data_type = sample_block.getByName(primary_key).type;
    std::tie(keys, all_scan) = getFilterKeys(primary_key, primary_key_data_type, query_info);
    if (all_scan)
    {
        auto iterator = std::unique_ptr<rocksdb::Iterator>(rocksdb_ptr->NewIterator(rocksdb::ReadOptions()));
        iterator->SeekToFirst();
        return Pipe(std::make_shared<EmbeddedRocksDBSource>(*this, sample_block, std::move(iterator), max_block_size));
    }
    else
    {
        if (keys->empty())
            return {};

        ::sort(keys->begin(), keys->end());
        keys->erase(std::unique(keys->begin(), keys->end()), keys->end());

        Pipes pipes;

        size_t num_keys = keys->size();
        size_t num_threads = std::min(size_t(num_streams), keys->size());

        assert(num_keys <= std::numeric_limits<uint32_t>::max());
        assert(num_threads <= std::numeric_limits<uint32_t>::max());

        for (size_t thread_idx = 0; thread_idx < num_threads; ++thread_idx)
        {
            size_t begin = num_keys * thread_idx / num_threads;
            size_t end = num_keys * (thread_idx + 1) / num_threads;

            pipes.emplace_back(std::make_shared<EmbeddedRocksDBSource>(
                    *this, sample_block, keys, keys->begin() + begin, keys->begin() + end, max_block_size));
        }
        return Pipe::unitePipes(std::move(pipes));
    }
}

SinkToStoragePtr StorageEmbeddedRocksDB::write(
    const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr /*context*/)
{
    return std::make_shared<EmbeddedRocksDBSink>(*this, metadata_snapshot);
}

static StoragePtr create(const StorageFactory::Arguments & args)
{
    // TODO custom RocksDBSettings, table function
    if (!args.engine_args.empty())
        throw Exception(
            "Engine " + args.engine_name + " doesn't support any arguments (" + toString(args.engine_args.size()) + " given)",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    StorageInMemoryMetadata metadata;
    metadata.setColumns(args.columns);
    metadata.setConstraints(args.constraints);

    if (!args.storage_def->primary_key)
        throw Exception("StorageEmbeddedRocksDB must require one column in primary key", ErrorCodes::BAD_ARGUMENTS);

    metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, args.getContext());
    auto primary_key_names = metadata.getColumnsRequiredForPrimaryKey();
    if (primary_key_names.size() != 1)
    {
        throw Exception("StorageEmbeddedRocksDB must require one column in primary key", ErrorCodes::BAD_ARGUMENTS);
    }
    return StorageEmbeddedRocksDB::create(args.table_id, args.relative_data_path, metadata, args.attach, args.getContext(), primary_key_names[0]);
}

std::shared_ptr<rocksdb::Statistics> StorageEmbeddedRocksDB::getRocksDBStatistics() const
{
    std::shared_lock<std::shared_mutex> lock(rocksdb_ptr_mx);
    if (!rocksdb_ptr)
        return nullptr;
    return rocksdb_ptr->GetOptions().statistics;
}

std::vector<rocksdb::Status> StorageEmbeddedRocksDB::multiGet(const std::vector<rocksdb::Slice> & slices_keys, std::vector<String> & values) const
{
    std::shared_lock<std::shared_mutex> lock(rocksdb_ptr_mx);
    if (!rocksdb_ptr)
        return {};
    return rocksdb_ptr->MultiGet(rocksdb::ReadOptions(), slices_keys, &values);
}

void registerStorageEmbeddedRocksDB(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_sort_order = true,
        .supports_parallel_insert = true,
    };

    factory.registerStorage("EmbeddedRocksDB", create, features);
}

/// proton: starts.
void StorageEmbeddedRocksDB::checkMutationIsPossible(const MutationCommands & commands, const Settings & /**/) const
{
    for (const auto & command : commands)
    {
        if (command.type != MutationCommand::UPDATE && command.type != MutationCommand::DELETE)
            /// proton: starts
            throw Exception("The engine EmbeddedRocksDB supports only UPDATE and DELETE mutations", ErrorCodes::NOT_IMPLEMENTED);
            /// proton: ends
        if (command.partition)
            /// proton: starts
            throw Exception("The engine EmbeddedRocksDB mutations in partition is not supports", ErrorCodes::NOT_IMPLEMENTED);
            /// proton: ends
    }
}

void StorageEmbeddedRocksDB::mutate(const MutationCommands & commands, ContextPtr /**/)
{
    assert(rocksdb_ptr);
    /// [get_key], We get key by where clause
    /// Supported where clause: 'key = literal' or 'literal = key'
    auto get_key = [this](const auto & command, const auto & sample_block, auto * wb_key) -> bool { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        if (!command.predicate)
            return false;
        const auto * function = command.predicate->template as<ASTFunction>();
        if (!function || function->name != "equals")
            return false;

        const auto & args = function->arguments->template as<ASTExpressionList &>();
        const ASTIdentifier * ident;
        const IAST * value;

        if (args.children.size() != 2)
            return false;

        if ((ident = args.children.at(0)->template as<ASTIdentifier>()))
            value = args.children.at(1).get();
        else if ((ident = args.children.at(1)->template as<ASTIdentifier>()))
            value = args.children.at(0).get();
        else
            return false;

        if (ident->name() != primary_key)
            return false;

        if (const auto * literal = value->template as<ASTLiteral>())
        {
            sample_block.getByName(primary_key).type->getDefaultSerialization()->serializeBinary(literal->value, *wb_key, {});
            return true;
        }

        return false;
    };

    /// [update_value], We get new value by update clause
    /// Supported update clause: 'value1 = literal', 'value2 = literal' ...
    /// In Rocksdb:
    ///     key = column(key)
    ///     value = columns(value1, value2 ...)
    /// so, we need to partial update with new value on old value
    auto update_value = [this](const auto & command, const auto & sample_block, const auto & old_value, auto * wb_value) -> bool { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
        std::unordered_map<String, Field> new_values_index_by_name;
        for (const auto & kv : command.column_to_update_expression)
        {
            if (kv.first == primary_key)
                return false;

            const auto * literal = kv.second->template as<ASTLiteral>();
            if (!literal)
                return false;

            new_values_index_by_name.emplace(kv.first, literal->value);
        }

        ReadBufferFromString old_value_buffer(old_value);
        Field replaced_value;
        for (const auto & elem : sample_block)
        {
            if (elem.name == primary_key)
                continue;

            elem.type->getDefaultSerialization()->deserializeBinary(replaced_value, old_value_buffer, {});

            /// if the column in update list, we replace old value to new value.
            auto iter = new_values_index_by_name.find(elem.name);
            if (iter != new_values_index_by_name.end())
                replaced_value = iter->second;

            elem.type->getDefaultSerialization()->serializeBinary(replaced_value, *wb_value, {});
        }

        return true;
    };

    const auto & sample_block = getInMemoryMetadataPtr()->getSampleBlock();
    WriteBufferFromOwnString wb_key;
    WriteBufferFromOwnString wb_value;
    for (const auto & command : commands)
    {
        wb_key.restart();
        /// get key
        if (!get_key(command, sample_block, &wb_key))
            /// proton: starts
            throw Exception("The engine EmbeddedRocksDB DELETE mutations key is invalid", ErrorCodes::BAD_ARGUMENTS);
            /// proton: ends

        if (command.type == MutationCommand::DELETE)
        {
            auto status = rocksdb_ptr->Delete(rocksdb::WriteOptions(), wb_key.str());
            if (!status.ok())
                throw Exception("RocksDB write error: " + status.ToString(), ErrorCodes::ROCKSDB_ERROR);
        }
        else if (command.type == MutationCommand::UPDATE)
        {
            wb_value.restart();
            String old_value;
            auto get_status = rocksdb_ptr->Get(rocksdb::ReadOptions(), wb_key.str(), &old_value);
            if (!get_status.ok())
                throw Exception("RocksDB get error: " + get_status.ToString(), ErrorCodes::ROCKSDB_ERROR);

            if (!update_value(command, sample_block, old_value, &wb_value))
                /// proton: starts
                throw Exception("The engine EmbeddedRocksDB UPDATE mutations values is invalid", ErrorCodes::BAD_ARGUMENTS);
                /// proton: ends

            auto update_status = rocksdb_ptr->Put(rocksdb::WriteOptions(), wb_key.str(), wb_value.str());
            if (!update_status.ok())
                throw Exception("RocksDB write error: " + update_status.ToString(), ErrorCodes::ROCKSDB_ERROR);
        }
        else
            /// proton: starts
            throw Exception("The engine EmbeddedRocksDB supports only UPDATE and DELETE mutations", ErrorCodes::NOT_IMPLEMENTED);
            /// proton: ends
    }
}
/// proton: ends.

}
