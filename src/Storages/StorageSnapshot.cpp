#include <Storages/StorageSnapshot.h>
#include <Storages/LightweightDeleteDescription.h>
#include <Storages/IStorage.h>
#include <DataTypes/ObjectUtils.h>
#include <DataTypes/NestedUtils.h>
#include <Storages/StorageView.h>
#include <sparsehash/dense_hash_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int EMPTY_LIST_OF_COLUMNS_QUERIED;
    extern const int NO_SUCH_COLUMN_IN_STREAM;
    extern const int COLUMN_QUERIED_MORE_THAN_ONCE;
}

std::shared_ptr<StorageSnapshot> StorageSnapshot::clone(DataPtr data_) const
{
    auto res = std::make_shared<StorageSnapshot>(storage, metadata);  /// proton: updates. object_columns removed

    res->projection = projection;
    res->data = std::move(data_);

    return res;
}

void StorageSnapshot::init()
{
    for (const auto & [name, type] : storage.getVirtuals())
        virtual_columns[name] = type;

    if (storage.hasLightweightDeletedMask())
        system_columns[LightweightDeleteDescription::FILTER_COLUMN.name] = LightweightDeleteDescription::FILTER_COLUMN.type;
}

NamesAndTypesList StorageSnapshot::getColumns(const GetColumnsOptions & options) const
{
    auto all_columns = getMetadataForQuery()->getColumns().get(options);

    NameSet column_names;
    if (options.with_virtuals)
    {
        /// Virtual columns must be appended after ordinary,
        /// because user can override them.
        if (!virtual_columns.empty())
        {
            for (const auto & column : all_columns)
                column_names.insert(column.name);

            for (const auto & [name, type] : virtual_columns)
                if (!column_names.contains(name))
                    all_columns.emplace_back(name, type);
        }
    }

    if (options.with_system_columns)
    {
        if (!system_columns.empty() && column_names.empty())
        {
            for (const auto & column : all_columns)
                column_names.insert(column.name);
        }

        for (const auto & [name, type] : system_columns)
            if (!column_names.contains(name))
                all_columns.emplace_back(name, type);
    }

    return all_columns;
}

NamesAndTypesList StorageSnapshot::getColumnsByNames(const GetColumnsOptions & options, const Names & names) const
{
    NamesAndTypesList res;
    for (const auto & name : names)
        res.push_back(getColumn(options, name));
    return res;
}

std::optional<NameAndTypePair> StorageSnapshot::tryGetColumn(const GetColumnsOptions & options, const String & column_name) const
{
    const auto & columns = getMetadataForQuery()->getColumns();
    auto column = columns.tryGetColumn(options, column_name);
    if (column)
        return column;

    /// if (options.with_extended_objects)
    /// {
    ///     auto object_column = object_columns.get()->tryGetColumn(options, column_name);
    ///     if (object_column)
    ///         return object_column;
    /// }

    if (options.with_virtuals)
    {
        auto it = virtual_columns.find(column_name);
        if (it != virtual_columns.end())
            return NameAndTypePair(column_name, it->second);
    }

    if (options.with_system_columns)
    {
        auto it = system_columns.find(column_name);
        if (it != system_columns.end())
            return NameAndTypePair(column_name, it->second);
    }

    return {};
}

NameAndTypePair StorageSnapshot::getColumn(const GetColumnsOptions & options, const String & column_name) const
{
    auto column = tryGetColumn(options, column_name);
    if (!column)
        throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_STREAM, "There is no column {} in table", column_name);

    return *column;
}

ASTPtr StorageSnapshot::getCodecDescOrDefault(const String & column_name, CompressionCodecPtr default_codec) const
{
    auto get_codec_or_default = [&](const auto & column_desc)
    {
        return column_desc.codec ? column_desc.codec : default_codec->getFullCodecDesc();
    };

    const auto & columns = getMetadataForQuery()->getColumns();
    if (const auto * column_desc = columns.tryGet(column_name))
        return get_codec_or_default(*column_desc);

    // if (const auto * virtual_desc = virtual_columns->tryGetDescription(column_name))
    //     return get_codec_or_default(*virtual_desc);

    return default_codec->getFullCodecDesc();
}

Block StorageSnapshot::getSampleBlockForColumns(const Names & column_names) const
{
    Block res;

    const auto & columns = getMetadataForQuery()->getColumns();
    for (const auto & column_name : column_names)
    {
        auto column = columns.tryGetColumnOrSubcolumn(GetColumnsOptions::All, column_name);
        /// proton: updates. object_columns removed
        /// auto object_column = object_columns.get()->tryGetColumnOrSubcolumn(GetColumnsOptions::All, column_name);
        if (column)
        {
            res.insert({column->type->createColumn(), column->type, column_name});
        }
        else if (auto it = virtual_columns.find(column_name); it != virtual_columns.end())
        {
            /// Virtual columns must be appended after ordinary, because user can
            /// override them.
            const auto & type = it->second;
            res.insert({type->createColumn(), type, column_name});
        }
        else
        {
            throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK,
                "Column {} not found in table {}", backQuote(column_name), storage.getStorageID().getNameForLogs());
        }
    }
    return res;
}

ColumnsDescription StorageSnapshot::getDescriptionForColumns(const Names & column_names) const
{
    ColumnsDescription res;
    const auto & columns = getMetadataForQuery()->getColumns();
    for (const auto & name : column_names)
    {
        auto column = columns.tryGetColumnOrSubcolumnDescription(GetColumnsOptions::All, name);
        if (column)
        {
            res.add(*column, "", false, false);
        }
        else if (auto it = virtual_columns.find(name); it != virtual_columns.end())
        {
            /// Virtual columns must be appended after ordinary, because user can
            /// override them.
            const auto & type = it->second;
            res.add({name, type});
        }
        else
        {
            throw Exception(ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK,
                            "Column {} not found in table {}", backQuote(name), storage.getStorageID().getNameForLogs());
        }
    }

    return res;
}

namespace
{
    using DenseHashSet = google::dense_hash_set<StringRef, StringRefHash>;
}

void StorageSnapshot::check(const Names & column_names) const
{
    const auto & columns = getMetadataForQuery()->getColumns();
    auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns();

    if (column_names.empty())
    {
        auto list_of_columns = listOfColumns(columns.get(options));
        throw Exception(ErrorCodes::EMPTY_LIST_OF_COLUMNS_QUERIED,
            "Empty list of columns queried. There are columns: {}", list_of_columns);
    }

    DenseHashSet unique_names;
    unique_names.set_empty_key(StringRef());

    for (const auto & name : column_names)
    {
        bool has_column = columns.hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, name)
            || virtual_columns.contains(name);

        if (!has_column)
        {
            auto list_of_columns = listOfColumns(columns.get(options));
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_STREAM,
                "There is no column with name {} in stream {}. There are columns: {}",
                backQuote(name), storage.getStorageID().getNameForLogs(), list_of_columns);
        }

        if (unique_names.count(name))
            throw Exception(ErrorCodes::COLUMN_QUERIED_MORE_THAN_ONCE, "Column {} queried more than once", name);

        unique_names.insert(name);
    }
}

DataTypePtr StorageSnapshot::getConcreteType(const String & column_name) const
{
    return metadata->getColumns().get(column_name).type;
}

std::shared_ptr<StorageSnapshot> StorageSnapshot::clone() const
{
    return std::make_shared<StorageSnapshot>(*this);
}

void StorageSnapshot::addVirtuals(const NamesAndTypesList & virtuals_)
{
    for (const auto & [name, type] : virtuals_)
        virtual_columns.emplace(name, type);
}

}
