#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/PathInData.h>
#include <Storages/ColumnsDescription.h>
#include <Common/FieldVisitors.h>

namespace DB
{

struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

/// Returns number of dimensions in Array type. 0 if type is not array.
size_t getNumberOfDimensions(const IDataType & type);

/// Returns number of dimensions in Array column. 0 if column is not array.
size_t getNumberOfDimensions(const IColumn & column);

/// Returns type of scalars of Array of arbitrary dimensions.
DataTypePtr getBaseTypeOfArray(const DataTypePtr & type);

/// The same as above but takes into account Tuples of Nested.
DataTypePtr getBaseTypeOfArray(DataTypePtr type, const Names & tuple_elements);

/// Returns Array type with requested scalar type and number of dimensions.
DataTypePtr createArrayOfType(DataTypePtr type, size_t num_dimensions);

/// Returns column of scalars of Array of arbitrary dimensions.
ColumnPtr getBaseColumnOfArray(const ColumnPtr & column);

/// Returns empty Array column with requested scalar column and number of dimensions.
ColumnPtr createArrayOfColumn(const ColumnPtr & column, size_t num_dimensions);

/// Returns Array with requested number of dimensions and no scalars.
Array createEmptyArrayField(size_t num_dimensions);

/// Tries to get data type by column. Only limited subset of types is supported
DataTypePtr getDataTypeByColumn(const IColumn & column);

/// Checks that each path is not the prefix of any other path.
void checkObjectHasNoAmbiguosPaths(const PathsInData & paths);

/// Receives several Tuple types and deduces the least common type among them.
DataTypePtr getLeastCommonTypeForDynamicColumns(
    const DataTypePtr & type_in_storage, const DataTypes & types, bool check_ambiguos_paths = false);

DataTypePtr createConcreteEmptyDynamicColumn(const DataTypePtr & type_in_storage);

using DataTypeTuplePtr = std::shared_ptr<DataTypeTuple>;

/// Flattens nested Tuple to plain Tuple. I.e extracts all paths and types from tuple.
/// E.g. Tuple(t Tuple(c1 UInt32, c2 String), c3 UInt64) -> Tuple(t.c1 UInt32, t.c2 String, c3 UInt32)
std::pair<PathsInData, DataTypes> flattenTuple(const DataTypePtr & type);

/// Flattens nested Tuple column to plain Tuple column.
ColumnPtr flattenTuple(const ColumnPtr & column);

/// The reverse operation to 'flattenTuple'.
/// Creates nested Tuple from all paths and types.
/// E.g. Tuple(t.c1 UInt32, t.c2 String, c3 UInt32) -> Tuple(t Tuple(c1 UInt32, c2 String), c3 UInt64)
DataTypePtr unflattenTuple(
    const PathsInData & paths,
    const DataTypes & tuple_types);

std::pair<ColumnPtr, DataTypePtr> unflattenTuple(
    const PathsInData & paths,
    const DataTypes & tuple_types,
    const Columns & tuple_columns);


/// For all columns which exist in @expected_columns and
/// don't exist in @available_columns adds to WITH clause
/// an alias with column name to literal of default value of column type.
void replaceMissedSubcolumnsByConstants(
    const ColumnsDescription & expected_columns,
    const ColumnsDescription & available_columns,
    ASTPtr query);

/// Visitor that keeps @num_dimensions_to_keep dimensions in arrays
/// and replaces all scalars or nested arrays to @replacement at that level.
class FieldVisitorReplaceScalars : public StaticVisitor<Field>
{
public:
    FieldVisitorReplaceScalars(const Field & replacement_, size_t num_dimensions_to_keep_)
        : replacement(replacement_), num_dimensions_to_keep(num_dimensions_to_keep_)
    {
    }

    Field operator()(const Array & x) const;

    template <typename T>
    Field operator()(const T &) const { return replacement; }

private:
    const Field & replacement;
    size_t num_dimensions_to_keep;
};

/// Calculates number of dimensions in array field.
/// Returns 0 for scalar fields.
class FieldVisitorToNumberOfDimensions : public StaticVisitor<size_t>
{
public:
    size_t operator()(const Array & x);

    template <typename T>
    size_t operator()(const T &) const { return 0; }

    bool need_fold_dimension = false;
};

/// Fold field (except Null) to the higher dimension, e.g. `1` -- fold 2 --> `[[1]]`
/// used to normalize dimension of element in an array. e.g [1, [2]] --> [[1], [2]]
class FieldVisitorFoldDimension : public StaticVisitor<Field>
{
public:
    explicit FieldVisitorFoldDimension(size_t num_dimensions_to_fold_) : num_dimensions_to_fold(num_dimensions_to_fold_) { }

    Field operator()(const Array & x) const;

    Field operator()(const Null & x) const { return x; }

    template <typename T>
    Field operator()(const T & x) const
    {
        if (num_dimensions_to_fold == 0)
            return x;

        Array res(1, x);
        for (size_t i = 1; i < num_dimensions_to_fold; ++i)
        {
            Array new_res;
            new_res.push_back(std::move(res));
            res = std::move(new_res);
        }

        return res;
    }

private:
    size_t num_dimensions_to_fold;
};

}
