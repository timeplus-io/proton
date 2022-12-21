#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeObject.h>

#include <NativeLog/Base/ByteVector.h>
#include <NativeLog/Record/SchemaNativeReader.h>
#include <NativeLog/Record/SchemaNativeWriter.h>

#include <gtest/gtest.h>

namespace
{
template <typename ColumnType, typename IntegerType>
void doInsertColumnNumber(DB::Block & block, size_t rows, DB::DataTypePtr data_type)
{
    auto col = data_type->createColumn();
    auto * col_ptr = typeid_cast<ColumnType *>(col.get());

    for (size_t i = 0; i < rows; ++i)
        col_ptr->insert(IntegerType(i + 100));

    DB::ColumnWithTypeAndName col_with_name{std::move(col), data_type, typeid(*col_ptr).name()};
    block.insert(std::move(col_with_name));
}

template <typename NumberType, typename ColumnType, typename IntegerType>
void insertColumnNumber(DB::Block & block, size_t rows)
{
    doInsertColumnNumber<ColumnType, IntegerType>(block, rows, std::make_shared<NumberType>());
}

template <typename DecimalType, typename ColumnType, typename IntegerType>
void insertColumnDecimal(DB::Block & block, size_t rows, int32_t precision, int32_t scale)
{
    doInsertColumnNumber<ColumnType, IntegerType>(block, rows, std::make_shared<DecimalType>(precision, scale));
}

void insertColumnDateTime64(DB::Block & block, size_t rows)
{
    doInsertColumnNumber<DB::ColumnDecimal<DB::DateTime64>, DB::Decimal64>(block, rows, std::make_shared<DB::DataTypeDateTime64>(3));
}

[[maybe_unused]] void insertColumnNumber(DB::Block & block, size_t rows)
{
    insertColumnNumber<DB::DataTypeInt8, DB::ColumnInt8, Int8>(block, rows);
    insertColumnNumber<DB::DataTypeInt16, DB::ColumnInt16, Int16>(block, rows);
    insertColumnNumber<DB::DataTypeInt32, DB::ColumnInt32, Int32>(block, rows);
    insertColumnNumber<DB::DataTypeInt64, DB::ColumnInt64, Int64>(block, rows);
    insertColumnNumber<DB::DataTypeInt128, DB::ColumnInt128, Int128>(block, rows);
    insertColumnNumber<DB::DataTypeInt256, DB::ColumnInt256, Int256>(block, rows);

    insertColumnNumber<DB::DataTypeUInt8, DB::ColumnUInt8, UInt8>(block, rows);
    insertColumnNumber<DB::DataTypeUInt8, DB::ColumnUInt8, UInt8>(block, rows);
    insertColumnNumber<DB::DataTypeUInt16, DB::ColumnUInt16, UInt16>(block, rows);
    insertColumnNumber<DB::DataTypeUInt32, DB::ColumnUInt32, UInt32>(block, rows);
    insertColumnNumber<DB::DataTypeUInt64, DB::ColumnUInt64, UInt64>(block, rows);
    insertColumnNumber<DB::DataTypeUInt128, DB::ColumnUInt128, UInt128>(block, rows);
    insertColumnNumber<DB::DataTypeUInt256, DB::ColumnUInt256, UInt256>(block, rows);

    insertColumnNumber<DB::DataTypeFloat32, DB::ColumnFloat32, float>(block, rows);
    insertColumnNumber<DB::DataTypeFloat64, DB::ColumnFloat64, double>(block, rows);

    insertColumnDecimal<DB::DataTypeDecimal32, DB::ColumnDecimal<DB::Decimal32>, DB::Decimal32>(block, rows, 9, 3);
    insertColumnDecimal<DB::DataTypeDecimal64, DB::ColumnDecimal<DB::Decimal64>, DB::Decimal64>(block, rows, 10, 3);
    insertColumnDecimal<DB::DataTypeDecimal128, DB::ColumnDecimal<DB::Decimal128>, DB::Decimal128>(block, rows, 19, 3);
    insertColumnDecimal<DB::DataTypeDecimal256, DB::ColumnDecimal<DB::Decimal256>, DB::Decimal256>(block, rows, 39, 3);

    insertColumnDateTime64(block, rows);
}

template <typename ColumnType>
void doInsertColumnString(DB::Block & block, size_t rows, DB::DataTypePtr data_type)
{
    auto col = data_type->createColumn();
    auto * col_ptr = typeid_cast<ColumnType *>(col.get());

    for (size_t i = 0; i < rows; ++i)
        col_ptr->insert(std::to_string(i + 100));

    DB::ColumnWithTypeAndName col_with_name{std::move(col), data_type, typeid(*col_ptr).name()};
    block.insert(std::move(col_with_name));
}

[[maybe_unused]] void insertColumnString(DB::Block & block, size_t rows)
{
    doInsertColumnString<DB::ColumnString>(block, rows, std::make_shared<DB::DataTypeString>());
    doInsertColumnString<DB::ColumnFixedString>(block, rows, std::make_shared<DB::DataTypeFixedString>(16));
}

[[maybe_unused]] void insertColumnUUID(DB::Block & block, size_t rows)
{
    auto data_type = std::make_shared<DB::DataTypeUUID>();
    auto col = data_type->createColumn();
    auto * col_ptr = typeid_cast<DB::ColumnUUID *>(col.get());

    for (size_t i = 0; i < rows; ++i)
        col_ptr->insert(DB::UUIDHelpers::generateV4());

    DB::ColumnWithTypeAndName col_with_name{std::move(col), data_type, typeid(*col_ptr).name()};
    block.insert(std::move(col_with_name));
}

template <typename KeyType, typename ValueType>
void doInsertColumnMap(DB::Block & block, const String & col_name, std::function<void(DB::ColumnMap *)> insert_func)
{
    auto data_type = std::make_shared<DB::DataTypeMap>(std::make_shared<KeyType>(), std::make_shared<ValueType>());
    auto col = data_type->createColumn();
    auto * col_ptr = typeid_cast<DB::ColumnMap *>(col.get());

    insert_func(col_ptr);

    DB::ColumnWithTypeAndName col_with_name{std::move(col), data_type, col_name};
    block.insert(std::move(col_with_name));
}

[[maybe_unused]] void insertColumnMap(DB::Block & block, size_t rows)
{
    /// Key string, value string
    doInsertColumnMap<DB::DataTypeString, DB::DataTypeString>(block, "string-string-map", [&](auto * col_ptr) {
        for (size_t i = 0; i < rows; ++i)
            col_ptr->insert(DB::Map{DB::Tuple{DB::Field(std::to_string(i) + "abc"), DB::Field(std::to_string(i) + "efg")}});
    });

    /// key number, value number
    doInsertColumnMap<DB::DataTypeUInt64, DB::DataTypeUInt64>(block, "uint64-uint64_map", [&](auto * col_ptr) {
        for (size_t i = 0; i < rows; ++i)
            col_ptr->insert(DB::Map{DB::Tuple{DB::Field(i), DB::Field(i)}});
    });

    /// key string, value number
    doInsertColumnMap<DB::DataTypeString, DB::DataTypeUInt64>(block, "string-uint64_map", [&](auto * col_ptr) {
        for (size_t i = 0; i < rows; ++i)
            col_ptr->insert(DB::Map{DB::Tuple{DB::Field(std::to_string(i)), DB::Field(i)}});
    });

    /// key number, value string
    doInsertColumnMap<DB::DataTypeUInt64, DB::DataTypeString>(block, "uint64_string-map", [&](auto * col_ptr) {
        for (size_t i = 0; i < rows; ++i)
            col_ptr->insert(DB::Map{DB::Tuple{DB::Field(i), DB::Field(std::to_string(i))}});
    });
}

template <typename NestType, bool is_nullable>
void doInsertColumnLowCardinality(DB::Block & block, const String & col_name, std::function<void(DB::ColumnLowCardinality *)> insert_func)
{
    DB::DataTypePtr data_type;
    if constexpr (is_nullable)
        data_type = std::make_shared<DB::DataTypeLowCardinality>(std::make_shared<DB::DataTypeNullable>(std::make_shared<NestType>()));
    else
        data_type = std::make_shared<DB::DataTypeLowCardinality>(std::make_shared<NestType>());

    auto col = data_type->createColumn();
    auto * col_ptr = typeid_cast<DB::ColumnLowCardinality *>(col.get());

    insert_func(col_ptr);

    DB::ColumnWithTypeAndName col_with_name{std::move(col), data_type, col_name};
    block.insert(std::move(col_with_name));
}

[[maybe_unused]] void insertColumnLowCardinality(DB::Block & block, size_t rows)
{
    /// LowCardinality number
    doInsertColumnLowCardinality<DB::DataTypeUInt64, false>(block, "uint64_lc", [&](auto * col_ptr) {
        for (size_t i = 0; i < rows; ++i)
            col_ptr->insert(rows);
    });

    /// LowCardinality string
    doInsertColumnLowCardinality<DB::DataTypeString, false>(block, "string_lc", [&](auto * col_ptr) {
        for (size_t i = 0; i < rows; ++i)
            col_ptr->insert(std::to_string(rows));
    });

    /// LowCardinality nullable number
    doInsertColumnLowCardinality<DB::DataTypeUInt64, true>(block, "nullable_uint64_lc", [&](auto * col_ptr) {
        for (size_t i = 0; i < rows; ++i)
            col_ptr->insert(rows);
    });

    /// LowCardinality nullable string
    doInsertColumnLowCardinality<DB::DataTypeString, true>(block, "nullable_string_lc", [&](auto * col_ptr) {
        for (size_t i = 0; i < rows; ++i)
            col_ptr->insert(std::to_string(rows));
    });
}

template <typename NestType>
void doInsertColumnNullable(DB::Block & block, const String & col_name, std::function<void(DB::ColumnNullable *)> insert_func)
{
    auto data_type = std::make_shared<DB::DataTypeNullable>(std::make_shared<NestType>());

    auto col = data_type->createColumn();
    auto * col_ptr = typeid_cast<DB::ColumnNullable *>(col.get());

    insert_func(col_ptr);

    DB::ColumnWithTypeAndName col_with_name{std::move(col), data_type, col_name};
    block.insert(std::move(col_with_name));
}

[[maybe_unused]] void insertColumnNullable(DB::Block & block, size_t rows)
{
    /// Nullable number
    doInsertColumnNullable<DB::DataTypeUInt64>(block, "uint64_nullable", [&](auto * col_ptr) {
        for (size_t i = 0; i < rows; ++i)
            col_ptr->insert(i);
    });

    /// Nullable string
    doInsertColumnNullable<DB::DataTypeString>(block, "string_nullable", [&](auto * col_ptr) {
        for (size_t i = 0; i < rows; ++i)
            col_ptr->insert(std::to_string(i));
    });
}

template <typename NestType>
void doInsertColumnArray(DB::Block & block, const String & col_name, std::function<void(DB::ColumnArray *)> insert_func)
{
    auto data_type = std::make_shared<DB::DataTypeArray>(std::make_shared<NestType>());

    auto col = data_type->createColumn();
    auto * col_ptr = typeid_cast<DB::ColumnArray *>(col.get());

    insert_func(col_ptr);

    DB::ColumnWithTypeAndName col_with_name{std::move(col), data_type, col_name};
    block.insert(std::move(col_with_name));
}

template <typename KeyType, typename ValueType>
void doInsertColumnArrayMap(DB::Block & block, const String & col_name, std::function<void(DB::ColumnArray *)> insert_func)
{
    auto data_type = std::make_shared<DB::DataTypeArray>(
        std::make_shared<DB::DataTypeMap>(std::make_shared<KeyType>(), std::make_shared<ValueType>()));

    auto col = data_type->createColumn();
    auto * col_ptr = typeid_cast<DB::ColumnArray *>(col.get());

    insert_func(col_ptr);

    DB::ColumnWithTypeAndName col_with_name{std::move(col), data_type, col_name};
    block.insert(std::move(col_with_name));
}

[[maybe_unused]] void insertColumnArrayMap(DB::Block & block, size_t rows)
{
    /// array of string-string map
    doInsertColumnArrayMap<DB::DataTypeString, DB::DataTypeString>(block, "array-string-string-map", [&](auto * col_ptr) {
        for (size_t i = 0; i < rows; ++i)
        {
            DB::Map m{DB::Tuple{DB::Field(std::to_string(i)), DB::Field(std::to_string(i))}};
            col_ptr->insert(DB::Array{m, m, m});
        }
    });

    /// array of number-number map
    doInsertColumnArrayMap<DB::DataTypeUInt64, DB::DataTypeUInt64>(block, "array-number-number-map", [&](auto * col_ptr) {
        for (size_t i = 0; i < rows; ++i)
        {
            DB::Map m{DB::Tuple{DB::Field(i), DB::Field(i)}};
            col_ptr->insert(DB::Array{m, m, m});
        }
    });
}

[[maybe_unused]] void insertColumnArrayTuple(DB::Block & block, size_t rows)
{
    /// Tuple: fixed string, string, number, nullable(string), nullable(number), array number, array string,
    /// map[string]string, map[number]number
    auto string_type = std::make_shared<DB::DataTypeString>();
    auto number_type = std::make_shared<DB::DataTypeUInt64>();
    std::vector<DB::DataTypePtr> types = {
        std::make_shared<DB::DataTypeFixedString>(16),
        string_type,
        number_type,
        std::make_shared<DB::DataTypeNullable>(string_type),
        std::make_shared<DB::DataTypeNullable>(number_type),
        std::make_shared<DB::DataTypeArray>(number_type),
        std::make_shared<DB::DataTypeArray>(string_type),
        std::make_shared<DB::DataTypeMap>(string_type, string_type),
        std::make_shared<DB::DataTypeMap>(number_type, number_type),
    };

    auto nested_data_type = std::make_shared<DB::DataTypeTuple>(types);
    auto data_type = std::make_shared<DB::DataTypeArray>(nested_data_type);

    auto col = data_type->createColumn();
    auto * col_ptr = typeid_cast<DB::ColumnArray *>(col.get());

    for (size_t i = 0; i < rows; ++i)
    {
        auto tuple_field = DB::Tuple{
            DB::Field{"abc"},
            DB::Field{"xyz"},
            DB::Field{i},
            DB::Field{"nullable abc"},
            DB::Field{i},
            DB::Array{DB::Field{i}, DB::Field{i}, DB::Field{i}},
            DB::Array{DB::Field{std::to_string(i)}, DB::Field{std::to_string(i)}, DB::Field{std::to_string(i)}},
            DB::Map{DB::Tuple{DB::Field{std::to_string(i)}, DB::Field{std::to_string(i)}}},
            DB::Map{DB::Tuple{DB::Field{i}, DB::Field{i}}},
        };
        col_ptr->insert(DB::Array{tuple_field, tuple_field, tuple_field});
    }

    DB::ColumnWithTypeAndName col_with_name{std::move(col), data_type, "array-tuple"};
    block.insert(std::move(col_with_name));
}

template<typename NestType>
void doInsertColumnArrayArray(DB::Block & block, const String & col_name, std::function<void(DB::ColumnArray *)> insert_func)
{
    auto data_type = std::make_shared<DB::DataTypeArray>(std::make_shared<DB::DataTypeArray>(std::make_shared<NestType>()));

    auto col = data_type->createColumn();
    auto * col_ptr = typeid_cast<DB::ColumnArray *>(col.get());

    insert_func(col_ptr);

    DB::ColumnWithTypeAndName col_with_name{std::move(col), data_type, col_name};
    block.insert(std::move(col_with_name));
}

[[maybe_unused]] void insertColumnArrayArray(DB::Block & block, size_t rows)
{
    /// array of string array
    doInsertColumnArrayArray<DB::DataTypeString>(block, "array-of-string-array", [&](auto * col_ptr) {
        for (size_t i = 0; i < rows; ++i)
        {
            DB::Array arr{DB::Field{std::to_string(i)}, DB::Field{std::to_string(i)}, DB::Field{std::to_string(i)}};
            col_ptr->insert(DB::Array{arr, arr, arr});
        }
    });

    /// array of number array
    doInsertColumnArrayArray<DB::DataTypeUInt64>(block, "array-of-number-array", [&](auto * col_ptr) {
        for (size_t i = 0; i < rows; ++i)
        {
            DB::Array arr{DB::Field{i}, DB::Field{i}, DB::Field{i}};
            col_ptr->insert(DB::Array{arr, arr, arr});
        }
    });
}

[[maybe_unused]] void insertColumnArray(DB::Block & block, size_t rows)
{
    /// Array string
    doInsertColumnArray<DB::DataTypeString>(block, "string_array", [&](auto * col_ptr) {
        for (size_t i = 0; i < rows; ++i)
        {
            auto f = DB::Field(std::to_string(i));
            col_ptr->insert(DB::Array{f, f, f, f});
        }
    });

    /// Array Number
    doInsertColumnArray<DB::DataTypeUInt64>(block, "uint64_array", [&](auto * col_ptr) {
        for (size_t i = 0; i < rows; ++i)
        {
            auto f = DB::Field(i);
            col_ptr->insert(DB::Array{f, f, f, f});
        }
    });

    /// Array Map
    insertColumnArrayMap(block, rows);

    /// Array tuple
    insertColumnArrayTuple(block, rows);

    /// Array array
    insertColumnArrayArray(block, rows);
}

[[maybe_unused]] void insertColumnTuple(DB::Block & block, size_t rows)
{
    /// Tuple: fixed string, string, number, nullable(string), nullable(number), array number, array string,
    /// map[string]string, map[number]number
    auto string_type = std::make_shared<DB::DataTypeString>();
    auto number_type = std::make_shared<DB::DataTypeUInt64>();
    std::vector<DB::DataTypePtr> types = {
        std::make_shared<DB::DataTypeFixedString>(16),
        string_type,
        number_type,
        std::make_shared<DB::DataTypeNullable>(string_type),
        std::make_shared<DB::DataTypeNullable>(number_type),
        std::make_shared<DB::DataTypeArray>(number_type),
        std::make_shared<DB::DataTypeArray>(string_type),
        std::make_shared<DB::DataTypeMap>(string_type, string_type),
        std::make_shared<DB::DataTypeMap>(number_type, number_type),
    };

    DB::Strings names = {
        "fixed_string",
        "string",
        "uint64",
        "nullable_string",
        "nullable_uint64",
        "array_uint64",
        "array_string",
        "map_string_string",
        "map_uint64_uint64",
    };

    EXPECT_EQ(types.size(), names.size());

    auto insert_func = [&](DB::DataTypePtr type, const String & col_name) {
        auto col = type->createColumn();
        auto * col_ptr = typeid_cast<DB::ColumnTuple *>(col.get());

        for (size_t i = 0; i < rows; ++i)
        {
            col_ptr->insert(DB::Tuple{
                DB::Field{"abc"},
                DB::Field{"xyz"},
                DB::Field{i},
                DB::Field{"nullable abc"},
                DB::Field{i},
                DB::Array{DB::Field{i}, DB::Field{i}, DB::Field{i}},
                DB::Array{DB::Field{std::to_string(i)}, DB::Field{std::to_string(i)}, DB::Field{std::to_string(i)}},
                DB::Map{DB::Tuple{DB::Field{std::to_string(i)}, DB::Field{std::to_string(i)}}},
                DB::Map{DB::Tuple{DB::Field{i}, DB::Field{i}}},
            });
        }

        DB::ColumnWithTypeAndName col_with_name{std::move(col), type, col_name};
        block.insert(std::move(col_with_name));
    };

    auto data_type = std::make_shared<DB::DataTypeTuple>(types);
    insert_func(data_type, "tuple_without_names");

    data_type = std::make_shared<DB::DataTypeTuple>(types, names);
    insert_func(data_type, "tuple_with_names");
}

void insertColumnSparse(DB::Block & block, size_t rows)
{
    /// number column
    auto number_type = std::make_shared<DB::DataTypeUInt64>();
    auto number_col = number_type->createColumn();
    auto sparse_number = DB::ColumnSparse::create(number_col->assumeMutable());
    auto number_col2 = number_type->createColumn();
    auto sparse_number2 = DB::ColumnSparse::create(number_col2->assumeMutable());

    for (size_t i = 0; i < rows; ++i)
    {
        if (i % 2 == 0)
            sparse_number->insertDefault();
        else
            sparse_number->insert(i);

        sparse_number2->insertDefault();
    }

    auto string_type = std::make_shared<DB::DataTypeString>();
    auto string_col = string_type->createColumn();
    auto sparse_string = DB::ColumnSparse::create(string_col->assumeMutable());
    auto string_col2 = string_type->createColumn();
    auto sparse_string2 = DB::ColumnSparse::create(string_col2->assumeMutable());

    for (size_t i = 0; i < rows; ++i)
    {
        if (i % 2 == 0)
            sparse_string->insertDefault();
        else
            sparse_string->insert(std::to_string(i));

        sparse_string2->insertDefault();
    }

    block.insert(DB::ColumnWithTypeAndName{std::move(sparse_number), number_type, "sparse-number"});
    block.insert(DB::ColumnWithTypeAndName{std::move(sparse_number2), number_type, "sparse-number-all-default"});
    block.insert(DB::ColumnWithTypeAndName{std::move(sparse_string), string_type, "sparse-string"});
    block.insert(DB::ColumnWithTypeAndName{std::move(sparse_string2), string_type, "sparse-string-all-default"});
}

void insertColumnObject(DB::Block & block, size_t rows)
{
    /// Json: {"data": string, "id": number, "obj": {"data": string, "array": [numbers]}, "array": [strings]}
    auto insert_func = [&](DB::DataTypePtr type, const String & col_name) {
        auto col = type->createColumn();
        auto * col_ptr = typeid_cast<DB::ColumnObject *>(col.get());

        for (size_t i = 0; i < rows; ++i)
        {
            col_ptr->insert(DB::Object{
                {"data", DB::Field{"abc"}},
                {"id", DB::Field{i}},
                {"obj.data", DB::Field{"xyz"}},
                {"obj.array", DB::Array{DB::Field{i}, DB::Field{i}, DB::Field{i}}},
                {"array", DB::Array{DB::Field{std::to_string(i)}, DB::Field{std::to_string(i)}, DB::Field{std::to_string(i)}}}
            });
        }

        DB::ColumnWithTypeAndName col_with_name{std::move(col), type, col_name};
        block.insert(std::move(col_with_name));
    };

    auto json_type = std::make_shared<DB::DataTypeObject>("json", false);
    auto nullable_json_type = std::make_shared<DB::DataTypeObject>("json", true);
    insert_func(json_type, "json_obj");
    insert_func(nullable_json_type, "nullable_json_obj");
}

void checkColumnObject(DB::ColumnPtr origin, DB::ColumnPtr actual, size_t rows, const std::vector<std::string> & requried_subcolumns = {})
{
    const auto * col_object = assert_cast<const DB::ColumnObject *>(origin.get());
    const auto * col_object_got = assert_cast<const DB::ColumnObject *>(actual.get());
    EXPECT_TRUE(col_object != nullptr);
    EXPECT_TRUE(col_object_got != nullptr);

    /// Check got subcolumns size
    const auto & subcolumns_got = col_object_got->getSubcolumns();
    auto subcolumns_size_expected = requried_subcolumns.empty() ? col_object->getSubcolumns().size() : requried_subcolumns.size();
    EXPECT_EQ(subcolumns_got.size(), subcolumns_size_expected);

    for (const auto & entry : subcolumns_got)
    {
        EXPECT_TRUE(col_object->hasSubcolumn(entry->path));
        const auto & subcolumn_data = col_object->getSubcolumn(entry->path);

        /// Check got subcolumn type
        EXPECT_TRUE(subcolumn_data.getLeastCommonType()->equals(*(entry->data.getLeastCommonType())));

        /// Check got subcolumn
        auto subcolumn_expected = subcolumn_data.getFinalizedColumnPtr();
        auto subcolumn_got = entry->data.getFinalizedColumnPtr();
        for (size_t row = 0; row < rows; ++row)
        {
            EXPECT_EQ(subcolumn_expected->compareAt(row, row, *subcolumn_got, -1), 0);
        }
    }
}

void checkBlock(const DB::Block & origin, DB::Block actual, const DB::SourceColumnsDescription::PhysicalColumnPositions & physical_positions)
{
    EXPECT_EQ(actual.columns(), physical_positions.positions.size());
    EXPECT_EQ(actual.rows(), origin.rows());

    for (size_t pos = 0; pos < origin.columns(); ++pos)
    {
        auto iter = std::find(physical_positions.positions.begin(), physical_positions.positions.end(), pos);
        if (iter == physical_positions.positions.end())
        {
            /// skipped column
            EXPECT_TRUE(actual.findByName(origin.getByPosition(pos).name) == nullptr);
        }
        else
        {
            /// Validate column values
            auto col_expected = origin.getByPosition(pos);
            auto * col_got = actual.findByName(col_expected.name);
            EXPECT_TRUE(col_got != nullptr);

            if (isObject(col_got->type))
            {
                auto target_pos = std::distance(physical_positions.positions.begin(), iter);
                auto subcolumns_iter = physical_positions.subcolumns.find(target_pos);
                if (subcolumns_iter != physical_positions.subcolumns.end())
                    checkColumnObject(col_expected.column, col_got->column, actual.rows(), subcolumns_iter->second);
                else
                    checkColumnObject(col_expected.column, col_got->column, actual.rows());
            }
            else
                /// std::cout << "comparing " << col_expected.name << "\n";
                for (size_t row = 0; row < actual.rows(); ++row)
                    EXPECT_EQ(col_expected.column->compareAt(row, row, *col_got->column, -1), 0);
        }
    }
}

DB::Block createBlock(size_t rows)
{
    DB::Block block;

    insertColumnNumber(block, rows);

    insertColumnTuple(block, rows);

    insertColumnUUID(block, rows);

    insertColumnMap(block, rows);

    insertColumnLowCardinality(block, rows);

    insertColumnNullable(block, rows);

    insertColumnArray(block, rows);

    insertColumnSparse(block, rows);

    insertColumnString(block, rows);

    insertColumnObject(block, rows);

    return block;
}
}

DB::Block createBlockBig(size_t rows)
{
    DB::Block block;

    insertColumnNumber(block, rows);

    /// insertColumnTuple(block, rows);

    insertColumnUUID(block, rows);

    insertColumnMap(block, rows);

    /// insertColumnLowCardinality(block, rows);

    insertColumnNullable(block, rows);

    insertColumnArray(block, rows);

    /// insertColumnSparse(block, rows);

    insertColumnString(block, rows);

    return block;
}

DB::Block createBlockForCompress(size_t rows, bool only_string)
{
    DB::Block block;

    if (!only_string)
    {
        insertColumnNumber(block, rows);

        /// insertColumnTuple(block, rows);

        insertColumnUUID(block, rows);

        insertColumnMap(block, rows);

        /// insertColumnLowCardinality(block, rows);

        insertColumnNullable(block, rows);

        insertColumnArray(block, rows);

        /// insertColumnSparse(block, rows);
    }

    insertColumnString(block, rows);

    return block;
}

TEST(RecordSerder, Skip)
{
    size_t rows = 3;
    auto block = createBlock(rows);

    /// Serialize everything
    nlog::ByteVector data{static_cast<size_t>((block.bytes() + 2) * 1.5)};
    DB::WriteBufferFromVector wb{data};
    /// write all columns
    nlog::SchemaNativeWriter writer(wb, {});
    writer.write(block);
    wb.finalize();

    struct TestSchemaProvider : public nlog::SchemaProvider
    {
        TestSchemaProvider(DB::Block header_) : header(std::move(header_)) { }

        const DB::Block & getSchema(uint16_t /*schema_version*/) const override { return header; }

        DB::Block header;
    };

    /// skip deserialize
    TestSchemaProvider schema_provider(block.cloneEmpty());
    nlog::SchemaContext schema_ctx(schema_provider);

    uint16_t schema_version = 0;
    for (size_t pos = 0; pos < block.columns(); ++pos)
    {
        DB::ReadBufferFromMemory rb{data.data(), data.size()};

        /// Only read column at pos and skip all others
        schema_ctx.column_positions = {static_cast<uint16_t>(pos)};

        nlog::SchemaNativeReader reader{rb, false, schema_version, schema_ctx};

        DB::Block new_block;
        reader.read(new_block);
        checkBlock(block, new_block, schema_ctx.column_positions);
    }

    for (size_t pos = 0; pos < block.columns(); ++pos)
    {
        DB::ReadBufferFromMemory rb{data.data(), data.size()};

        DB::SourceColumnsDescription::PhysicalColumnPositions column_positions;
        column_positions.positions.reserve(block.columns() - 1);

        for (size_t i = 0; i < block.columns(); ++i)
        {
            if (i != pos)
                column_positions.positions.push_back(i);
        }

        schema_ctx.column_positions = column_positions;

        nlog::SchemaNativeReader reader{rb, false, schema_version, schema_ctx};

        /// Skip current pos
        if (!column_positions.positions.empty())
        {
            DB::Block new_block;
            reader.read(new_block);
            checkBlock(block, new_block, column_positions);
        }
    }
}

TEST(RecordSerder, SkipPartialJsonSubcolumns)
{
    size_t rows = 3;
    auto block = createBlock(rows);

    /// Serialize everything
    nlog::ByteVector data{static_cast<size_t>((block.bytes() + 2) * 1.5)};
    DB::WriteBufferFromVector wb{data};
    /// write all columns
    nlog::SchemaNativeWriter writer(wb, {});
    writer.write(block);
    wb.finalize();

    struct TestSchemaProvider : public nlog::SchemaProvider
    {
        TestSchemaProvider(DB::Block header_) : header(std::move(header_)) { }

        const DB::Block & getSchema(uint16_t /*schema_version*/) const override { return header; }

        DB::Block header;
    };

    TestSchemaProvider schema_provider(block.cloneEmpty());
    nlog::SchemaContext schema_ctx(schema_provider);

    uint16_t schema_version = 0;

    /// Only read partial subcolumns of json
    for (size_t pos = 0; pos < block.columns(); ++pos)
    {
        if (!isObject(block.getByPosition(pos).type))
            continue;

        DB::ReadBufferFromMemory rb{data.data(), data.size()};

        schema_ctx.column_positions.positions = {static_cast<uint16_t>(pos)};
        schema_ctx.column_positions.subcolumns = {{0, {"data"}}};

        nlog::SchemaNativeReader reader{rb, false, schema_version, schema_ctx};

        DB::Block new_block;
        reader.read(new_block);
        checkBlock(block, new_block, schema_ctx.column_positions);
    }

    /// Only read more partial subcolumns of json
    for (size_t pos = 0; pos < block.columns(); ++pos)
    {
        if (!isObject(block.getByPosition(pos).type))
            continue;

        DB::ReadBufferFromMemory rb{data.data(), data.size()};

        /// Only read column at pos and skip all others
        schema_ctx.column_positions.positions = {static_cast<uint16_t>(pos)};
        schema_ctx.column_positions.subcolumns = {{0, {"data", "obj.data", "obj.array", "array", "id"}}};

        nlog::SchemaNativeReader reader{rb, false, schema_version, schema_ctx};

        DB::Block new_block;
        reader.read(new_block);
        checkBlock(block, new_block, schema_ctx.column_positions);
    }

    /// Read partial subcolumns of json and others columns
    {
        schema_ctx.column_positions.clear();
        for (size_t pos = 0; pos < block.columns(); ++pos)
        {
            if (isObject(block.getByPosition(pos).type))
                schema_ctx.column_positions.subcolumns[schema_ctx.column_positions.positions.size()] = {"data"};

            schema_ctx.column_positions.positions.push_back(pos);
        }
        DB::ReadBufferFromMemory rb{data.data(), data.size()};
        nlog::SchemaNativeReader reader{rb, false, schema_version, schema_ctx};

        DB::Block new_block;
        reader.read(new_block);
        checkBlock(block, new_block, schema_ctx.column_positions);
    }

    /// Read more partial subcolumns of json and others columns
    {
        schema_ctx.column_positions.clear();
        for (size_t pos = 0; pos < block.columns(); ++pos)
        {
            if (isObject(block.getByPosition(pos).type))
                schema_ctx.column_positions.subcolumns[schema_ctx.column_positions.positions.size()] = {"data", "obj.data", "obj.array", "array", "id"};

            schema_ctx.column_positions.positions.push_back(pos);
        }
        DB::ReadBufferFromMemory rb{data.data(), data.size()};
        nlog::SchemaNativeReader reader{rb, false, schema_version, schema_ctx};

        DB::Block new_block;
        reader.read(new_block);
        checkBlock(block, new_block, schema_ctx.column_positions);
    }
}
