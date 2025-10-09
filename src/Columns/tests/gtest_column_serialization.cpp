#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <Common/tests/gtest_global_context.h>

#include <gtest/gtest.h>

namespace DB
{

namespace ErrorCodes
{
extern const int TYPE_MISMATCH;
}

}

using namespace DB;

namespace
{

void createAndApplyBasicColumns(std::function<void(DataTypePtr, ColumnPtr)> && apply)
{
    /// Number types
    for (const auto & data_type : {
             "bool",
             "uint8",
             "int8",
             "uint16",
             "int16",
             "uint32",
             "int32",
             "uint64",
             "int64",
             "date",
             "datetime",
         })
    {
        auto type = DataTypeFactory::instance().get(data_type);
        auto col = type->createColumn();
        col->insertDefault();
        col->insert(1234567890);
        apply(std::move(type), std::move(col));
    }
    for (const auto & data_type : {"float32", "float64"})
    {
        auto type = DataTypeFactory::instance().get(data_type);
        auto col = type->createColumn();
        col->insertDefault();
        col->insert(Float64(1234567.890));
        apply(std::move(type), std::move(col));
    }
    {
        auto type = DataTypeFactory::instance().get("uint128");
        auto col = type->createColumn();
        col->insertDefault();
        col->insert(UInt128{1234567890, 1234567890});
        apply(std::move(type), std::move(col));
    }
    {
        auto type = DataTypeFactory::instance().get("int128");
        auto col = type->createColumn();
        col->insertDefault();
        col->insert(Int128{1234567890, 1234567890});
        apply(std::move(type), std::move(col));
    }
    {
        auto type = DataTypeFactory::instance().get("uint256");
        auto col = type->createColumn();
        col->insertDefault();
        col->insert(UInt256{1234567890, 1234567890});
        apply(std::move(type), std::move(col));
    }
    {
        auto type = DataTypeFactory::instance().get("int256");
        auto col = type->createColumn();
        col->insertDefault();
        col->insert(Int256{1234567890, 1234567890});
        apply(std::move(type), std::move(col));
    }

    /// Decimal types
    {
        auto type = DataTypeFactory::instance().get("decimal32(9)");
        auto col = type->createColumn();
        col->insertDefault();
        col->insert(Decimal32(1234567890));
        apply(std::move(type), std::move(col));
    }
    {
        auto type = DataTypeFactory::instance().get("decimal64(18)");
        auto col = type->createColumn();
        col->insertDefault();
        col->insert(Decimal64(1234567890));
        apply(std::move(type), std::move(col));
    }
    {
        auto type = DataTypeFactory::instance().get("decimal128(38)");
        auto col = type->createColumn();
        col->insertDefault();
        col->insert(Decimal128(1234567890));
        apply(std::move(type), std::move(col));
    }
    {
        auto type = DataTypeFactory::instance().get("decimal256(76)");
        auto col = type->createColumn();
        col->insertDefault();
        col->insert(Decimal256(1234567890));
        apply(std::move(type), std::move(col));
    }
    {
        auto type = DataTypeFactory::instance().get("datetime64(3)");
        auto col = type->createColumn();
        col->insertDefault();
        col->insert(DateTime64(1234567890));
        apply(std::move(type), std::move(col));
    }

    /// String types
    {
        auto type = DataTypeFactory::instance().get("string");
        auto col = type->createColumn();
        col->insertDefault();
        col->insert("Hello, World!");
        apply(std::move(type), std::move(col));
    }
    {
        auto type = DataTypeFactory::instance().get("fixed_string(20)");
        auto col = type->createColumn();
        col->insertDefault();
        col->insert("Hello, World!");
        apply(std::move(type), std::move(col));
    }

    /// Enum types
    {
        auto type = DataTypeFactory::instance().get("enum8('red' = 1, 'blue' = 2, 'green' = 3)");
        auto col = type->createColumn();
        col->insertDefault();
        col->insert(2);
        apply(std::move(type), std::move(col));
    }
    {
        auto type = DataTypeFactory::instance().get("enum16('red' = 1, 'blue' = 2, 'green' = 3)");
        auto col = type->createColumn();
        col->insertDefault();
        col->insert(3);
        apply(std::move(type), std::move(col));
    }

    /// UUID
    {
        auto type = DataTypeFactory::instance().get("uuid");
        auto col = type->createColumn();
        col->insertDefault();
        col->insert(UUID(UInt128{1234567890, 1234567890}));
        apply(std::move(type), std::move(col));
    }

    /// JSON
    {
        auto type = DataTypeFactory::instance().get("json(max_dynamic_types=10, max_dynamic_paths=2, b.d uint32, a.b array(string))");
        auto col = type->createColumn();
        col->insertDefault();

        Object object1 = {{"a.b", Array{String("Hello"), String("World")}}, {"a.c", Field(42)}, {"b.d", Field(42)}, {"a.d", Field("str")}, {"a.e", Field(242)}, {"a.f", Array{Field(42), Field(43)}}};
        col->insert(object1);
        apply(std::move(type), std::move(col));
    }
}

template <typename ColPtr>
void serializeAndCheck(const ColPtr & col)
{
    ASSERT_EQ(col->size(), 2);
    {
        WriteBufferFromOwnString wb;
        col->serializeValueIntoBuffer(0, wb);

        Arena arena;
        const char * begin = nullptr;
        auto ref = col->serializeValueIntoArena(0, arena, begin);

        EXPECT_EQ(wb.str(), ref.toString()) << "Failed check default value for " << col->getName();
    }

    {
        WriteBufferFromOwnString wb;
        col->serializeValueIntoBuffer(1, wb);

        Arena arena;
        const char * begin = nullptr;
        auto ref = col->serializeValueIntoArena(1, arena, begin);

        EXPECT_EQ(wb.str(), ref.toString()) << "Failed check sample value for " << col->getName();
    }
}
}

TEST(ColumnSerialization, BasicTypes)
{
    createAndApplyBasicColumns([](DataTypePtr, ColumnPtr col) { serializeAndCheck(col); });
}

TEST(ColumnSerialization, Nullable)
{
    createAndApplyBasicColumns([&](DataTypePtr, ColumnPtr nested_col) {
        auto null_map = ColumnUInt8::create(nested_col->size(), 0);
        null_map->insert(1);
        null_map->insert(0);
        auto col = ColumnNullable::create(std::move(nested_col), std::move(null_map));
        serializeAndCheck(col);
    });
}

TEST(ColumnSerialization, LowCardinality)
{
    createAndApplyBasicColumns([&](DataTypePtr nested_type, ColumnPtr nested_col) {
        /// dictionary doesn't support Decimal and Enum types
        if (isColumnedAsDecimal(nested_type) || isEnum(nested_type) || isObject(nested_type))
            return;

        MutableColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(*nested_type, IColumn::mutate(std::move(nested_col)));
        auto indexes = ColumnUInt8::create();
        indexes->insert(0);
        indexes->insert(1);
        auto col = ColumnLowCardinality::create(std::move(dictionary), std::move(indexes));
        serializeAndCheck(col);
    });
}

TEST(ColumnSerialization, LowCardinalityWithNullable)
{
    createAndApplyBasicColumns([&](DataTypePtr nested_type, ColumnPtr nested_col) {
        /// dictionary doesn't support Decimal and Enum types
        if (isColumnedAsDecimal(nested_type) || isEnum(nested_type) || isObject(nested_type))
            return;

        auto nullable_type = std::make_shared<DataTypeNullable>(nested_type);
        MutableColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(*nullable_type, IColumn::mutate(std::move(nested_col)));
        auto indexes = ColumnUInt8::create();
        indexes->insert(0);
        indexes->insert(1);
        auto col = ColumnLowCardinality::create(std::move(dictionary), std::move(indexes));
        serializeAndCheck(col);
    });
}

TEST(ColumnSerialization, Array)
{
    createAndApplyBasicColumns([&](DataTypePtr, ColumnPtr nested_col) {
        auto offsets = ColumnArray::ColumnOffsets::create();
        offsets->insert(0); /// []
        offsets->insert(2); /// [nested_col_row1, nested_col_row2]
        auto col = ColumnArray::create(std::move(nested_col), std::move(offsets));
        serializeAndCheck(col);
    });
}

TEST(ColumnSerialization, Map)
{
    createAndApplyBasicColumns([&](DataTypePtr, ColumnPtr key_col) {
        createAndApplyBasicColumns([&](DataTypePtr, ColumnPtr val_col) {
            auto offsets = ColumnArray::ColumnOffsets::create();
            offsets->insert(1);
            offsets->insert(2);
            auto col = ColumnMap::create(key_col->cloneResized(2), std::move(val_col), std::move(offsets));
            serializeAndCheck(col);
        });
    });
}

TEST(ColumnSerialization, Tuple)
{
    createAndApplyBasicColumns([&](DataTypePtr, ColumnPtr nested_col) {
        createAndApplyBasicColumns([&](DataTypePtr, ColumnPtr nested_col2) {
            auto col = ColumnTuple::create(Columns{nested_col->cloneResized(2), std::move(nested_col2)});
            serializeAndCheck(col);
        });
    });
}

TEST(ColumnSerialization, Const)
{
    createAndApplyBasicColumns([&](DataTypePtr, ColumnPtr nested_col) {
        auto col = ColumnConst::create(nested_col->cloneResized(1), 2);
        serializeAndCheck(col);
    });
}

TEST(ColumnSerialization, Sparse)
{
    createAndApplyBasicColumns([&](DataTypePtr, ColumnPtr nested_col) {
        auto offsets = ColumnUInt64::create();
        /// nested_col[0] is default value
        offsets->insert(0); /// -> nested_col[1]
        auto col = ColumnSparse::create(std::move(nested_col), std::move(offsets), 2);
        serializeAndCheck(col);
    });
}
