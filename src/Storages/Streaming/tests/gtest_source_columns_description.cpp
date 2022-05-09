#include <gtest/gtest.h>

#include <DataTypes/DataTypeFactory.h>
#include <Storages/Streaming/SourceColumnsDescription.h>
#include <Common/ProtonCommon.h>

namespace
{
DB::DataTypePtr getType(const std::string full_name)
{
    return DB::DataTypeFactory::instance().get(full_name);
}

/// schema: "col0" int, "col1" string, "col2" tuple, "col3" tuple, "col4" json, "col5" json,
///         "_tp_time" datetime64(3, UTC), "_tp_index_time" datetime64(3, UTC)
DB::Block generateCommonSchema()
{
    return DB::Block{DB::ColumnsWithTypeAndName{
        {getType("int"), "col0"},
        {getType("string"), "col1"},
        {getType("tuple(a int, b int)"), "col2"},
        {getType("tuple(x int, y string)"), "col3"},
        {getType("json"), "col4"},
        {getType("json"), "col5"},
        {getType("datetime64(3, UTC)"), DB::ProtonConsts::RESERVED_EVENT_TIME},
        {getType("datetime64(3, UTC)"), DB::ProtonConsts::RESERVED_INDEX_TIME}}};
}

constexpr auto Physical = DB::SourceColumnsDescription::ReadColumnType::PHYSICAL;
constexpr auto Virtual = DB::SourceColumnsDescription::ReadColumnType::VIRTUAL;
constexpr auto Sub = DB::SourceColumnsDescription::ReadColumnType::SUB;
}

TEST(SourceColumnsDescription, AllPhysical)
{
    auto schema = generateCommonSchema();

    DB::NamesAndTypesList columns_to_read{{"col1", getType("string")}, {"col3", getType("tuple(x int, y string)")}};
    DB::SourceColumnsDescription columns_desc(columns_to_read, schema);
    /// Pos to read
    ASSERT_EQ(columns_desc.positions.size(), 2);
    ASSERT_EQ(columns_desc.positions[0].type(), Physical);
    EXPECT_EQ(columns_desc.positions[0].physicalPosition(), 0);
    ASSERT_EQ(columns_desc.positions[1].type(), Physical);
    EXPECT_EQ(columns_desc.positions[1].physicalPosition(), 1);

    /// Physical columns description
    ASSERT_EQ(columns_desc.physical_column_positions_to_read.size(), 2);
    EXPECT_EQ(columns_desc.physical_column_positions_to_read[0], 1);
    EXPECT_EQ(columns_desc.physical_column_positions_to_read[1], 3);

    /// Virtual columns description
    ASSERT_EQ(columns_desc.virtual_time_columns_calc.size(), 0);
    ASSERT_EQ(columns_desc.virtual_col_type, nullptr);

    /// Sub-columns description
    ASSERT_EQ(columns_desc.subcolumns_to_read.size(), 0);

    /// Json description
    ASSERT_EQ(columns_desc.physical_object_column_names_to_read.size(), 0);
}

TEST(SourceColumnsDescription, AllPhysicalWithJSON)
{
    auto schema = generateCommonSchema();

    DB::NamesAndTypesList columns_to_read{{"col1", getType("string")}, {"col4", getType("json")}};
    DB::SourceColumnsDescription columns_desc(columns_to_read, schema);
    /// Pos to read
    ASSERT_EQ(columns_desc.positions.size(), 2);
    ASSERT_EQ(columns_desc.positions[0].type(), Physical);
    EXPECT_EQ(columns_desc.positions[0].physicalPosition(), 0);
    ASSERT_EQ(columns_desc.positions[1].type(), Physical);
    EXPECT_EQ(columns_desc.positions[1].physicalPosition(), 1);

    /// Physical columns description
    ASSERT_EQ(columns_desc.physical_column_positions_to_read.size(), 2);
    EXPECT_EQ(columns_desc.physical_column_positions_to_read[0], 1);
    EXPECT_EQ(columns_desc.physical_column_positions_to_read[1], 4);

    /// Virtual columns description
    ASSERT_EQ(columns_desc.virtual_time_columns_calc.size(), 0);
    ASSERT_EQ(columns_desc.virtual_col_type, nullptr);

    /// Sub-columns description
    ASSERT_EQ(columns_desc.subcolumns_to_read.size(), 0);

    /// Json description
    ASSERT_EQ(columns_desc.physical_object_column_names_to_read.size(), 1);
    EXPECT_EQ(columns_desc.physical_object_column_names_to_read[0], "col4");
}

TEST(SourceColumnsDescription, AllPhysicalAndAllJson)
{
    auto schema = generateCommonSchema();

    DB::NamesAndTypesList columns_to_read{{"col5", getType("json")}, {"col4", getType("json")}};
    DB::SourceColumnsDescription columns_desc(columns_to_read, schema);
    /// Pos to read
    ASSERT_EQ(columns_desc.positions.size(), 2);
    ASSERT_EQ(columns_desc.positions[0].type(), Physical);
    EXPECT_EQ(columns_desc.positions[0].physicalPosition(), 0);
    ASSERT_EQ(columns_desc.positions[1].type(), Physical);
    EXPECT_EQ(columns_desc.positions[1].physicalPosition(), 1);

    /// Physical columns description
    ASSERT_EQ(columns_desc.physical_column_positions_to_read.size(), 2);
    EXPECT_EQ(columns_desc.physical_column_positions_to_read[0], 5);
    EXPECT_EQ(columns_desc.physical_column_positions_to_read[1], 4);

    /// Virtual columns description
    ASSERT_EQ(columns_desc.virtual_time_columns_calc.size(), 0);
    ASSERT_EQ(columns_desc.virtual_col_type, nullptr);

    /// Sub-columns description
    ASSERT_EQ(columns_desc.subcolumns_to_read.size(), 0);

    /// Json description
    ASSERT_EQ(columns_desc.physical_object_column_names_to_read.size(), 2);
    EXPECT_EQ(columns_desc.physical_object_column_names_to_read[0], "col5");
    EXPECT_EQ(columns_desc.physical_object_column_names_to_read[1], "col4");
}

TEST(SourceColumnsDescription, AllVirtual)
{
    auto schema = generateCommonSchema();

    DB::NamesAndTypesList columns_to_read{
        {DB::ProtonConsts::RESERVED_APPEND_TIME, getType("int64")}, {DB::ProtonConsts::RESERVED_PROCESS_TIME, getType("int64")}};
    DB::SourceColumnsDescription columns_desc(columns_to_read, schema);
    /// Pos to read
    ASSERT_EQ(columns_desc.positions.size(), 2);
    ASSERT_EQ(columns_desc.positions[0].type(), Virtual);
    EXPECT_EQ(columns_desc.positions[0].virtualPosition(), 0);
    ASSERT_EQ(columns_desc.positions[1].type(), Virtual);
    EXPECT_EQ(columns_desc.positions[1].virtualPosition(), 1);

    /// Physical columns description
    ASSERT_EQ(columns_desc.physical_column_positions_to_read.size(), 1);
    ASSERT_EQ(columns_desc.physical_column_positions_to_read[0], 6); /// Default is `_tp_time`

    /// Virtual columns description
    ASSERT_EQ(columns_desc.virtual_time_columns_calc.size(), 2);
    ASSERT_TRUE(columns_desc.virtual_col_type->equals(*getType("int64")));

    /// Sub-columns description
    ASSERT_EQ(columns_desc.subcolumns_to_read.size(), 0);

    /// Json description
    ASSERT_EQ(columns_desc.physical_object_column_names_to_read.size(), 0);
}

TEST(SourceColumnsDescription, AllSubcolumn)
{
    auto schema = generateCommonSchema();

    DB::NamesAndTypesList columns_to_read{
        {"col3", "y", getType("tuple(x int, y string)"), getType("string")},
        {"col3", "x", getType("tuple(x int, y string)"), getType("int")}};
    DB::SourceColumnsDescription columns_desc(columns_to_read, schema);
    /// Pos to read
    ASSERT_EQ(columns_desc.positions.size(), 2);
    ASSERT_EQ(columns_desc.positions[0].type(), Sub);
    EXPECT_EQ(columns_desc.positions[0].parentPosition(), 0);
    EXPECT_EQ(columns_desc.positions[0].subPosition(), 0);
    ASSERT_EQ(columns_desc.positions[1].type(), Sub);
    EXPECT_EQ(columns_desc.positions[1].parentPosition(), 0);
    EXPECT_EQ(columns_desc.positions[1].subPosition(), 1);

    /// Physical columns description
    ASSERT_EQ(columns_desc.physical_column_positions_to_read.size(), 1);
    EXPECT_EQ(columns_desc.physical_column_positions_to_read[0], 3);

    /// Virtual columns description
    ASSERT_EQ(columns_desc.virtual_time_columns_calc.size(), 0);
    ASSERT_EQ(columns_desc.virtual_col_type, nullptr);

    /// Sub-columns description
    ASSERT_EQ(columns_desc.subcolumns_to_read.size(), 2);
    EXPECT_TRUE(columns_desc.subcolumns_to_read[0].isSubcolumn());
    EXPECT_EQ(columns_desc.subcolumns_to_read[0].getNameInStorage(), "col3");
    EXPECT_EQ(columns_desc.subcolumns_to_read[0].getSubcolumnName(), "y");
    EXPECT_TRUE(columns_desc.subcolumns_to_read[0].getTypeInStorage()->equals(*getType("tuple(x int, y string)")));
    EXPECT_TRUE(columns_desc.subcolumns_to_read[0].type->equals(*getType("string")));
    EXPECT_EQ(columns_desc.subcolumns_to_read[0].name, "col3.y");
    EXPECT_TRUE(columns_desc.subcolumns_to_read[1].isSubcolumn());
    EXPECT_EQ(columns_desc.subcolumns_to_read[1].getNameInStorage(), "col3");
    EXPECT_EQ(columns_desc.subcolumns_to_read[1].getSubcolumnName(), "x");
    EXPECT_TRUE(columns_desc.subcolumns_to_read[1].getTypeInStorage()->equals(*getType("tuple(x int, y string)")));
    EXPECT_TRUE(columns_desc.subcolumns_to_read[1].type->equals(*getType("int")));
    EXPECT_EQ(columns_desc.subcolumns_to_read[1].name, "col3.x");

    /// Json description
    ASSERT_EQ(columns_desc.physical_object_column_names_to_read.size(), 0);
}

TEST(SourceColumnsDescription, AllSubcolumnWithJson)
{
    auto schema = generateCommonSchema();
    /// Contains: json column `col5`, the concrete type is `tuple(abc int, xyz string)`
    DB::NamesAndTypesList columns_to_read{
        {"col3", "y", getType("tuple(x int, y string)"), getType("string")},
        {"col5", "abc", getType("tuple(abc int, xyz string)"), getType("int")}};
    DB::SourceColumnsDescription columns_desc(columns_to_read, schema);
    /// Pos to read
    ASSERT_EQ(columns_desc.positions.size(), 2);
    ASSERT_EQ(columns_desc.positions[0].type(), Sub);
    EXPECT_EQ(columns_desc.positions[0].parentPosition(), 0);
    EXPECT_EQ(columns_desc.positions[0].subPosition(), 0);
    ASSERT_EQ(columns_desc.positions[1].type(), Sub);
    EXPECT_EQ(columns_desc.positions[1].parentPosition(), 1);
    EXPECT_EQ(columns_desc.positions[1].subPosition(), 1);

    /// Physical columns description
    ASSERT_EQ(columns_desc.physical_column_positions_to_read.size(), 2);
    EXPECT_EQ(columns_desc.physical_column_positions_to_read[0], 3);
    EXPECT_EQ(columns_desc.physical_column_positions_to_read[1], 5);

    /// Virtual columns description
    ASSERT_EQ(columns_desc.virtual_time_columns_calc.size(), 0);
    ASSERT_EQ(columns_desc.virtual_col_type, nullptr);

    /// Sub-columns description
    ASSERT_EQ(columns_desc.subcolumns_to_read.size(), 2);
    EXPECT_TRUE(columns_desc.subcolumns_to_read[0].isSubcolumn());
    EXPECT_EQ(columns_desc.subcolumns_to_read[0].getNameInStorage(), "col3");
    EXPECT_EQ(columns_desc.subcolumns_to_read[0].getSubcolumnName(), "y");
    EXPECT_TRUE(columns_desc.subcolumns_to_read[0].getTypeInStorage()->equals(*getType("tuple(x int, y string)")));
    EXPECT_TRUE(columns_desc.subcolumns_to_read[0].type->equals(*getType("string")));
    EXPECT_EQ(columns_desc.subcolumns_to_read[0].name, "col3.y");
    EXPECT_TRUE(columns_desc.subcolumns_to_read[1].isSubcolumn());
    EXPECT_EQ(columns_desc.subcolumns_to_read[1].getNameInStorage(), "col5");
    EXPECT_EQ(columns_desc.subcolumns_to_read[1].getSubcolumnName(), "abc");
    EXPECT_TRUE(columns_desc.subcolumns_to_read[1].getTypeInStorage()->equals(*getType("json")));
    EXPECT_TRUE(columns_desc.subcolumns_to_read[1].type->equals(*getType("int")));
    EXPECT_EQ(columns_desc.subcolumns_to_read[1].name, "col5.abc");

    /// Json description
    ASSERT_EQ(columns_desc.physical_object_column_names_to_read.size(), 1);
    EXPECT_EQ(columns_desc.physical_object_column_names_to_read[0], "col5");
}

TEST(SourceColumnsDescription, PhysicalAndVirtual)
{
    auto schema = generateCommonSchema();

    { /// physical + virtual
        DB::NamesAndTypesList columns_to_read{{"col1", getType("string")}, {DB::ProtonConsts::RESERVED_APPEND_TIME, getType("int64")}};
        DB::SourceColumnsDescription columns_desc(columns_to_read, schema);
        /// Pos to read
        ASSERT_EQ(columns_desc.positions.size(), 2);
        ASSERT_EQ(columns_desc.positions[0].type(), Physical);
        EXPECT_EQ(columns_desc.positions[0].physicalPosition(), 0);
        ASSERT_EQ(columns_desc.positions[1].type(), Virtual);
        EXPECT_EQ(columns_desc.positions[1].virtualPosition(), 0);

        /// Physical columns description
        ASSERT_EQ(columns_desc.physical_column_positions_to_read.size(), 1);
        EXPECT_EQ(columns_desc.physical_column_positions_to_read[0], 1);

        /// Virtual columns description
        ASSERT_EQ(columns_desc.virtual_time_columns_calc.size(), 1);
        ASSERT_TRUE(columns_desc.virtual_col_type->equals(*getType("int64")));

        /// Sub-columns description
        ASSERT_EQ(columns_desc.subcolumns_to_read.size(), 0);

        /// Json description
        ASSERT_EQ(columns_desc.physical_object_column_names_to_read.size(), 0);
    }

    { /// physical-json + virtual
        DB::NamesAndTypesList columns_to_read{{"col5", getType("json")}, {DB::ProtonConsts::RESERVED_APPEND_TIME, getType("int64")}};
        DB::SourceColumnsDescription columns_desc(columns_to_read, schema);
        /// Pos to read
        ASSERT_EQ(columns_desc.positions.size(), 2);
        ASSERT_EQ(columns_desc.positions[0].type(), Physical);
        EXPECT_EQ(columns_desc.positions[0].physicalPosition(), 0);
        ASSERT_EQ(columns_desc.positions[1].type(), Virtual);
        EXPECT_EQ(columns_desc.positions[1].virtualPosition(), 0);

        /// Physical columns description
        ASSERT_EQ(columns_desc.physical_column_positions_to_read.size(), 1);
        EXPECT_EQ(columns_desc.physical_column_positions_to_read[0], 5);

        /// Virtual columns description
        ASSERT_EQ(columns_desc.virtual_time_columns_calc.size(), 1);
        ASSERT_TRUE(columns_desc.virtual_col_type->equals(*getType("int64")));

        /// Sub-columns description
        ASSERT_EQ(columns_desc.subcolumns_to_read.size(), 0);

        /// Json description
        ASSERT_EQ(columns_desc.physical_object_column_names_to_read.size(), 1);
        EXPECT_EQ(columns_desc.physical_object_column_names_to_read[0], "col5");
    }

    { /// virtual + physical
        DB::NamesAndTypesList columns_to_read{{DB::ProtonConsts::RESERVED_APPEND_TIME, getType("int64")}, {"col1", getType("string")}};
        DB::SourceColumnsDescription columns_desc(columns_to_read, schema);
        /// Pos to read
        ASSERT_EQ(columns_desc.positions.size(), 2);
        ASSERT_EQ(columns_desc.positions[0].type(), Virtual);
        EXPECT_EQ(columns_desc.positions[0].virtualPosition(), 0);
        ASSERT_EQ(columns_desc.positions[1].type(), Physical);
        EXPECT_EQ(columns_desc.positions[1].physicalPosition(), 0);

        /// Physical columns description
        ASSERT_EQ(columns_desc.physical_column_positions_to_read.size(), 1);
        EXPECT_EQ(columns_desc.physical_column_positions_to_read[0], 1);

        /// Virtual columns description
        ASSERT_EQ(columns_desc.virtual_time_columns_calc.size(), 1);
        ASSERT_TRUE(columns_desc.virtual_col_type->equals(*getType("int64")));

        /// Sub-columns description
        ASSERT_EQ(columns_desc.subcolumns_to_read.size(), 0);

        /// Json description
        ASSERT_EQ(columns_desc.physical_object_column_names_to_read.size(), 0);
    }

    { /// virtual + physical-json
        DB::NamesAndTypesList columns_to_read{{DB::ProtonConsts::RESERVED_APPEND_TIME, getType("int64")}, {"col5", getType("json")}};
        DB::SourceColumnsDescription columns_desc(columns_to_read, schema);
        /// Pos to read
        ASSERT_EQ(columns_desc.positions.size(), 2);
        ASSERT_EQ(columns_desc.positions[0].type(), Virtual);
        EXPECT_EQ(columns_desc.positions[0].virtualPosition(), 0);
        ASSERT_EQ(columns_desc.positions[1].type(), Physical);
        EXPECT_EQ(columns_desc.positions[1].physicalPosition(), 0);

        /// Physical columns description
        ASSERT_EQ(columns_desc.physical_column_positions_to_read.size(), 1);
        EXPECT_EQ(columns_desc.physical_column_positions_to_read[0], 5);

        /// Virtual columns description
        ASSERT_EQ(columns_desc.virtual_time_columns_calc.size(), 1);
        ASSERT_TRUE(columns_desc.virtual_col_type->equals(*getType("int64")));

        /// Sub-columns description
        ASSERT_EQ(columns_desc.subcolumns_to_read.size(), 0);

        /// Json description
        ASSERT_EQ(columns_desc.physical_object_column_names_to_read.size(), 1);
        EXPECT_EQ(columns_desc.physical_object_column_names_to_read[0], "col5");
    }
}


TEST(SourceColumnsDescription, PhysicalAndSubcolumn)
{
    auto schema = generateCommonSchema();

    { /// physical + subcolumn
        DB::NamesAndTypesList columns_to_read{
            {"col1", getType("string")},
            {"col3", "y", getType("tuple(x int, y string)"), getType("string")},
        };
        DB::SourceColumnsDescription columns_desc(columns_to_read, schema);
        /// Pos to read
        ASSERT_EQ(columns_desc.positions.size(), 2);
        ASSERT_EQ(columns_desc.positions[0].type(), Physical);
        EXPECT_EQ(columns_desc.positions[0].physicalPosition(), 0);
        ASSERT_EQ(columns_desc.positions[1].type(), Sub);
        EXPECT_EQ(columns_desc.positions[1].parentPosition(), 1);
        EXPECT_EQ(columns_desc.positions[1].subPosition(), 0);

        /// Physical columns description
        ASSERT_EQ(columns_desc.physical_column_positions_to_read.size(), 2);
        EXPECT_EQ(columns_desc.physical_column_positions_to_read[0], 1);
        EXPECT_EQ(columns_desc.physical_column_positions_to_read[1], 3);

        /// Virtual columns description
        ASSERT_EQ(columns_desc.virtual_time_columns_calc.size(), 0);

        /// Sub-columns description
        ASSERT_EQ(columns_desc.subcolumns_to_read.size(), 1);
        EXPECT_TRUE(columns_desc.subcolumns_to_read[0].isSubcolumn());
        EXPECT_EQ(columns_desc.subcolumns_to_read[0].getNameInStorage(), "col3");
        EXPECT_EQ(columns_desc.subcolumns_to_read[0].getSubcolumnName(), "y");
        EXPECT_TRUE(columns_desc.subcolumns_to_read[0].getTypeInStorage()->equals(*getType("tuple(x int, y string)")));
        EXPECT_TRUE(columns_desc.subcolumns_to_read[0].type->equals(*getType("string")));
        EXPECT_EQ(columns_desc.subcolumns_to_read[0].name, "col3.y");

        /// Json description
        ASSERT_EQ(columns_desc.physical_object_column_names_to_read.size(), 0);
    }

    { /// subcolumn-json + physical-json
        DB::NamesAndTypesList columns_to_read{
            {"col5", "abc", getType("tuple(abc int, xyz string)"), getType("int")}, {"col4", getType("json")}};
        DB::SourceColumnsDescription columns_desc(columns_to_read, schema);
        /// Pos to read
        ASSERT_EQ(columns_desc.positions.size(), 2);
        ASSERT_EQ(columns_desc.positions[0].type(), Sub);
        EXPECT_EQ(columns_desc.positions[0].parentPosition(), 0);
        EXPECT_EQ(columns_desc.positions[0].subPosition(), 0);
        ASSERT_EQ(columns_desc.positions[1].type(), Physical);
        EXPECT_EQ(columns_desc.positions[1].physicalPosition(), 1);

        /// Physical columns description
        ASSERT_EQ(columns_desc.physical_column_positions_to_read.size(), 2);
        EXPECT_EQ(columns_desc.physical_column_positions_to_read[0], 5);
        EXPECT_EQ(columns_desc.physical_column_positions_to_read[1], 4);

        /// Virtual columns description
        ASSERT_EQ(columns_desc.virtual_time_columns_calc.size(), 0);

        /// Sub-columns description
        ASSERT_EQ(columns_desc.subcolumns_to_read.size(), 1);
        EXPECT_TRUE(columns_desc.subcolumns_to_read[0].isSubcolumn());
        EXPECT_EQ(columns_desc.subcolumns_to_read[0].getNameInStorage(), "col5");
        EXPECT_EQ(columns_desc.subcolumns_to_read[0].getSubcolumnName(), "abc");
        EXPECT_TRUE(columns_desc.subcolumns_to_read[0].getTypeInStorage()->equals(*getType("json")));
        EXPECT_TRUE(columns_desc.subcolumns_to_read[0].type->equals(*getType("int")));
        EXPECT_EQ(columns_desc.subcolumns_to_read[0].name, "col5.abc");

        /// Json description
        ASSERT_EQ(columns_desc.physical_object_column_names_to_read.size(), 2);
        EXPECT_EQ(columns_desc.physical_object_column_names_to_read[0], "col5");
        EXPECT_EQ(columns_desc.physical_object_column_names_to_read[1], "col4");
    }
}


TEST(SourceColumnsDescription, VirtualAndSubcolumn)
{
    auto schema = generateCommonSchema();

    { /// virtual + subcolumn
        DB::NamesAndTypesList columns_to_read{
            {DB::ProtonConsts::RESERVED_APPEND_TIME, getType("int64")},
            {"col3", "y", getType("tuple(x int, y string)"), getType("string")},
        };
        DB::SourceColumnsDescription columns_desc(columns_to_read, schema);
        /// Pos to read
        ASSERT_EQ(columns_desc.positions.size(), 2);
        ASSERT_EQ(columns_desc.positions[0].type(), Virtual);
        EXPECT_EQ(columns_desc.positions[0].virtualPosition(), 0);
        ASSERT_EQ(columns_desc.positions[1].type(), Sub);
        EXPECT_EQ(columns_desc.positions[1].parentPosition(), 0);
        EXPECT_EQ(columns_desc.positions[1].subPosition(), 0);

        /// Physical columns description
        ASSERT_EQ(columns_desc.physical_column_positions_to_read.size(), 1);
        EXPECT_EQ(columns_desc.physical_column_positions_to_read[0], 3);

        /// Virtual columns description
        ASSERT_EQ(columns_desc.virtual_time_columns_calc.size(), 1);
        ASSERT_TRUE(columns_desc.virtual_col_type->equals(*getType("int64")));

        /// Sub-columns description
        ASSERT_EQ(columns_desc.subcolumns_to_read.size(), 1);
        EXPECT_TRUE(columns_desc.subcolumns_to_read[0].isSubcolumn());
        EXPECT_EQ(columns_desc.subcolumns_to_read[0].getNameInStorage(), "col3");
        EXPECT_EQ(columns_desc.subcolumns_to_read[0].getSubcolumnName(), "y");
        EXPECT_TRUE(columns_desc.subcolumns_to_read[0].getTypeInStorage()->equals(*getType("tuple(x int, y string)")));
        EXPECT_TRUE(columns_desc.subcolumns_to_read[0].type->equals(*getType("string")));
        EXPECT_EQ(columns_desc.subcolumns_to_read[0].name, "col3.y");

        /// Json description
        ASSERT_EQ(columns_desc.physical_object_column_names_to_read.size(), 0);
    }

    { /// virtual + subcolumn-json
        DB::NamesAndTypesList columns_to_read{
            {DB::ProtonConsts::RESERVED_APPEND_TIME, getType("int64")},
            {"col5", "abc", getType("tuple(abc int, xyz string)"), getType("int")}};
        DB::SourceColumnsDescription columns_desc(columns_to_read, schema);
        /// Pos to read
        ASSERT_EQ(columns_desc.positions.size(), 2);
        ASSERT_EQ(columns_desc.positions[0].type(), Virtual);
        EXPECT_EQ(columns_desc.positions[0].virtualPosition(), 0);
        ASSERT_EQ(columns_desc.positions[1].type(), Sub);
        EXPECT_EQ(columns_desc.positions[1].parentPosition(), 0);
        EXPECT_EQ(columns_desc.positions[1].subPosition(), 0);

        /// Physical columns description
        ASSERT_EQ(columns_desc.physical_column_positions_to_read.size(), 1);
        EXPECT_EQ(columns_desc.physical_column_positions_to_read[0], 5);

        /// Virtual columns description
        ASSERT_EQ(columns_desc.virtual_time_columns_calc.size(), 1);
        ASSERT_TRUE(columns_desc.virtual_col_type->equals(*getType("int64")));

        /// Sub-columns description
        ASSERT_EQ(columns_desc.subcolumns_to_read.size(), 1);
        EXPECT_TRUE(columns_desc.subcolumns_to_read[0].isSubcolumn());
        EXPECT_EQ(columns_desc.subcolumns_to_read[0].getNameInStorage(), "col5");
        EXPECT_EQ(columns_desc.subcolumns_to_read[0].getSubcolumnName(), "abc");
        EXPECT_TRUE(columns_desc.subcolumns_to_read[0].getTypeInStorage()->equals(*getType("json")));
        EXPECT_TRUE(columns_desc.subcolumns_to_read[0].type->equals(*getType("int")));
        EXPECT_EQ(columns_desc.subcolumns_to_read[0].name, "col5.abc");

        /// Json description
        ASSERT_EQ(columns_desc.physical_object_column_names_to_read.size(), 1);
        EXPECT_EQ(columns_desc.physical_object_column_names_to_read[0], "col5");
    }
}


TEST(SourceColumnsDescription, PhysicalAndVirtualAndSubcolumn)
{
    auto schema = generateCommonSchema();

    { /// physical + virtual + subcolumn
        DB::NamesAndTypesList columns_to_read{
            {"col1", getType("string")},
            {DB::ProtonConsts::RESERVED_APPEND_TIME, getType("int64")},
            {"col3", "y", getType("tuple(x int, y string)"), getType("string")},
            {"col5", "abc", getType("tuple(abc int, xyz string)"), getType("int")}};
        DB::SourceColumnsDescription columns_desc(columns_to_read, schema);
        /// Pos to read
        ASSERT_EQ(columns_desc.positions.size(), 4);
        ASSERT_EQ(columns_desc.positions[0].type(), Physical);
        EXPECT_EQ(columns_desc.positions[0].physicalPosition(), 0);
        ASSERT_EQ(columns_desc.positions[1].type(), Virtual);
        EXPECT_EQ(columns_desc.positions[1].virtualPosition(), 0);
        ASSERT_EQ(columns_desc.positions[2].type(), Sub);
        EXPECT_EQ(columns_desc.positions[2].parentPosition(), 1);
        EXPECT_EQ(columns_desc.positions[2].subPosition(), 0);
        ASSERT_EQ(columns_desc.positions[3].type(), Sub);
        EXPECT_EQ(columns_desc.positions[3].parentPosition(), 2);
        EXPECT_EQ(columns_desc.positions[3].subPosition(), 1);

        /// Physical columns description
        ASSERT_EQ(columns_desc.physical_column_positions_to_read.size(), 3);
        EXPECT_EQ(columns_desc.physical_column_positions_to_read[0], 1);
        EXPECT_EQ(columns_desc.physical_column_positions_to_read[1], 3);
        EXPECT_EQ(columns_desc.physical_column_positions_to_read[2], 5);

        /// Virtual columns description
        ASSERT_EQ(columns_desc.virtual_time_columns_calc.size(), 1);
        ASSERT_TRUE(columns_desc.virtual_col_type->equals(*getType("int64")));

        /// Sub-columns description
        ASSERT_EQ(columns_desc.subcolumns_to_read.size(), 2);
        EXPECT_TRUE(columns_desc.subcolumns_to_read[0].isSubcolumn());
        EXPECT_EQ(columns_desc.subcolumns_to_read[0].getNameInStorage(), "col3");
        EXPECT_EQ(columns_desc.subcolumns_to_read[0].getSubcolumnName(), "y");
        EXPECT_TRUE(columns_desc.subcolumns_to_read[0].getTypeInStorage()->equals(*getType("tuple(x int, y string)")));
        EXPECT_TRUE(columns_desc.subcolumns_to_read[0].type->equals(*getType("string")));
        EXPECT_EQ(columns_desc.subcolumns_to_read[0].name, "col3.y");
        EXPECT_TRUE(columns_desc.subcolumns_to_read[1].isSubcolumn());
        EXPECT_EQ(columns_desc.subcolumns_to_read[1].getNameInStorage(), "col5");
        EXPECT_EQ(columns_desc.subcolumns_to_read[1].getSubcolumnName(), "abc");
        EXPECT_TRUE(columns_desc.subcolumns_to_read[1].getTypeInStorage()->equals(*getType("json")));
        EXPECT_TRUE(columns_desc.subcolumns_to_read[1].type->equals(*getType("int")));
        EXPECT_EQ(columns_desc.subcolumns_to_read[1].name, "col5.abc");

        /// Json description
        ASSERT_EQ(columns_desc.physical_object_column_names_to_read.size(), 1);
        EXPECT_EQ(columns_desc.physical_object_column_names_to_read[0], "col5");
    }

    { /// (complex) physical + virtual + subcolumn
        DB::NamesAndTypesList columns_to_read{
            {"col3", "y", getType("tuple(x int, y string)"), getType("string")},
            {"col1", getType("string")},
            {"col5", getType("json")},
            {"col5", "xyz", getType("tuple(abc int, xyz string)"), getType("string")},
            {"col4", getType("json")},
            {"col2", getType("tuple(a int, b int)")},
            {DB::ProtonConsts::RESERVED_APPEND_TIME, getType("int64")},
            {"col5", "abc", getType("tuple(abc int, xyz string)"), getType("int")},
            {DB::ProtonConsts::RESERVED_PROCESS_TIME, getType("int64")}};
        DB::SourceColumnsDescription columns_desc(columns_to_read, schema);
        /// Pos to read
        ASSERT_EQ(columns_desc.positions.size(), 9);
        ASSERT_EQ(columns_desc.positions[0].type(), Sub);
        EXPECT_EQ(columns_desc.positions[0].parentPosition(), 0);
        EXPECT_EQ(columns_desc.positions[0].subPosition(), 0);
        ASSERT_EQ(columns_desc.positions[1].type(), Physical);
        EXPECT_EQ(columns_desc.positions[1].physicalPosition(), 1);
        ASSERT_EQ(columns_desc.positions[2].type(), Physical);
        EXPECT_EQ(columns_desc.positions[2].physicalPosition(), 2);
        ASSERT_EQ(columns_desc.positions[3].type(), Sub);
        EXPECT_EQ(columns_desc.positions[3].parentPosition(), 2);
        EXPECT_EQ(columns_desc.positions[3].subPosition(), 1);
        ASSERT_EQ(columns_desc.positions[4].type(), Physical);
        EXPECT_EQ(columns_desc.positions[4].physicalPosition(), 3);
        ASSERT_EQ(columns_desc.positions[5].type(), Physical);
        EXPECT_EQ(columns_desc.positions[5].physicalPosition(), 4);
        ASSERT_EQ(columns_desc.positions[6].type(), Virtual);
        EXPECT_EQ(columns_desc.positions[6].virtualPosition(), 0);
        ASSERT_EQ(columns_desc.positions[7].type(), Sub);
        EXPECT_EQ(columns_desc.positions[7].parentPosition(), 2);
        EXPECT_EQ(columns_desc.positions[7].subPosition(), 2);
        ASSERT_EQ(columns_desc.positions[8].type(), Virtual);
        EXPECT_EQ(columns_desc.positions[8].virtualPosition(), 1);

        /// Physical columns description
        ASSERT_EQ(columns_desc.physical_column_positions_to_read.size(), 5);
        EXPECT_EQ(columns_desc.physical_column_positions_to_read[0], 3);
        EXPECT_EQ(columns_desc.physical_column_positions_to_read[1], 1);
        EXPECT_EQ(columns_desc.physical_column_positions_to_read[2], 5);
        EXPECT_EQ(columns_desc.physical_column_positions_to_read[3], 4);
        EXPECT_EQ(columns_desc.physical_column_positions_to_read[4], 2);

        /// Virtual columns description
        ASSERT_EQ(columns_desc.virtual_time_columns_calc.size(), 2);
        ASSERT_TRUE(columns_desc.virtual_col_type->equals(*getType("int64")));

        /// Sub-columns description
        ASSERT_EQ(columns_desc.subcolumns_to_read.size(), 3);
        EXPECT_TRUE(columns_desc.subcolumns_to_read[0].isSubcolumn());
        EXPECT_EQ(columns_desc.subcolumns_to_read[0].getNameInStorage(), "col3");
        EXPECT_EQ(columns_desc.subcolumns_to_read[0].getSubcolumnName(), "y");
        EXPECT_TRUE(columns_desc.subcolumns_to_read[0].getTypeInStorage()->equals(*getType("tuple(x int, y string)")));
        EXPECT_TRUE(columns_desc.subcolumns_to_read[0].type->equals(*getType("string")));
        EXPECT_EQ(columns_desc.subcolumns_to_read[0].name, "col3.y");
        EXPECT_TRUE(columns_desc.subcolumns_to_read[1].isSubcolumn());
        EXPECT_EQ(columns_desc.subcolumns_to_read[1].getNameInStorage(), "col5");
        EXPECT_EQ(columns_desc.subcolumns_to_read[1].getSubcolumnName(), "xyz");
        EXPECT_TRUE(columns_desc.subcolumns_to_read[1].getTypeInStorage()->equals(*getType("json")));
        EXPECT_TRUE(columns_desc.subcolumns_to_read[1].type->equals(*getType("string")));
        EXPECT_EQ(columns_desc.subcolumns_to_read[1].name, "col5.xyz");
        EXPECT_TRUE(columns_desc.subcolumns_to_read[2].isSubcolumn());
        EXPECT_EQ(columns_desc.subcolumns_to_read[2].getNameInStorage(), "col5");
        EXPECT_EQ(columns_desc.subcolumns_to_read[2].getSubcolumnName(), "abc");
        EXPECT_TRUE(columns_desc.subcolumns_to_read[2].getTypeInStorage()->equals(*getType("json")));
        EXPECT_TRUE(columns_desc.subcolumns_to_read[2].type->equals(*getType("int")));
        EXPECT_EQ(columns_desc.subcolumns_to_read[2].name, "col5.abc");

        /// Json description
        ASSERT_EQ(columns_desc.physical_object_column_names_to_read.size(), 2);
        EXPECT_EQ(columns_desc.physical_object_column_names_to_read[0], "col5");
        EXPECT_EQ(columns_desc.physical_object_column_names_to_read[1], "col4");
    }
}
