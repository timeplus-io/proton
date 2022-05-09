#pragma once

#include <Core/Block.h>
#include <Core/NamesAndTypes.h>

namespace DB
{

/// We calculate these column positions and lambda vector for simplify the logic and
/// fast processing in readAndProcess since we don't need index by column name any more
struct SourceColumnsDescription
{
    SourceColumnsDescription() = default;
    SourceColumnsDescription(const NamesAndTypesList & columns_to_read, const Block & schema);

    enum class ReadColumnType : uint8_t
    {
        PHYSICAL,
        VIRTUAL,
        SUB
    };

    struct ReadColumnPosition
    {
    public:
        ReadColumnPosition(ReadColumnType type_, uint16_t pos_) : ReadColumnPosition(type_, pos_, 0) { }
        ReadColumnPosition(ReadColumnType type_, uint16_t pos_, uint16_t sub_pos_) : col_type(type_), pos(pos_), sub_pos(sub_pos_) { }

        ReadColumnType type() const { return col_type; }

        uint16_t physicalPosition() const
        {
            assert(col_type == ReadColumnType::PHYSICAL);
            return pos;
        }

        uint16_t virtualPosition() const
        {
            assert(col_type == ReadColumnType::VIRTUAL);
            return pos;
        }

        uint16_t parentPosition() const
        {
            assert(col_type == ReadColumnType::SUB);
            return pos;
        }

        uint16_t subPosition() const
        {
            assert(col_type == ReadColumnType::SUB);
            return sub_pos;
        }

    private:
        ReadColumnType col_type;
        uint16_t pos;
        uint16_t sub_pos;
    };

    /// Column positions requested and returned to downstream
    std::vector<ReadColumnPosition> positions;

    /// Column positions to read from file system
    std::vector<uint16_t> physical_column_positions_to_read;

    std::vector<std::function<Int64(const BlockInfo &)>> virtual_time_columns_calc;

    /// These virtual columns have the same Int64 type
    DataTypePtr virtual_col_type;

    NamesAndTypes subcolumns_to_read;

    Names physical_object_column_names_to_read;
};

}
