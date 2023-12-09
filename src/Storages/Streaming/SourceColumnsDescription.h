#pragma once

#include <Core/NamesAndTypes.h>

namespace nlog
{
struct Record;
using RecordPtr = std::shared_ptr<Record>;
}

namespace DB
{

class Block;

struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

/// We calculate these column positions and lambda vector for simplify the logic and
/// fast processing in readAndProcess since we don't need index by column name any more
struct SourceColumnsDescription
{
    SourceColumnsDescription() = default;
    SourceColumnsDescription(const Names & required_column_names, StorageSnapshotPtr storage_snapshot, bool enable_partial_read = true);
    SourceColumnsDescription(
        const NamesAndTypesList & columns_to_read,
        const Block & schema,
        const NamesAndTypesList & all_extended_columns,
        bool enable_partial_read = true);

    enum class ReadColumnType : uint8_t
    {
        PHYSICAL,
        VIRTUAL,
        SUB
    };

    struct PhysicalColumnPositions
    {
        std::vector<uint16_t> positions;
        std::unordered_map<uint16_t, std::vector<String>> subcolumns; /// Only json / tuple column will have an entry in subcolumns map

        PhysicalColumnPositions() = default;
        explicit PhysicalColumnPositions(std::initializer_list<uint16_t> positions_) : positions(std::move(positions_)) {}
        explicit PhysicalColumnPositions(std::vector<uint16_t> positions_) : positions(std::move(positions_)) {}
        PhysicalColumnPositions & operator=(std::initializer_list<uint16_t> positions_);
        PhysicalColumnPositions & operator=(const std::vector<uint16_t> & positions_);

        void clear();
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
    /// For some physical column positions, we only marked those partial subcolumns to read
    /// <Column position, <is_all_read, subcolumns_to_read> >
    PhysicalColumnPositions physical_column_positions_to_read;

    std::vector<std::function<Int64(const nlog::RecordPtr &)>> virtual_col_calcs;
    std::vector<DataTypePtr> virtual_col_types;

    NamesAndTypes subcolumns_to_read;

    /// We like to use extended object type instead of original json type
    NamesAndTypesList physical_object_columns_to_read;
};

}
