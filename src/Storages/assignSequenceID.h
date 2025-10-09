#include <Cluster/SchemaRecord/SchemaRecord.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Common/ProtonCommon.h>
#include <Common/timeScale.h>

namespace DB
{
inline void assignSequenceID(const cluster::SchemaRecordPtr & record)
{
    auto * col = record->getBlock().findByName(ProtonConsts::RESERVED_EVENT_SEQUENCE_ID);
    if (!col)
        return;

    auto * seq_id_col = typeid_cast<ColumnInt64 *>(col->column->assumeMutable().get());
    if (seq_id_col)
    {
        auto & data = seq_id_col->getData();
        for (auto & item : data)
            item = record->getSN();
    }
}

inline void assignIndexTime(ColumnWithTypeAndName * index_time_col)
{
    if (!index_time_col)
        return;

    /// index_time_col->column
    ///    = index_time_col->type->createColumnConst(moved_block.rows(), nowSubsecond(3))->convertToFullColumnIfConst();

    const auto * type = typeid_cast<const DataTypeDateTime64 *>(index_time_col->type.get());
    if (type)
    {
        auto scale = type->getScale();
        auto * col = typeid_cast<ColumnDecimal<DateTime64> *>(index_time_col->column->assumeMutable().get());
        assert(col);

        auto now = nowSubsecond(scale).get<DateTime64>();
        auto & data = col->getData();
        for (auto & item : data)
            item = now;
    }
}

}
