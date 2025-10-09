#pragma once

#include <base/types.h>

#include <unordered_map>
#include <vector>

/// proton: starts.
#include <Cluster/Common/Constants.h>
#include <Core/Streaming/Watermark.h>
#include <base/defines.h>
#include <Common/ProtonCommon.h>
/// proton: ends.

namespace DB
{

class ReadBuffer;
class WriteBuffer;

/** More information about the block.
  */
struct BlockInfo
{
    /** is_overflows:
      * After running GROUP BY ... WITH TOTALS with the max_rows_to_group_by and group_by_overflow_mode = 'any' settings,
      *  a row is inserted in the separate block with aggregated values that have not passed max_rows_to_group_by.
      * If it is such a block, then is_overflows is set to true for it.
      */

    /** bucket_num:
      * When using the two-level aggregation method, data with different key groups are scattered across different buckets.
      * In this case, the bucket number is indicated here. It is used to optimize the merge for distributed aggregation.
      * Otherwise -1.
      */

#define APPLY_FOR_BLOCK_INFO_FIELDS(M) \
    M(bool, is_overflows, false, 1) \
    M(Int32, bucket_num, -1, 2)

    /// Internal information are only used in cluster

    /** any_field_1:
    * Here we try to reuse existing data members for different purposes since they work at different stage, it shall be fine
    * - \watermark (Main): used to indicate the watermark of the block. It's used in the streaming scenario for now.
    * - \max_ts: Only used in internal cached joined block
    * - \append_time: Only used in source stage before watermark is set
    */
    union AnyField1
    {
        Int64 watermark;
        Int64 max_ts;
        Int64 append_time;
        bool operator==(const AnyField1 & rhs) const { return watermark == rhs.watermark; }
    };

    /** any_field_2:
    * Here we try to reuse existing data members for different purposes since they work at different stage, it shall be fine
    * - \sn (Main): used to identify current processed progress. It's used in remote fetch columns scenario for now.
    * - \min_ts: Only used in internal cached joined block
    */
    union AnyField2
    {
        Int64 sn;
        Int64 min_ts;
        bool retract;
        bool operator==(const AnyField2 & rhs) const { return sn == rhs.sn; }
    };

#define APPLY_FOR_BLOCK_INFO_FIELDS_INTERNAL(M) \
    M(UInt64, flags, 0, 100) \
    M(AnyField1, any_field_1, AnyField1{}, 101) \
    M(AnyField2, any_field_2, AnyField2{}, 102)

#define DECLARE_FIELD(TYPE, NAME, DEFAULT, FIELD_NUM) TYPE NAME = DEFAULT;

    APPLY_FOR_BLOCK_INFO_FIELDS(DECLARE_FIELD)

private:
    APPLY_FOR_BLOCK_INFO_FIELDS_INTERNAL(DECLARE_FIELD)

#undef DECLARE_FIELD

public:
    bool hasBucketNum() const { return bucket_num >= 0; }
    bool hasOverflows() const { return is_overflows; }

    /// proton: starts
    Int32 bucketNum() const { return bucket_num; }

    ALWAYS_INLINE bool hasFlags() const noexcept { return flags != 0; }

    ALWAYS_INLINE bool hasWatermark() const noexcept { return flags & ProtonConsts::WATERMARK_FLAG; }
    ALWAYS_INLINE void setWatermark(Int64 watermark)
    {
        chassert(watermark != Streaming::INVALID_WATERMARK);
        flags |= ProtonConsts::WATERMARK_FLAG;
        any_field_1.watermark = watermark;
    }
    ALWAYS_INLINE Int64 watermark() const
    {
        chassert(hasWatermark());
        return any_field_1.watermark;
    }

    ALWAYS_INLINE bool hasMaxTimestamp() const noexcept { return flags & ProtonConsts::MAX_TIMESTAMP_FLAG; }
    ALWAYS_INLINE void setMaxTimestamp(Int64 max_ts)
    {
        flags |= ProtonConsts::MAX_TIMESTAMP_FLAG;
        any_field_1.max_ts = max_ts;
    }
    ALWAYS_INLINE Int64 maxTimestamp() const
    {
        chassert(hasMinTimestamp());
        return any_field_1.max_ts;
    }

    ALWAYS_INLINE bool hasMinTimestamp() const noexcept { return flags & ProtonConsts::MIN_TIMESTAMP_FLAG; }
    ALWAYS_INLINE void setMinTimestamp(Int64 min_ts)
    {
        flags |= ProtonConsts::MIN_TIMESTAMP_FLAG;
        any_field_2.min_ts = min_ts;
    }
    ALWAYS_INLINE Int64 minTimestamp() const
    {
        chassert(hasMinTimestamp());
        return any_field_2.min_ts;
    }

    ALWAYS_INLINE bool hasAppendTime() const noexcept { return flags & ProtonConsts::APPEND_TIME_FLAG; }
    ALWAYS_INLINE void setAppendTime(Int64 append_time)
    {
        flags |= ProtonConsts::APPEND_TIME_FLAG;
        any_field_1.append_time = append_time;
    }
    ALWAYS_INLINE Int64 appendTime() const
    {
        chassert(hasAppendTime());
        return any_field_1.append_time;
    }

    ALWAYS_INLINE bool hasSN() const noexcept { return flags & ProtonConsts::SEQUENCE_NUMBER_FLAG; }
    ALWAYS_INLINE void setSN(Int64 sn)
    {
        chassert(sn >= cluster::Constants::LogStartSN);
        flags |= ProtonConsts::SEQUENCE_NUMBER_FLAG;
        any_field_2.sn = sn;
    }
    ALWAYS_INLINE Int64 getSN() const
    {
        chassert(hasSN());
        return any_field_2.sn;
    }

    ALWAYS_INLINE void setRetract() noexcept { flags |= ProtonConsts::RETRACT_FLAG; }
    ALWAYS_INLINE bool isRetract() const noexcept { return flags & ProtonConsts::RETRACT_FLAG; }
    /// proton: ends

    /// Write the values in binary form. NOTE: You could use protobuf, but it would be overkill for this case.
    void write(WriteBuffer & out, bool additional_internal_fields = false) const;

    /// Read the values in binary form.
    void read(ReadBuffer & in);

    inline bool operator==(const BlockInfo & rhs) const
    {
        return
    #define COMPARE_FIELD(TYPE, NAME, DEFAULT, FIELD_NUM) NAME == rhs.NAME &&

            APPLY_FOR_BLOCK_INFO_FIELDS(COMPARE_FIELD)
            APPLY_FOR_BLOCK_INFO_FIELDS_INTERNAL(COMPARE_FIELD)

    #undef COMPARE_FIELD
                true;
    }
};

/// Block extension to support delayed defaults. AddingDefaultsTransform uses it to replace missing values with column defaults.
class BlockMissingValues
{
public:
    using RowsBitMask = std::vector<bool>; /// a bit per row for a column

    /// Get mask for column, column_idx is index inside corresponding block
    const RowsBitMask & getDefaultsBitmask(size_t column_idx) const;
    /// Check that we have to replace default value at least in one of columns
    bool hasDefaultBits(size_t column_idx) const;
    /// Set bit for a specified row in a single column.
    void setBit(size_t column_idx, size_t row_idx);
    /// Set bits for all rows in a single column.
    void setBits(size_t column_idx, size_t rows);
    bool empty() const { return rows_mask_by_column_id.empty(); }
    size_t size() const { return rows_mask_by_column_id.size(); }
    void clear() { rows_mask_by_column_id.clear(); }

private:
    using RowsMaskByColumnId = std::unordered_map<size_t, RowsBitMask>;

    /// If rows_mask_by_column_id[column_id][row_id] is true related value in Block should be replaced with column default.
    /// It could contain less columns and rows then related block.
    RowsMaskByColumnId rows_mask_by_column_id;
};

}
