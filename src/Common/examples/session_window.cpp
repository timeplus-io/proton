#include <iomanip>
#include <iostream>

#include <Interpreters/AggregationCommon.h>
#include <Interpreters/Streaming/SessionMap.h>

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

using namespace DB;

Block createHeader()
{
    Block block;
    {
        auto data_type = std::make_shared<DataTypeUInt8>();
        ColumnWithTypeAndName col_with_name{data_type, "id"};
        block.insert(std::move(col_with_name));
    }
    {
        auto data_type = std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale, String{"Asia/Shanghai"});
        ColumnWithTypeAndName col_with_name{data_type, "window_start"};
        block.insert(std::move(col_with_name));
    }
    {
        auto data_type = std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale, String{"Asia/Shanghai"});
        ColumnWithTypeAndName col_with_name{data_type, "window_end"};
        block.insert(std::move(col_with_name));
    }
    {
        auto data_type = std::make_shared<DataTypeUInt64>();
        ColumnWithTypeAndName col_with_name{data_type, "__session_id"};
        block.insert(std::move(col_with_name));
    }

    return block;
}

Block createBlock(size_t rows)
{
    Block block;
    {
        auto data_type = std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale, String{"Asia/Shanghai"});
        auto col = data_type->createColumn();
        auto * col_ptr = typeid_cast<ColumnDecimal<DateTime64> *>(col.get());

        ColumnWithTypeAndName col_with_name{std::move(col), data_type, "_tp_time"};

        DateTime64 start{0};
        for (size_t i = 0; i < rows; ++i)
            col_ptr->insert(Decimal64(start + i * 1000));

        block.insert(std::move(col_with_name));
    }
    {
        auto data_type = std::make_shared<DataTypeUInt8>();
        auto col = data_type->createColumn();
        auto * col_ptr = typeid_cast<ColumnUInt8 *>(col.get());

        ColumnWithTypeAndName col_with_name{std::move(col), data_type, "id"};

        for (size_t i = 0; i < rows; ++i)
            col_ptr->insert(UInt8(i % 5));

        block.insert(std::move(col_with_name));
    }
    {
        auto data_type = std::make_shared<DataTypeUInt64>();
        auto col = data_type->createColumn();
        auto * col_ptr = typeid_cast<ColumnUInt64 *>(col.get());

        ColumnWithTypeAndName col_with_name{std::move(col), data_type, "__session_id"};

        for (size_t i = 0; i < rows; ++i)
            col_ptr->insert(0);

        block.insert(std::move(col_with_name));
    }

    return block;
}

void printChunk(const std::vector<size_t> & chunks)
{
    std::cout << "current block is: ";
    for (const auto & it : chunks)
    {
        std::cout << it << ",";
    }
    std::cout << std::endl;
}

void printResultBlock(const Block & block)
{
    for (size_t i = 0; i < block.rows(); i++)
    {
        std::cout << block.getByPosition(0).column->getUInt(i) << ",";
        std::cout << block.getByPosition(1).column->get64(i) << ",";
        std::cout << block.getByPosition(2).column->get64(i) << ",";
        std::cout << block.getByPosition(3).column->get64(i) << std::endl;
    }
}

class SessionWatermark
{
public:
    Int64 session_size;
    Int64 interval;
    Int64 max_event_ts;
    SessionHashMap session_map;
    SessionHashMap::Type method_chosen;
    Sizes key_sizes; /// sizes of key columns
    Sizes keys;
    size_t keys_size; /// number of key columns
    size_t aggregates_size; /// number of aggregate columns
    Block src_header;

    Names argument_names;
    DataTypes argument_types;


public:
    SessionWatermark()
    {
        keys_size = 1;
        keys.push_back(1);
        argument_names.push_back("id");
        argument_types.emplace_back(std::make_shared<DataTypeUInt8>());

        aggregates_size = 3;

        method_chosen = chooseBlockCacheMethod();
        session_map.init(method_chosen);
        src_header = createHeader();
    }

    ~SessionWatermark() = default;

    Block getHeader() const { return src_header; }

    std::pair<SessionBlockQueuePtr, size_t> getSessionBlockQueue(Block & block, size_t offset)
    {
        ColumnRawPtrs key_columns(keys_size);

        assert(keys.size() == keys_size);

        /// Remember the columns we will work with
        for (size_t i = 0; i < keys_size; ++i)
            key_columns[i] = block.safeGetByPosition(keys[i]).column.get();

        #define M(NAME) \
        else if (method_chosen == SessionHashMap::Type::NAME) \
            return session_map.getSessionBlockQueue(*session_map.NAME, key_columns, block, offset, &session_map.arena);

        if (false) {} // NOLINT
            APPLY_FOR_CACHE_VARIANTS(M)
        #undef M
        __builtin_unreachable();
    }

    SessionHashMap::Type chooseBlockCacheMethod()
    {
        /// If no keys. All aggregating to single row.
        if (keys_size == 0)
            return SessionHashMap::Type::without_key;

        /// Check if at least one of the specified keys is nullable.
        DataTypes types_removed_nullable;
        types_removed_nullable.reserve(keys_size);
        bool has_nullable_key = false;
        //    bool has_low_cardinality = false;

        for (size_t i =0; i< keys.size();i++)
        {
            DataTypePtr type = argument_types[i];

            //        if (type->lowCardinality())
            //        {
            //            has_low_cardinality = true;
            //            type = removeLowCardinality(type);
            //        }

            if (type->isNullable())
            {
                has_nullable_key = true;
                type = removeNullable(type);
            }

            types_removed_nullable.push_back(type);
        }

        /** Returns ordinary (not two-level) methods, because we start from them.
      * Later, during aggregation process, data may be converted (partitioned) to two-level structure, if cardinality is high.
      */

        size_t keys_bytes = 0;
        size_t num_fixed_contiguous_keys = 0;

        key_sizes.resize(keys_size);
        for (size_t j = 0; j < keys_size; ++j)
        {
            if (types_removed_nullable[j]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
            {
                if (types_removed_nullable[j]->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
                {
                    ++num_fixed_contiguous_keys;
                    key_sizes[j] = types_removed_nullable[j]->getSizeOfValueInMemory();
                    keys_bytes += key_sizes[j];
                }
            }
        }

        if (has_nullable_key)
        {
            //            if (keys_size == num_fixed_contiguous_keys && !has_low_cardinality)
            //            {
            //                /// Pack if possible all the keys along with information about which key values are nulls
            //                /// into a fixed 16- or 32-byte blob.
            //                if (std::tuple_size<KeysNullMap<UInt128>>::value + keys_bytes <= 16)
            //                    return StreamingAggregatedDataVariants::Type::nullable_keys128;
            //                if (std::tuple_size<KeysNullMap<UInt256>>::value + keys_bytes <= 32)
            //                    return StreamingAggregatedDataVariants::Type::nullable_keys256;
            //            }
            //
            //            if (has_low_cardinality && params.keys_size == 1)
            //            {
            //                if (types_removed_nullable[0]->isValueRepresentedByNumber())
            //                {
            //                    size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();
            //
            //                    if (size_of_field == 1)
            //                        return StreamingAggregatedDataVariants::Type::low_cardinality_key8;
            //                    if (size_of_field == 2)
            //                        return StreamingAggregatedDataVariants::Type::low_cardinality_key16;
            //                    if (size_of_field == 4)
            //                        return StreamingAggregatedDataVariants::Type::low_cardinality_key32;
            //                    if (size_of_field == 8)
            //                        return StreamingAggregatedDataVariants::Type::low_cardinality_key64;
            //                }
            //                else if (isString(types_removed_nullable[0]))
            //                    return StreamingAggregatedDataVariants::Type::low_cardinality_key_string;
            //                else if (isFixedString(types_removed_nullable[0]))
            //                    return StreamingAggregatedDataVariants::Type::low_cardinality_key_fixed_string;
            //            }

            /// Fallback case.
            return SessionHashMap::Type::key_string_hash64;
        }

        /// No key has been found to be nullable.

        /// Single numeric key.
        if (keys_size == 1 && types_removed_nullable[0]->isValueRepresentedByNumber())
        {
            size_t size_of_field = types_removed_nullable[0]->getSizeOfValueInMemory();

            //        if (has_low_cardinality)
            //        {
            //            if (size_of_field == 1)
            //                return StreamingAggregatedDataVariants::Type::low_cardinality_key8;
            //            if (size_of_field == 2)
            //                return StreamingAggregatedDataVariants::Type::low_cardinality_key16;
            //            if (size_of_field == 4)
            //                return StreamingAggregatedDataVariants::Type::low_cardinality_key32;
            //            if (size_of_field == 8)
            //                return StreamingAggregatedDataVariants::Type::low_cardinality_key64;
            //        }

            if (size_of_field == 1)
                return SessionHashMap::Type::key8;
            if (size_of_field == 2)
                return SessionHashMap::Type::key16;
            if (size_of_field == 4)
                return SessionHashMap::Type::key32;
            if (size_of_field == 8)
                return SessionHashMap::Type::key64;
            if (size_of_field == 16)
                return SessionHashMap::Type::keys128;
            if (size_of_field == 32)
                return SessionHashMap::Type::keys256;
            throw Exception("Logical error: numeric column has sizeOfField not in 1, 2, 4, 8, 16, 32.", ErrorCodes::LOGICAL_ERROR);
        }

        if (keys_size == 1 && isFixedString(types_removed_nullable[0]))
        {
            //        if (has_low_cardinality)
            //            return SessionHashMap::Type::low_cardinality_key_fixed_string;
            //        else
            return SessionHashMap::Type::key_fixed_string_hash64;
        }

        /// If all keys fits in N bits, will use hash table with all keys packed (placed contiguously) to single N-bit key.
        if (keys_size == num_fixed_contiguous_keys)
        {
            //            if (has_low_cardinality)
            //            {
            //                if (keys_bytes <= 16)
            //                    return StreamingAggregatedDataVariants::Type::low_cardinality_keys128;
            //                if (keys_bytes <= 32)
            //                    return StreamingAggregatedDataVariants::Type::low_cardinality_keys256;
            //            }

            if (keys_bytes <= 2)
                return SessionHashMap::Type::key16;
            if (keys_bytes <= 4)
                return SessionHashMap::Type::key32;
            if (keys_bytes <= 8)
                return SessionHashMap::Type::key64;
            if (keys_bytes <= 16)
                return SessionHashMap::Type::keys128;
            if (keys_bytes <= 32)
                return SessionHashMap::Type::keys256;
        }

        /// If single string key - will use hash table with references to it. Strings itself are stored separately in Arena.
        if (keys_size == 1 && isString(types_removed_nullable[0]))
        {
            //        if (has_low_cardinality)
            //            return StreamingAggregatedDataVariants::Type::low_cardinality_key_string;
            //        else
            return SessionHashMap::Type::serialized;
        }

        return SessionHashMap::Type::serialized;
    }

    void updateSessionInfo(DateTime64 /*timestamp*/, SessionBlockQueue & queue)
    {
        for (auto & it : queue.block_queue)
        {
            auto tp_time = it.first;
            auto result = handleSession(tp_time, queue.session_info);
            if (result == SessionStatus::END_EXTENDED || result == SessionStatus::START_EXTENDED)
                updateSessionInfo(tp_time, queue);

            if (result == SessionStatus::IGNORE)
                std::cout << "LOGIC ERROR" << std::endl;

            if (result == SessionStatus::EMIT)
            {
                queue.removeCurrentAndLateSession(tp_time);
                auto chunks = queue.emitCurrentSession(tp_time);
                printChunk(chunks);
                updateSessionInfo(tp_time, queue);
            }
        }
    }

    template <typename Method, typename Table>
    bool NO_INLINE convertToBlockImplFinal(
        Method & method,
        Table & data,
        std::vector<IColumn *>  key_columns,
        MutableColumns & final_aggregate_columns,
        DateTime64 max_ts) const
    {
        bool emit = false;
        auto shuffled_key_sizes = method.shuffleKeyColumns(key_columns, key_sizes);
        const auto & key_sizes_ref = shuffled_key_sizes ? *shuffled_key_sizes :  key_sizes;

        std::vector<SessionBlockQueuePtr> places;
        places.reserve(data.size());

        data.forEachValue([&](const auto & key, auto & mapped)
        {
            SessionBlockQueuePtr queue = reinterpret_cast<SessionBlockQueuePtr>(mapped);
            SessionInfo & info = queue->getCurrentSessionInfo();

            if (max_ts - session_size > info.win_end)
            {
                std::cout << "emit session : (" << info.toString() << ")" << std::endl;
                queue->removeCurrentAndLateSession(max_ts);
                method.insertKeyIntoColumns(key, key_columns, key_sizes_ref);
                places.emplace_back(mapped);
                final_aggregate_columns[0]->insert(Decimal64(info.win_start));
                final_aggregate_columns[1]->insert(Decimal64(info.win_end));
                final_aggregate_columns[2]->insert(UInt64(info.cur_session_id));

                auto chunks = queue->emitCurrentSession(max_ts);
                printChunk(chunks);
                emit = true;
            }
            /// Mark the cell as destroyed so it will not be destroyed in destructor.
//            mapped = nullptr;
        });
        return emit;
    }

    void emitSessionsIfPossible(DateTime64 max_ts) const
    {
        auto emitter = [&](MutableColumns & key_columns, MutableColumns & final_aggregate_columns, DateTime64 ts)
        {
            std::vector<IColumn *> raw_key_columns;
            raw_key_columns.reserve(key_columns.size());
            for (auto & column : key_columns)
                raw_key_columns.push_back(column.get());

            #define M(NAME) \
            else if (session_map.type == SessionHashMap::Type::NAME) \
                return convertToBlockImplFinal(*session_map.NAME, session_map.NAME->data, raw_key_columns, final_aggregate_columns, ts);

            if (false) {} // NOLINT
            APPLY_FOR_CACHE_VARIANTS(M)
            #undef M
            __builtin_unreachable();
        };

        auto rows = session_map.size();
        MutableColumns key_columns(keys_size);
        MutableColumns final_aggregate_columns(aggregates_size);

        Block header = getHeader();

        for (size_t i = 0; i < keys_size; ++i)
        {
            key_columns[i] = header.safeGetByPosition(i).type->createColumn();
            key_columns[i]->reserve(rows);
        }

        for (size_t i = 0; i < aggregates_size; ++i)
        {
            final_aggregate_columns[i] = header.safeGetByPosition(i + keys_size).type->createColumn();
            final_aggregate_columns[i]->reserve(rows);
        }

        bool emit = emitter(key_columns, final_aggregate_columns, max_ts);

        if (emit)
        {
            Block res = header.cloneEmpty();

            for (size_t i = 0; i < keys_size; ++i)
                res.getByPosition(i).column = std::move(key_columns[i]);

            for (size_t i = 0; i < aggregates_size; ++i)
            {
                res.getByPosition(i + keys_size).column = std::move(final_aggregate_columns[i]);
            }

            /// Change the size of the columns-constants in the block.
            size_t columns = header.columns();
            for (size_t i = 0; i < columns; ++i)
                if (isColumnConst(*res.getByPosition(i).column))
                    res.getByPosition(i).column = res.getByPosition(i).column->cut(0, rows);

            printResultBlock(res);
        }
    }

    /// return true means the boundary of session window has been updated
    SessionStatus handleSession(const DateTime64 & tp_time, SessionInfo & info) const
    {
        assert(info.win_start <= info.win_end);
        std::cout << info.toString() << std::endl;
        if (tp_time + session_size < info.win_start)
        {
            /// late session, ignore this event
            return SessionStatus::IGNORE;
        }
        else if (tp_time + interval < info.win_start)
        {
            /// with session_size, possible late event in current session
            /// TODO: append block into possible_session_end_list
            return SessionStatus::KEEP;
        }
        else if (tp_time + interval >= info.win_start && tp_time < info.win_end)
        {
            //            session_data[offeset] = cur_session_id;
            return SessionStatus::KEEP;
        }
        else if (tp_time - interval <= info.win_end)
        {
            /// belongs to current session
            //            session_data[offeset] = cur_session_id;
            info.win_end = tp_time;
            return SessionStatus::END_EXTENDED;
        }
        else
        {
            /// possible belongs to current session, cache it
            return SessionStatus::KEEP;
        }
    }

    bool processSessionBlock(Block & block, size_t offset)
    {
        size_t time_col_pos = 0;

        const auto * time_col_ptr = typeid_cast<const ColumnDecimal<DateTime64> *>(block.getByPosition(time_col_pos).column.get());
        const auto & time_data = time_col_ptr->getData();
        auto tp_time = time_data[offset];

        if (tp_time > max_event_ts)
        {
            max_event_ts = tp_time;
            /// emit sessions if possible
            emitSessionsIfPossible(max_event_ts);
        }

        SessionBlockQueuePtr queue_ptr;
        size_t session_id;
        std::tie(queue_ptr, session_id) = getSessionBlockQueue(block, offset);
        auto & queue = *queue_ptr;
        auto & info = queue.getCurrentSessionInfo();
        bool hasSession = info.win_end != 0;
        if (!hasSession)
        {
            /// Initial session window
            info.win_start = tp_time;
            info.win_end = tp_time + interval;
            info.interval = interval;
            info.cur_session_id = 0;
            queue.appendBlock(tp_time, offset);
            return false;
        }

        switch (handleSession(tp_time, info))
        {
            case SessionStatus::IGNORE:
                return false;
            case SessionStatus::KEEP:
                queue.appendBlock(tp_time, offset);
                return false;
            case SessionStatus::END_EXTENDED:
            case SessionStatus::START_EXTENDED:
                /// TODO: check possible_session_end_list if window boundary can be updated
                updateSessionInfo(tp_time, queue);
                queue.appendBlock(tp_time, offset);
                return false;
            case SessionStatus::EMIT:
                queue.removeCurrentAndLateSession(tp_time);
                auto chunks = queue.emitCurrentSession(tp_time);
                printChunk(chunks);
                updateSessionInfo(tp_time, queue);
                queue.appendBlock(tp_time, offset);
                /// TODO: update all cached block
                return true;
        }

        return false;
    }
};

void outputBlock(const Block & block)
{
    for (size_t i = 0; i < block.rows(); i++)
    {
        std::cout << block.getByPosition(0).column->get64(i) << ",";
        std::cout << block.getByPosition(1).column->getUInt(i) << ",";
        std::cout << block.getByPosition(2).column->getUInt(i) << std::endl;
    }
}

void outputSessionInfo(const SessionWatermark & /*watermark*/)
{
//    for (auto it : watermark.session_map.session_map)
//        std::cout << it.first << ":" << it.second.getCurrentSessionInfo().toString() << ", queue size: " << it.second.block_queue.size()
//                  << std::endl;
}

int main(int, char **)
{
    int rows = 20;
    Block block = createBlock(rows);
    outputBlock(block);

    std::cout << "Start processing" << std::endl;

    SessionWatermark watermark;
    watermark.session_size = 10000;
    watermark.interval = 500;

    for (int i = 0; i < rows; i++)
    {
        std::cout << "process row " << i << std::endl;
        watermark.processSessionBlock(block, i);
        outputSessionInfo(watermark);
        std::cout << std::endl;
    }

    return 0;
}
