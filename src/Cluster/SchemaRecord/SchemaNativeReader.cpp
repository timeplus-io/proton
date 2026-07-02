#include <Cluster/SchemaRecord/SchemaNativeReader.h>

#include <Cluster/SchemaRecord/SchemaContext.h>
#include <Cluster/SchemaRecord/SchemaRecord.h>
#include <DataTypes/Serializations/SerializationInfoObject.h>
#include <IO/ReadHelpers.h>

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_READ_ALL_DATA;
extern const int UNSUPPORTED;
extern const int INCOMPATIBLE_SCHEMA;
}
}

namespace cluster
{
namespace
{
ALWAYS_INLINE void readData(const DB::ISerialization & serialization, DB::ColumnPtr & column, DB::ReadBuffer & istr, size_t rows)
{
    DB::ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](DB::ISerialization::SubstreamPath) -> DB::ReadBuffer * { return &istr; };
    settings.avg_value_size_hint = 0;
    settings.position_independent_encoding = false;
    settings.native_format = true;

    DB::ISerialization::DeserializeBinaryBulkStatePtr state;

    serialization.deserializeBinaryBulkStatePrefix(settings, state, nullptr);
    serialization.deserializeBinaryBulkWithMultipleStreams(column, rows, settings, state, nullptr);

    if (column->size() != rows)
        throw DB::Exception(
            DB::ErrorCodes::CANNOT_READ_ALL_DATA,
            "Cannot read all data in NativeBlockInputStream. Rows read: {}. Rows expected: {}",
            column->size(),
            rows);
}

ALWAYS_INLINE void readDataDiscard(const DB::ISerialization & serialization, DB::ReadBuffer & istr, size_t rows)
{
    DB::ISerialization::DeserializeBinaryBulkSettings settings;
    settings.getter = [&](DB::ISerialization::SubstreamPath) -> DB::ReadBuffer * { return &istr; };
    settings.avg_value_size_hint = 0;
    settings.position_independent_encoding = false;
    settings.native_format = true;

    DB::ISerialization::DeserializeBinaryBulkStatePtr state;

    serialization.deserializeBinaryBulkStatePrefix(settings, state, nullptr);
    serialization.deserializeBinaryBulkWithMultipleStreamsDiscard(rows, settings, state);
}

/// Add partial deserialized subcolumns for tuple
ALWAYS_INLINE void processPartialDeserializationInfo(
    DB::MutableSerializationInfoPtr & /*info*/, const DB::ColumnWithTypeAndName & /*column*/, const std::vector<String> & subcolumns)
{
    chassert(subcolumns.size() > 0);

    /// TODO: So far, not support subcolumns of tuple/others.
    /// Still read all subcolumns.
}
}

/// \param schema_ctx_ schema_ctx_.column_positions, does client request only partial of columns in schema
SchemaNativeReader::SchemaNativeReader(uint16_t on_disk_schema_version_, const SchemaContext & schema_ctx_)
    : on_disk_schema_version(on_disk_schema_version_), schema_ctx(schema_ctx_)
{
    initDeserializationContext();
}

void SchemaNativeReader::initDeserializationContext()
{
    /// There may be mismatch between schema appended and column positions requested
    /// When there is a mismatch, we will need check if the appended `schema_version` and the query
    /// time `schema_ctx.schema_version_requested` are `compatible`. If they are, we can still
    /// use `schema_ctx.column_positions_requested` to deserialize the data
    on_disk_schema = schema_ctx.getSchema(on_disk_schema_version);
    const auto & requested_schema = schema_ctx.getRequestedSchema();
    const auto & column_positions_requested = schema_ctx.column_positions_requested;

    if (!schema_ctx.serializableCompatible(on_disk_schema_version))
        throw DB::Exception(
            DB::ErrorCodes::INCOMPATIBLE_SCHEMA,
            "Schema mismatch, requested version({}): {}, got version({}): {}",
            schema_ctx.schema_version_requested,
            requested_schema.dumpStructure(),
            on_disk_schema_version,
            on_disk_schema.dumpStructure());

    /// There are three scenarios:
    /// 1) Request version is higher (newer) than on disk version (backfill old data in a streaming query for example)
    /// 2) Request version is lower (older) than on disk version (during streaming query, schema was mutated)
    /// 3) Request version is same as on disk version
    /// If request version is ANY_SCHEMA_VERSION, it shall cover 1) and 3) and is used by streaming store -> historical store tailing
    auto columns_num = std::max(requested_schema.columns(), on_disk_schema.columns());
    serde_ctx.infos.resize(columns_num, nullptr);
    serde_ctx.target_positions.resize(columns_num, SchemaContext::NO_POSITION);

    for (size_t pos = 0; pos < columns_num; ++pos)
    {
        auto iter = std::find(column_positions_requested.positions.begin(), column_positions_requested.positions.end(), pos);
        if (iter != column_positions_requested.positions.end())
            serde_ctx.target_positions[pos] = std::distance(column_positions_requested.positions.begin(), iter);
        else if (column_positions_requested.positions.empty() && pos < on_disk_schema.columns()) /// Only request full on disk columns
            serde_ctx.target_positions[pos] = pos;

        /// Prepare deserialization info for on disk columns
        if (pos < on_disk_schema.columns())
        {
            const auto & column = on_disk_schema.getByPosition(pos);
            auto info = column.type->createSerializationInfo({});

            auto subcolumns_iter = column_positions_requested.subcolumns.find(pos);
            if (subcolumns_iter != column_positions_requested.subcolumns.end())
                processPartialDeserializationInfo(info, column, subcolumns_iter->second);

            serde_ctx.infos[pos] = std::move(info);
        }
    }

    for (auto col_pos : column_positions_requested.positions)
        requested_header.insert(requested_schema.getByPosition(col_pos));
}

/// read guarantee that the returned block has the same column order as request if `column_positions` is
/// set in schema context. Then clients don't need sort the block any more. If `column_positions` is not set
/// the returned block has the same column sequence as the schema
void SchemaNativeReader::read(DB::Block & res, DB::ReadBuffer & istr, bool partial) const
{
    chassert(!res);

    if (istr.eof())
        return;

    /// Dimensions, note the type here for columns and rows have to match what have been
    /// used in SchemaNativeWriter
    uint16_t columns = 0;
    uint32_t rows = 0;

    DB::readIntBinary(columns, istr);
    DB::readIntBinary(rows, istr);

    chassert(rows > 0);
    chassert(columns > 0);

    if (partial)
    {
        if (schema_ctx.column_positions_requested.positions.empty())
            /// Write partial / request full, return full on disk columns
            return readPartialForRequestFull(istr, columns, rows, res);
        else
            /// Write partial / request partial, return requested columns
            return readPartialForRequestPartial(istr, columns, rows, res);
    }
    else
    {
        chassert(columns == on_disk_schema.columns());

        if (schema_ctx.column_positions_requested.positions.empty())
            /// Write full / request full, return full on disk columns
            return readFullForRequestFull(istr, rows, res);
        else
            /// Write full / request partial, return requested columns
            return readFullForRequestPartial(istr, rows, res);
    }
}

ALWAYS_INLINE void SchemaNativeReader::readFullForRequestFull(DB::ReadBuffer & istr, uint32_t rows, DB::Block & res) const
{
    chassert(schema_ctx.column_positions_requested.positions.empty());
    res = on_disk_schema.cloneEmpty(); /// Request full on disk columns

    /// We assume the order of the columns serialized has the same column order of the schema
    /// This requests that during ingestion we order the columns according to the schema. Pushing sorting
    /// to ingest stage makes sense since ingestion is more scale and can be more concurrent
    for (size_t pos = 0; auto & column : res)
    {
        chassert(serde_ctx.infos[pos]);
        auto info = serde_ctx.infos[pos]->clone();

        uint8_t has_custom;
        DB::readIntBinary(has_custom, istr);
        if (has_custom)
            info->deserializeFromKindsBinary(istr);

        auto serialization = column.type->getSerialization(*info);

        /// Data
        DB::ColumnPtr read_column = column.type->createColumn(*serialization);

        readData(*serialization, read_column, istr, rows);
        column.column = std::move(read_column);

        ++pos;
    }
}

ALWAYS_INLINE void SchemaNativeReader::readFullForRequestPartial(DB::ReadBuffer & istr, uint32_t rows, DB::Block & res) const
{
    size_t read_columns = 0;
    size_t request_column_num = requested_header.columns();
    res = requested_header.cloneEmpty();

    for (size_t pos = 0; const auto & column : on_disk_schema)
    {
        auto info = serde_ctx.infos[pos];
        chassert(info);

        uint8_t has_custom;
        readIntBinary(has_custom, istr);
        if (has_custom)
        {
            info = info->clone();
            info->deserializeFromKindsBinary(istr);
        }

        auto serialization = column.type->getSerialization(*info);

        if (auto target_pos = serde_ctx.target_positions[pos]; target_pos != SchemaContext::NO_POSITION)
        {
            /// Data
            DB::ColumnPtr read_column = column.type->createColumn(*serialization);
            readData(*serialization, read_column, istr, rows);
            res.getByPosition(target_pos).column = std::move(read_column);
            ++read_columns;
        }
        else
        {
            /// Clients like to read only some columns, discard unwanted columns
            readDataDiscard(*serialization, istr, rows);
        }

        ++pos;

        if (request_column_num == read_columns)
            /// We have collected we like to collect
            break;
    }

    /// Fill missing source columns with default values
    for (auto & column : res)
    {
        if (column.column == nullptr || column.column->empty())
            column.column = column.type->createColumn()->cloneResized(rows);
    }
}

ALWAYS_INLINE void
SchemaNativeReader::readPartialForRequestFull(DB::ReadBuffer & istr, uint16_t columns, uint32_t rows, DB::Block & res) const
{
    /// In file system, we store partial columns, but clients request all columns
    /// For those we can get the columns from file system, we deserialize them
    /// For those we cannot get the columns from file system, we create these columns with default values
    chassert(schema_ctx.column_positions_requested.positions.empty());
    res = on_disk_schema.cloneEmpty(); /// Request full on disk columns

    /// Column positions
    std::vector<uint16_t> serialized_column_positions(columns, 0);
    for (uint16_t i = 0; i < columns; ++i)
    {
        DB::readIntBinary(serialized_column_positions[i], istr);
        chassert(serialized_column_positions[i] < on_disk_schema.columns());
    }

    for (uint16_t read_columns = 0; read_columns < columns; ++read_columns)
    {
        uint16_t col_pos = serialized_column_positions[read_columns];

        chassert(col_pos < res.columns());

        auto & column = res.getByPosition(col_pos);
        chassert(serde_ctx.infos[col_pos]);
        auto info = serde_ctx.infos[col_pos]->clone();

        uint8_t has_custom;
        DB::readIntBinary(has_custom, istr);
        if (has_custom)
            info->deserializeFromKindsBinary(istr);

        auto serialization = column.type->getSerialization(*info);

        /// Data
        DB::ColumnPtr read_column = column.type->createColumn(*serialization);
        readData(*serialization, read_column, istr, rows);
        res.getByPosition(col_pos).column = std::move(read_column);
    }

    if (!schema_ctx.avoid_fill_defaults_for_missing_columns)
    {
        for (auto & column : res)
        {
            if (!column.column || column.column->empty())
                column.column = column.type->createColumn()->cloneResized(rows);
        }
    }
}

ALWAYS_INLINE void
SchemaNativeReader::readPartialForRequestPartial(DB::ReadBuffer & istr, uint16_t columns, uint32_t rows, DB::Block & res) const
{
    /// In file system, we store partial columns, and clients also request partial columns
    /// For those we can get the columns from file system, we deserialize them
    /// For those we cannot get the columns from file system, we create these columns with default values

    res = requested_header.cloneEmpty();

    /// Column positions
    std::vector<uint16_t> serialized_column_positions(columns, 0);
    for (uint16_t i = 0; i < columns; ++i)
    {
        DB::readIntBinary(serialized_column_positions[i], istr);
        chassert(serialized_column_positions[i] < on_disk_schema.columns());
    }

    for (auto col_pos : serialized_column_positions)
    {
        const auto & column = on_disk_schema.getByPosition(col_pos);

        auto info = serde_ctx.infos[col_pos];

        uint8_t has_custom;
        readIntBinary(has_custom, istr);
        if (has_custom)
        {
            info = info->clone();
            info->deserializeFromKindsBinary(istr);
        }

        auto serialization = column.type->getSerialization(*info);

        if (auto target_pos = serde_ctx.target_positions[col_pos]; target_pos != SchemaContext::NO_POSITION)
        {
            /// Data
            DB::ColumnPtr read_column = column.type->createColumn(*serialization);
            readData(*serialization, read_column, istr, rows);
            res.getByPosition(target_pos).column = std::move(read_column);
        }
        else
        {
            /// Clients like to read some columns, discard unwanted columns
            readDataDiscard(*serialization, istr, rows);
        }
    }

    /// Fill missing source columns with default values
    for (auto & column : res)
    {
        if (column.column == nullptr || column.column->empty())
            column.column = column.type->createColumn()->cloneResized(rows);
    }
}

}
