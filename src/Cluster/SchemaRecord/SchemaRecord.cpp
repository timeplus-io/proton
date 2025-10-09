#include <Cluster/SchemaRecord/SchemaContext.h>
#include <Cluster/SchemaRecord/SchemaControl.h>
#include <Cluster/SchemaRecord/SchemaNativeReader.h>
#include <Cluster/SchemaRecord/SchemaNativeWriter.h>
#include <Cluster/SchemaRecord/SchemaRecord.h>

#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Core/ProtocolDefines.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/ColumnUtils.h>
#include <Common/ProtonCommon.h>

namespace cluster
{
ByteVector SchemaRecord::serialize() const
{
    /// ByteVector data{static_cast<size_t>(approximateSerializedSize() * 1.15)};
    ByteVector data{approximateSerializedSize()};

    {
        DB::WriteBufferFromVector wb{data};

        DB::writeVarUInt(flags, wb);
        DB::writeStringBinary(idempotent_key, wb);
        DB::writeVarUInt(schema_version, wb);
        DB::writeIntBinary(std::to_underlying(block_format), wb);

        serializeData(wb);

        wb.finalize();
    }

    return data;
}

void SchemaRecord::serializeData(DB::WriteBuffer & wb) const
{
    auto compression_codec = compressionCodec();

    /// materializeBlockInplace(record.block);

    if (likely(block_format != BlockFormat::Native))
    {
        assert(hasSchemaVersion());

        /// Data
        if (compression_codec == DB::CompressionMethodByte::NONE)
        {
            SchemaNativeWriter writer(wb, column_positions);
            writer.write(block);
        }
        else
        {
            DB::CompressedWriteBuffer compressed_out(
                wb, DB::CompressionCodecFactory::instance().get(std::to_underlying(compression_codec)), DBMS_DEFAULT_BUFFER_SIZE);

            SchemaNativeWriter writer(compressed_out, column_positions);
            writer.write(block);
        }
    }
    else
    {
        /// Native block format
        /// Data
        if (likely(compression_codec == DB::CompressionMethodByte::NONE))
        {
            DB::NativeWriter writer(wb, DBMS_TCP_PROTOCOL_VERSION, DB::Block{});
            writer.write(block);
        }
        else
        {
            DB::CompressedWriteBuffer compressed_out(
                wb, DB::CompressionCodecFactory::instance().get(std::to_underlying(compression_codec)));

            DB::NativeWriter writer(compressed_out, DBMS_TCP_PROTOCOL_VERSION, DB::Block{});
            writer.write(block);
        }
    }
}

void SchemaRecord::deserialize(DB::ReadBuffer & rb, const SchemaContext & schema_ctx)
{
    DB::readVarUInt(flags, rb);
    DB::readStringBinary(idempotent_key, rb);
    DB::readVarUInt(schema_version, rb);
    std::underlying_type_t<BlockFormat> block_format_i = 0;
    DB::readIntBinary(block_format_i, rb);
    block_format = static_cast<BlockFormat>(block_format_i);

    auto opcode = opCode();
    if (likely(opcode == cluster::protocol::OpCode::InsertData))
        deserializeData(rb, schema_ctx);
    else
        block = ctrl::deserializeBlock(rb, opcode, compressionCodec(), schema_version);
}

void SchemaRecord::deserializeData(DB::ReadBuffer & rb, const SchemaContext & schema_ctx)
{
    auto compression_codec = compressionCodec();

    /// materializeBlockInplace(record.block);

    if (likely(block_format != BlockFormat::Native))
    {
        assert(hasSchemaVersion());

        bool partial = (block_format == BlockFormat::NativeInSchemaPartial);

        /// Data
        if (compression_codec == DB::CompressionMethodByte::NONE)
        {
            SchemaNativeReader reader(rb, partial, schema_version, schema_ctx);
            reader.read(block);
        }
        else
        {
            DB::CompressedReadBuffer compressed_in = DB::CompressedReadBuffer(rb);
            SchemaNativeReader reader(compressed_in, partial, schema_version, schema_ctx);
            reader.read(block);
        }
    }
    else
    {
        /// Native block format
        assert(column_positions.empty());

        if (compression_codec == DB::CompressionMethodByte::NONE)
        {
            DB::NativeReader reader(rb, DBMS_TCP_PROTOCOL_VERSION);
            block = reader.read();
        }
        else
        {
            DB::CompressedReadBuffer compressed_in = DB::CompressedReadBuffer(rb);
            DB::NativeReader reader(compressed_in, DBMS_TCP_PROTOCOL_VERSION);
            block = reader.read();
        }
    }
}

SchemaRecordPtr SchemaRecord::parse(DB::ReadBuffer & rb, const SchemaContext & schema_ctx)
{
    auto record = std::make_shared<SchemaRecord>();
    record->deserialize(rb, schema_ctx);
    return record;
}

SchemaRecordPtr SchemaRecord::parse(std::string_view data, const SchemaContext & schema_ctx)
{
    DB::ReadBufferFromString rb(data);
    return parse(rb, schema_ctx);
}

std::pair<int64_t, int64_t> SchemaRecord::minMaxEventTime() const
{
    auto * ts_col = block.findByName(DB::ProtonConsts::RESERVED_EVENT_TIME);
    if (!ts_col)
        return {0, 0};

    return columnMinMaxTimestamp(ts_col->column, ts_col->type);
}

uint64_t SchemaRecord::approximateSerializedSize() const
{
    return block.bytes() + sizeof(SchemaRecord) + stream.size() + idempotent_key.size() + column_positions.size() * sizeof(uint16_t);
}
}
