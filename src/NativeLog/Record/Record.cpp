#include "Record.h"
#include "SchemaNativeReader.h"
#include "SchemaNativeWriter.h"

#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <Core/ProtocolDefines.h>
#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/ColumnUtils.h>
#include <Common/ProtonCommon.h>

namespace nlog
{
ByteVector Record::serialize() const
{
    ByteVector data{static_cast<size_t>(ballparkSize() * 1.05)};
    /// Do an initial resize to avoid resize during the serialization
    data.resize(data.capacity());

    DB::WriteBufferFromVector wb{data};

    /// Write common meta fields which always have the following order

    /// Write prefix length, we will revise it after serializing everything
    DB::writeIntBinary(prefix_length, wb);

    /// Write crc
    DB::writeIntBinary(crc, wb);

    /// Write flags
    DB::writeIntBinary(flags, wb);

    /// Write sn, we don't want to write var int since we like the offset fixed
    DB::writeIntBinary(sn, wb);

    /// Write append time
    DB::writeIntBinary(block.info.appendTime(), wb);

    /// Write schema version
    DB::writeIntBinary(schema_version, wb);

    /// Write block format
    DB::writeIntBinary(static_cast<uint8_t>(block_format), wb);

    /// FIXME, in future, we will need check `if (version() == Version::V0)`
    serializeMetadataV0(wb);

    serializeData(wb);

    /// Shrink to what has been written
    wb.finalize();

    /// Re-write the prefix length. Memory copy is the actual implementation of DB::writeIntBinary
    /// so borrow the logic here. Please note this is not CPU architecture compatible
    prefix_length = data.size() - sizeof(prefix_length);
    memcpy(data.data(), &prefix_length, sizeof(prefix_length));

    return data;
}

void Record::serializeData(DB::WriteBuffer & wb) const
{
    if (likely(block_format != BlockFormat::NATIVE))
    {
        serializeInSchemaV0(codec(), wb);
    }
    else
    {
        /// Native block format
        assert(column_positions.empty());

        /// Data
        /// materializeBlockInplace(record.block);
        auto compression_codec = codec();
        if (likely(compression_codec == DB::CompressionMethodByte::NONE))
        {
            DB::NativeWriter writer(wb, DB::Block{}, DBMS_TCP_PROTOCOL_VERSION);
            writer.write(block);
        }
        else
        {
            DB::CompressedWriteBuffer compressed_out = DB::CompressedWriteBuffer(
                wb, DB::CompressionCodecFactory::instance().get(static_cast<uint8_t>(compression_codec)));
            DB::NativeWriter writer(compressed_out, DB::Block{}, DBMS_TCP_PROTOCOL_VERSION);
            writer.write(block);
        }
    }
}

size_t Record::deserialize(const char * data, size_t size, RecordPtrs & records, const SchemaContext & schema_ctx)
{
    size_t deserialized = 0;
    while (deserialized < size)
    {
        auto record{deserialize(data + deserialized, size - deserialized, schema_ctx)};
        if (record)
        {
            deserialized += record->totalSerializedBytes();
            records.push_back(std::move(record));
        }
        else
            break;
    }

    return deserialized;
}

RecordPtr Record::deserialize(const char * data, size_t size, const SchemaContext & schema_ctx)
{
    DB::ReadBufferFromMemory rb{data, size};

    if (unlikely(commonMetadataBytes() > size))
        /// Partial record
        return nullptr;

    auto record = doDeserializeCommonMetadata(rb);
    if (unlikely(record->totalSerializedBytes() > size))
        /// Partial record
        return nullptr;

    /// FIXME, in future, we will need check `if (version() == Version::V0)`
    deserializeMetadataV0(*record, rb);

    if (likely(record->block_format != BlockFormat::NATIVE))
    {
        deserializeInSchemaDataV0(*record, rb, schema_ctx);
    }
    else
    {
        if (likely(record->codec() == DB::CompressionMethodByte::NONE))
        {
            DB::NativeReader reader(rb, DBMS_TCP_PROTOCOL_VERSION);
            record->setBlock(reader.read());
        }
        else
        {
            DB::CompressedReadBuffer compressed_in = DB::CompressedReadBuffer(rb);
            DB::NativeReader reader(compressed_in, DBMS_TCP_PROTOCOL_VERSION);
            record->setBlock(reader.read());
        }
    }

    return record;
}

RecordPtr Record::deserializeCommonMetadata(const char * data, size_t size)
{
    DB::ReadBufferFromMemory rb{data, size};
    return doDeserializeCommonMetadata(rb);
}

RecordPtr Record::doDeserializeCommonMetadata(DB::ReadBuffer & rb)
{
    auto record = std::make_shared<Record>(-1);

    /// deserialize fixed common metadata. The order of these metadata fields
    /// has is fixed across different record version
    DB::readIntBinary(record->prefix_length, rb);
    DB::readIntBinary(record->crc, rb);
    DB::readIntBinary(record->flags, rb);
    assert(record->magic() == record->MAGIC);

    /// Read sn
    DB::readIntBinary(record->sn, rb);

    /// Read append time
    Int64 append_time;
    DB::readIntBinary(append_time, rb);
    record->setAppendTime(append_time);

    /// Read schema version
    DB::readIntBinary(record->schema_version, rb);

    /// Read block format
    uint8_t block_format = 0;
    DB::readIntBinary(block_format, rb);
    record->block_format = static_cast<BlockFormat>(block_format);

    return record;
}

void Record::serializeInSchemaV0(DB::CompressionMethodByte codec, DB::WriteBuffer & wb) const
{
    assert(hasSchema());

    /// Data
    /// materializeBlockInplace(record.block);
    if (likely(codec == DB::CompressionMethodByte::NONE))
    {
        SchemaNativeWriter writer(wb, column_positions);
        writer.write(block);
    }
    else
    {
        DB::CompressedWriteBuffer compressed_out = DB::CompressedWriteBuffer(
            wb, DB::CompressionCodecFactory::instance().get(static_cast<uint8_t>(codec)), DBMS_DEFAULT_BUFFER_SIZE);
        SchemaNativeWriter writer(compressed_out, column_positions);
        writer.write(block);
    }
}

void Record::deserializeInSchemaDataV0(Record & record, DB::ReadBuffer & rb, const SchemaContext & schema_ctx)
{
    bool partial = (record.block_format == BlockFormat::NATIVE_IN_SCHEMA_PARTIAL);

    if (likely(record.codec() == DB::CompressionMethodByte::NONE))
    {
        SchemaNativeReader reader(rb, partial, record.schema_version, schema_ctx);
        reader.read(record.block);
    }
    else
    {
        DB::CompressedReadBuffer compressed_in = DB::CompressedReadBuffer(rb);
        SchemaNativeReader reader(compressed_in, partial, record.schema_version, schema_ctx);
        reader.read(record.block);
    }
}

std::pair<int64_t, int64_t> Record::minMaxEventTime() const
{
    auto * ts_col = block.findByName(DB::ProtonConsts::RESERVED_EVENT_TIME);
    if (!ts_col)
        return {0, 0};

    return columnMinMaxTimestamp(ts_col->column, ts_col->type);
}

void Record::serializeMetadataV0(DB::WriteBuffer & wb) const
{
    /// Write key. We like to write key as soon as possible since it is faster for deserialization during dedup
    DB::writeStringBinary(key, wb);

    /// Write headers
    DB::writeVarUInt(headers.size(), wb);

    for (const auto & kv : headers)
    {
        DB::writeStringBinary(kv.first, wb);
        DB::writeStringBinary(kv.second, wb);
    }
}

void Record::deserializeMetadataV0(Record & record, DB::ReadBuffer & rb)
{
    /// Read key
    DB::readStringBinary(record.key, rb);

    /// Read headers
    size_t header_size = 0;
    DB::readVarUInt(header_size, rb);

    for (size_t i = 0; i < header_size; ++i)
    {
        std::string key, value;
        DB::readStringBinary(key, rb);
        DB::readStringBinary(value, rb);
        record.headers.emplace(std::move(key), std::move(value));
    }
}

uint32_t Record::ballparkSize() const
{
    if (ballpark_size)
        return ballpark_size;

    size_t metadata_size = commonMetadataBytes() + sizeof(column_positions[0]) * column_positions.size();

    metadata_size += sizeof(headers.size()) + sizeof(key.size()) + key.size();
    for (const auto & kv : headers)
    {
        metadata_size += sizeof(kv.first.size());
        metadata_size += kv.first.size();
        metadata_size += sizeof(kv.second.size());
        metadata_size += kv.second.size();
    }

    ballpark_size = block.bytes() + metadata_size;
    return ballpark_size;
}

void Record::serializeSN(ByteVector & byte_vec) const
{
    assert(byte_vec.size() >= commonMetadataBytes());

    auto sliced{byte_vec.slice(sizeof(prefix_length) + sizeof(crc) + sizeof(flags), sizeof(sn))};

    DB::WriteBufferFromVector wb{sliced};

    DB::writeIntBinary(sn, wb);
}

void Record::serializeCRC(ByteVector &) const
{
}

void Record::deltaSerialize(ByteVector & byte_vec) const
{
    assert(byte_vec.size() >= commonMetadataBytes());

    auto sliced{byte_vec.slice(sizeof(prefix_length) + sizeof(crc) + sizeof(flags), sizeof(sn) + sizeof(block.info.appendTime()))};

    DB::WriteBufferFromVector wb{sliced};
    DB::writeIntBinary(sn, wb);
    DB::writeIntBinary(block.info.appendTime(), wb);

    /// FIXME, recalculate CRC
}
}
