#include <Cluster/Common/serde.h>
#include <Cluster/Common/Entry.h>

namespace DB::ErrorCodes
{
extern const int CORRUPTED_DATA;
extern const int OK;
}

namespace cluster
{
void Entry::serialize(DB::WriteBuffer & wb, uint16_t /*version_*/) const
{
    /// We don't use var encoding for prefix length since we may need fix
    /// it later after serializing all other data members
    DB::writeIntBinary(prefix_length, wb);
    DB::writeVarUInt(crc, wb);
    DB::writeVarUInt(version, wb);

    DB::writeVarUInt(correlation_id, wb);
    DB::writeVarInt(max_event_timestamp, wb);
    DB::writeVarInt(append_timestamp, wb);

    DB::writeVarInt(sn, wb);

    cluster::serializeEnum(type, wb);

    DB::writeVarUInt(data.size(), wb);
    wb.write(data.data(), data.size());
}

void Entry::deserialize(DB::ReadBuffer & rb, uint16_t version_, bool metadata_only)
{
    deserializePrefixLength(rb);
    deserializeRest(rb, version_, metadata_only);
}

void Entry::deserializePrefixLength(DB::ReadBuffer & rb)
{
    assert(prefix_length == 0);

    DB::readIntBinary(prefix_length, rb);
}

void Entry::deserializeRest(DB::ReadBuffer & rb, uint16_t /*version_*/, bool metadata_only)
{
    DB::readVarUInt(crc, rb);
    DB::readVarUInt(version, rb);

    DB::readVarUInt(correlation_id, rb);
    DB::readVarInt(max_event_timestamp, rb);
    DB::readVarInt(append_timestamp, rb);

    DB::readVarInt(sn, rb);

    type = cluster::deserializeEnum<EntryType>(rb);

    if (!metadata_only)
    {
        uint64_t size = 0;
        DB::readVarUInt(size, rb);

        ByteVector tmp{size};
        rb.readStrict(tmp.data(), size);

        data.swap(tmp);
    }
}

ByteVector Entry::serialize(uint16_t version_) const
{
    auto bytes = cluster::serialize<ByteVector>(*this, version_);

    /// Re-calculate and re-write the prefix length. Memory copy is the actual implementation of DB::writeIntBinary
    /// so borrow the logic here. Please this is not cross CPU arch compatible
    prefix_length = static_cast<uint32_t>(bytes.size() - sizeof(prefix_length));
    bytes.prefixWithLength(prefix_length);

    return bytes;
}

EntryPtr Entry::parse(std::string_view data_, uint16_t version_, bool metadata_only)
{
    DB::ReadBufferFromString rb(data_);
    return parse(rb, version_, metadata_only);
}

EntryPtr Entry::parse(DB::ReadBuffer & rb, uint16_t version_, bool metadata_only)
{
    auto entry = std::make_shared<Entry>();
    entry->deserialize(rb, version_, metadata_only);
    return entry;
}

/// [..., max_sn)
CallResult<size_t> Entry::parseMany(std::string_view data_, uint16_t version_, int64_t max_sn, size_t max_size, EntryPtrs & entries)
{
    assert(max_sn > 0);

    size_t data_size = data_.size();
    size_t parsed_bytes = 0;

    DB::ReadBufferFromString rb(data_);

    try
    {
        while (parsed_bytes + prefixLengthSize() < data_size)
        {
            auto entry = std::make_shared<Entry>();
            /// First parse the prefix length and then check if we have enough data to parse
            entry->deserializePrefixLength(rb);
            if (parsed_bytes + entry->serializedSize() > data_size)
                /// Partial log entry
                break;

            entry->deserializeRest(rb, version_, version_);

            if (entry->sn < max_sn)
            {
                parsed_bytes += entry->serializedSize();
                entries.push_back(std::move(entry));

                if (parsed_bytes >= max_size)
                    break;
            }
            else
                break;
        }
    }
    catch (const DB::Exception & e)
    {
        return CallResult{parsed_bytes, e.code()};
    }
    catch (...)
    {
        return CallResult{parsed_bytes, DB::ErrorCodes::CORRUPTED_DATA};
    }

    return CallResult{parsed_bytes, DB::ErrorCodes::OK};
}

void Entry::calculateCRC()
{
    if (crc != 0)
        return;
}

void Entry::validateCRC() const
{
}

size_t Entry::approximateSerializedSize() const noexcept
{
    return sizeof(Entry) + data.size();
}

bool Entry::operator==(const Entry & other) const noexcept
{
    return version == other.version && correlation_id == other.correlation_id && max_event_timestamp == other.max_event_timestamp
        && append_timestamp == other.append_timestamp && sn == other.sn && type == other.type
        && data.stringView() == other.data.stringView();
}

EntryPtr Entry::clone() const
{
    auto entry = std::make_shared<Entry>(data.clone());
    entry->prefix_length = prefix_length;
    entry->crc = crc;
    entry->version = version;
    entry->correlation_id = correlation_id;
    entry->max_event_timestamp = max_event_timestamp;
    entry->append_timestamp = append_timestamp;
    entry->sn = sn;
    entry->type = type;
    return entry;
}

EntryPtr Entry::clone(ByteVector && new_data) const
{
    auto entry = std::make_shared<Entry>(std::move(new_data));
    entry->prefix_length = static_cast<uint32_t>(new_data.size());
    entry->crc = 0;
    entry->version = version;
    entry->correlation_id = correlation_id;
    entry->max_event_timestamp = max_event_timestamp;
    entry->append_timestamp = append_timestamp;
    entry->sn = sn;
    entry->type = type;
    return entry;
}

std::string Entry::typeString() const
{
    return fmt::format("{}", magic_enum::enum_name(type));
}

std::string Entry::string() const
{
    return fmt::format(
        "prefix_length={} crc={} version={} correlation_id=0x{:x} max_event_timestamp={} append_timestamp={} sn={} type={} "
        "bytes={}",
        prefix_length,
        crc,
        version,
        correlation_id,
        max_event_timestamp,
        append_timestamp,
        sn,
        magic_enum::enum_name(type),
        data.size());
}
}
