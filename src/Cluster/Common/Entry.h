#pragma once

#include <Cluster/Base/ByteVector.h>
#include <Cluster/Common/CallResult.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/SerdeTag.h>

namespace DB
{
class ReadBuffer;
class WriteBuffer;
}

namespace cluster
{

enum class EntryType : uint8_t
{
    Normal = 0,
    ConfChange = 1,
};

struct Entry;
using EntryPtr = std::shared_ptr<Entry>;
using EntryPtrs = std::vector<EntryPtr>;

/// Log entry which gets persisted on disk
/// TODO, make this zero serde, FlatBuffer format
SERDE struct Entry
{
    Entry() = default;
    explicit Entry(ByteVector && data_) : data(std::move(data_)) { }
    explicit Entry(std::string_view data_) : data(data_) { }
    Entry(int64_t sn_) : sn(sn_) { }
    Entry(int64_t sn_, ByteVector && data_) : sn(sn_), data(std::move(data_)) { }
    Entry(int64_t sn_, std::string_view data_) : sn(sn_), data(data_) { }

    Entry(EntryType type_, ByteVector && data_) : type(type_), data(std::move(data_)) { }

    Entry(EntryType type_, uint64_t correlation_id_, ByteVector && data_)
        : correlation_id(correlation_id_), type(type_), data(std::move(data_))
    {
    }

    Entry(EntryType type_, uint64_t correlation_id_, ByteVector && data_, int64_t max_event_timestamp_, int64_t append_timestamp_)
        : correlation_id(correlation_id_)
        , max_event_timestamp(max_event_timestamp_)
        , append_timestamp(append_timestamp_)
        , type(type_)
        , data(std::move(data_))
    {
    }

    Entry(ByteVector && data_, int64_t max_event_timestamp_) : max_event_timestamp(max_event_timestamp_), data(std::move(data_)) { }

    void serialize(DB::WriteBuffer & wb, uint16_t version_) const;
    ByteVector serialize(uint16_t version_) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t version_, bool metadata_only = false);

    /// `serializedSize` is the total serialized bytes including prefix_length itself
    size_t serializedSize() const noexcept { return prefix_length + prefixLengthSize(); }
    size_t approximateSerializedSize() const noexcept;
    size_t payloadSize() const noexcept { return data.size(); }
    std::string string() const;
    std::string typeString() const;
    void calculateCRC();
    void validateCRC() const;
    uint32_t getCRC() const noexcept { return crc; }
    EntryPtr clone() const;
    EntryPtr clone(ByteVector && new_data_) const;

    bool dataEntry() const noexcept { return type == EntryType::Normal; }
    bool dataEmpty() const noexcept { return data.empty(); }

    bool operator==(const Entry & other) const noexcept;
    bool operator!=(const Entry & other) const noexcept { return !operator==(other); }

    void setData(ByteVector && data_)
    {
        data.swap(data_);
        prefix_length = static_cast<uint32_t>(data.size());
    }

    static constexpr size_t approximateSerializedMetadataSize() noexcept
    {
        return sizeof(prefix_length) + sizeof(crc) + sizeof(version) + sizeof(correlation_id) + sizeof(max_event_timestamp)
            + sizeof(append_timestamp) + sizeof(sn) + sizeof(type);
    }

    static EntryPtr parse(std::string_view data_, uint16_t version_, bool metadata_only = false);
    static EntryPtr parse(DB::ReadBuffer & rb, uint16_t version_, bool metadata_only = false);

    /// \parseMany parses data into many entries until \param max_sn (but not included max_sn), [..., max_sn)
    /// is reached or \param data_ is drained. The input data is may contain partial entry, in
    /// this case, the trailing data wont' be parsed.
    /// \return bytes parsed / error code
    static CallResult<size_t> parseMany(std::string_view data_, uint16_t version_, int64_t max_sn, size_t max_size, EntryPtrs & entries);

private:
    static constexpr size_t prefixLengthSize() { return sizeof(prefix_length); }
    inline void deserializePrefixLength(DB::ReadBuffer & rb);
    inline void deserializeRest(DB::ReadBuffer & rb, uint16_t version_, bool metadata_only);

private:
    /// We put prefix_length and crc in Raft entry here to make zero-copy send easier
    /// but at the expense of more contextual information which are needed to go through
    /// the Raft algorithm

    /// Prefix length tells log entry boundary at storage layer
    /// It doesn't count itself in.
    mutable uint32_t prefix_length = 0;

    /// crc covers the data integrity of header and the entry body but not include prefix_length
    uint32_t crc = 0;

public:
    /// Version control of Entry structure and the first 4 data members' order can't be changed
    /// across its whole life time. We rely on this order to decode the whole structure from
    /// filesystem or network.
    uint16_t version = Nulls::NullVersion;

    uint64_t correlation_id = 0;

    /// Timestamp which are not something essential to Raft algorithm but essential to
    /// database streaming processing level
    /// These timestamps shall be setup before Raft replicate the log entries
    /// usually they are set during proposal phase
    int64_t max_event_timestamp = 0;
    int64_t append_timestamp = 0;

    /// We use a signed integer for sequence number to keep consistent with Kafka
    /// since we will need operate between these 2 in database layer.
    int64_t sn = 0;
    EntryType type = EntryType::Normal;

    /// It is application's choice to apply compression codec
    ByteVector data;

private:
    /// [0-7]
    constexpr static uint64_t VERSION_MASK = 0xFFull;
    /// [8-15]
    constexpr static uint64_t CODEC_MASK = 0xFF00ull;

    constexpr static uint64_t CODEC_OFFSET = 8ull;
    constexpr static uint64_t MAGIC_OFFSET = 55ull;
    constexpr static uint64_t MAGIC = 0x9Full;
};

inline size_t payloadSizeOfEntries(const EntryPtrs & entries)
{
    size_t size = 0;
    for (const auto & e : entries)
        size += e->payloadSize();
    return size;
}
}
