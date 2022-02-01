#pragma once

#include <NativeLog/Base/Utils.h>
#include <NativeLog/Requests/CreateTopicRequest.h>
#include <NativeLog/Schemas/TopicMetadata.h>

#include <base/ClockUtils.h>

#include <span>

namespace nlog
{
/// A wrapper for Topic flatbuffer
/// We have preserved the class method naming convention of flatbuffer
class MemoryTopicMetadata
{
public:
    /// Call this ctor if clients like MemoryTopic to own the underlying memory
    MemoryTopicMetadata(std::shared_ptr<uint8_t[]> reserved_buf_, size_t reserved_buf_size_, std::span<char> payload_)
        : reserved_buf(std::move(reserved_buf_)), reserved_buf_size(reserved_buf_size_), payload(std::move(payload_))
    {
        topic = GetTopicMetadata(payload.data());
        (void)reserved_buf_size;
    }

    /// MemoryTopic acts like a view of underlying topic object. It doesn't own its memory
    /// Some of the data members like size, reserved_buf etc are not populated
    explicit MemoryTopicMetadata(std::span<char> payload_) : payload(std::move(payload_)) { topic = GetTopicMetadata(payload.data()); }

    const flatbuffers::String & name() const { return *topic->name(); }
    uint32_t partitions() const { return topic->partitions(); }
    uint32_t replicas() const { return topic->replicas(); }
    bool compacted() const { return topic->compacted(); }
    const flatbuffers::String & ns() const { return *topic->ns(); }
    int32_t version() const { return topic->version(); }
    int64_t createTimestamp() const { return topic->create_timestamp(); }
    int64_t lastModifyTimestamp() const { return topic->last_modify_timestamp(); }
    std::string idString() const
    {
        Poco::UUID topic_id;
        topic_id.copyFrom(reinterpret_cast<const char *>(topic->id()->data()));
        return topic_id.toString();
    }

    Poco::UUID id() const
    {
        Poco::UUID topic_id;
        topic_id.copyFrom(reinterpret_cast<const char *>(topic->id()->data()));
        return topic_id;
    }

    std::span<char> data() const { return payload; }

    static MemoryTopicMetadata buildFrom(const std::string & ns, const CreateTopicRequest & req)
    {
        /// Revise initial size if Topic schema get changed to avoid further memory allocation
        flatbuffers::FlatBufferBuilder fbb(128);
        auto topic_id{uuid()};
        std::vector<int8_t> topic_id_buf(16, 0);
        topic_id.copyTo(reinterpret_cast<char *>(topic_id_buf.data()));

        auto ts = DB::UTCMilliseconds::now();

        auto topic = CreateTopicMetadata(
            fbb,
            VERSION,
            ts,
            ts,
            fbb.CreateString(ns),
            fbb.CreateString(req.name),
            fbb.CreateVector(topic_id_buf),
            req.partitions,
            req.replicas,
            req.compacted);
        FinishTopicMetadataBuffer(fbb, topic);

        std::span<char> payload(reinterpret_cast<char *>(fbb.GetBufferPointer()), fbb.GetSize());

        size_t size, offset;
        auto * data = fbb.ReleaseRaw(size, offset);

        return MemoryTopicMetadata(std::shared_ptr<uint8_t[]>(data), size, std::move(payload));
    }

private:
    const static int32_t VERSION = 1;

    /// Total reserved buf: say 1024 bytes
    std::shared_ptr<uint8_t[]> reserved_buf;
    size_t reserved_buf_size;
    /// Payload is the valid bytes of topic object, say 58 bytes out of 1024 bytes
    std::span<char> payload;
    const TopicMetadata * topic;
};
}
