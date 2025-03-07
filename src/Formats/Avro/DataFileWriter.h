#pragma once

#include "config.h"

#if USE_AVRO

#    include <boost/core/noncopyable.hpp>

#    include <DataFile.hh>
#    include <Encoder.hh>
#    include <ValidSchema.hh>

namespace DB
{

namespace Avro
{

/// This is copied from avro/DataFile.hh.
/// The problem with the original implementation is that it does not allow to add custom metadata to the data file.
class DataFileWriterBase : boost::noncopyable
{
public:
    /// Constructs a data file writer with the given sync interval and name.
    DataFileWriterBase(
        std::unique_ptr<avro::OutputStream> output_stream,
        const std::string & schema_json,
        const std::unordered_map<std::string, std::string> & metadata,
        size_t sync_interval = 16 * 1024,
        avro::Codec codec = avro::Codec::NULL_CODEC);

    /// Returns the current encoder for this writer.
    avro::Encoder & encoder() const { return *encoder_ptr_; }

    /// Returns true if the buffer has sufficient data for a sync to be inserted.
    void syncIfNeeded();

    /// Increments the object count.
    void incr() { ++object_count_; }

    ~DataFileWriterBase();
    /// Closes the current file. Once closed this datafile object cannot be used for writing any more.
    void close();

    /// Returns the schema (in JSON format) for this data file.
    const std::string & schema_json() const { return schema_json_; }

    /// Flushes any unwritten data into the file.
    void flush();

private:
    const std::string & schema_json_;
    const avro::EncoderPtr encoder_ptr_;
    const size_t sync_interval_;
    avro::Codec codec_;

    std::unique_ptr<avro::OutputStream> stream_;
    std::unique_ptr<avro::OutputStream> buffer_;
    const avro::DataFileSync sync_;
    int64_t object_count_;

    using Metadata = std::map<std::string, std::vector<uint8_t>>;

    Metadata metadata_;

    static std::unique_ptr<avro::OutputStream> makeStream(const char * filename);
    static avro::DataFileSync makeSync();

    void setMetadata(const std::string & key, const std::string & value);
    void writeHeader();

    /// Generates a sync marker in the file.
    void sync();
};

}

}

#endif
