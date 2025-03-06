#include <Formats/Avro/DataFileWriter.h>

#if USE_AVRO

#    include <Common/Exception.h>

#    include <boost/crc.hpp> // for boost::crc_32_type
#    include <boost/iostreams/device/back_inserter.hpp>
#    include <boost/iostreams/filter/zlib.hpp>
#    include <boost/random/mersenne_twister.hpp>

#    ifdef SNAPPY_CODEC_AVAILABLE
#        include <snappy.h>
#    endif

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace Avro
{

namespace
{
const std::string AVRO_SCHEMA_KEY("avro.schema");
const std::string AVRO_CODEC_KEY("avro.codec");
const std::string AVRO_NULL_CODEC("null");
const std::string AVRO_DEFLATE_CODEC("deflate");

#    ifdef SNAPPY_CODEC_AVAILABLE
const std::string AVRO_SNAPPY_CODEC = "snappy";
#    endif

const size_t min_sync_interval = 32;
const size_t max_sync_interval = 1u << 30;

boost::iostreams::zlib_params getZlibParams()
{
    boost::iostreams::zlib_params ret;
    ret.method = boost::iostreams::zlib::deflated;
    ret.noheader = true;
    return ret;
}
}

DataFileWriterBase::DataFileWriterBase(
    std::unique_ptr<avro::OutputStream> output_stream,
    const std::string & schema_json,
    const std::unordered_map<std::string, std::string> & metadata,
    size_t sync_interval,
    avro::Codec codec)
    : schema_json_(schema_json)
    , encoder_ptr_(avro::binaryEncoder())
    , sync_interval_(sync_interval)
    , codec_(codec)
    , stream_(std::move(output_stream))
    , buffer_(avro::memoryOutputStream())
    , sync_(makeSync())
    , object_count_(0)
{
    if (sync_interval < min_sync_interval || sync_interval > max_sync_interval)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid sync interval: {}. Should be between {} and {}",
            sync_interval,
            min_sync_interval,
            max_sync_interval);
    }

    for (const auto & [key, value] : metadata)
        setMetadata(key, value);

    setMetadata(AVRO_CODEC_KEY, AVRO_NULL_CODEC);

    if (codec_ == avro::Codec::NULL_CODEC)
    {
        setMetadata(AVRO_CODEC_KEY, AVRO_NULL_CODEC);
    }
    else if (codec_ == avro::Codec::DEFLATE_CODEC)
    {
        setMetadata(AVRO_CODEC_KEY, AVRO_DEFLATE_CODEC);
#    ifdef SNAPPY_CODEC_AVAILABLE
    }
    else if (codec_ == avro::Codec::SNAPPY_CODEC)
    {
        setMetadata(AVRO_CODEC_KEY, AVRO_SNAPPY_CODEC);
#    endif
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown codec: {}", codec);
    }
    setMetadata(AVRO_SCHEMA_KEY, schema_json);

    writeHeader();
    encoder_ptr_->init(*buffer_);
}


DataFileWriterBase::~DataFileWriterBase()
{
    if (stream_)
    {
        try
        {
            close();
        }
        catch (...)
        {
        }
    }
}

void DataFileWriterBase::close()
{
    flush();
    stream_.reset();
}

void DataFileWriterBase::sync()
{
    encoder_ptr_->flush();

    encoder_ptr_->init(*stream_);
    avro::encode(*encoder_ptr_, object_count_);
    if (codec_ == avro::Codec::NULL_CODEC)
    {
        int64_t byte_count = buffer_->byteCount();
        avro::encode(*encoder_ptr_, byte_count);
        encoder_ptr_->flush();
        std::unique_ptr<avro::InputStream> in = memoryInputStream(*buffer_);
        copy(*in, *stream_);
    }
    else if (codec_ == avro::Codec::DEFLATE_CODEC)
    {
        std::vector<char> buf;
        {
            boost::iostreams::filtering_ostream os;
            os.push(boost::iostreams::zlib_compressor(getZlibParams()));
            os.push(boost::iostreams::back_inserter(buf));
            const uint8_t * data;
            size_t len;

            std::unique_ptr<avro::InputStream> input = memoryInputStream(*buffer_);
            while (input->next(&data, &len))
            {
                boost::iostreams::write(os, reinterpret_cast<const char *>(data), len);
            }
        } // make sure all is flushed
        std::unique_ptr<avro::InputStream> in = avro::memoryInputStream(reinterpret_cast<const uint8_t *>(buf.data()), buf.size());
        int64_t byte_count = buf.size();
        avro::encode(*encoder_ptr_, byte_count);
        encoder_ptr_->flush();
        copy(*in, *stream_);
#    ifdef SNAPPY_CODEC_AVAILABLE
    }
    else if (codec_ == avro::Codec::SNAPPY_CODEC)
    {
        std::vector<char> temp;
        std::string compressed;
        boost::crc_32_type crc;
        {
            boost::iostreams::filtering_ostream os;
            os.push(boost::iostreams::back_inserter(temp));
            const uint8_t * data;
            size_t len;

            std::unique_ptr<avro::InputStream> input = memoryInputStream(*buffer_);
            while (input->next(&data, &len))
            {
                boost::iostreams::write(os, reinterpret_cast<const char *>(data), len);
            }
        } // make sure all is flushed

        crc.process_bytes(reinterpret_cast<const char *>(temp.data()), temp.size());
        // For Snappy, add the CRC32 checksum
        int32_t checksum = crc();

        // Now compress
        size_t compressed_size = snappy::Compress(reinterpret_cast<const char *>(temp.data()), temp.size(), &compressed);
        temp.clear();
        {
            boost::iostreams::filtering_ostream os;
            os.push(boost::iostreams::back_inserter(temp));
            boost::iostreams::write(os, compressed.c_str(), compressed_size);
        }
        temp.push_back((checksum >> 24) & 0xFF);
        temp.push_back((checksum >> 16) & 0xFF);
        temp.push_back((checksum >> 8) & 0xFF);
        temp.push_back(checksum & 0xFF);
        std::unique_ptr<avro::InputStream> in = avro::memoryInputStream(reinterpret_cast<const uint8_t *>(temp.data()), temp.size());
        int64_t byte_count = temp.size();
        avro::encode(*encoder_ptr_, byte_count);
        encoder_ptr_->flush();
        copy(*in, *stream_);
#    endif
    }

    encoder_ptr_->init(*stream_);
    avro::encode(*encoder_ptr_, sync_);
    encoder_ptr_->flush();


    buffer_ = avro::memoryOutputStream();
    encoder_ptr_->init(*buffer_);
    object_count_ = 0;
}

void DataFileWriterBase::syncIfNeeded()
{
    encoder_ptr_->flush();
    if (buffer_->byteCount() >= sync_interval_)
    {
        sync();
    }
}

void DataFileWriterBase::flush()
{
    sync();
}

avro::DataFileSync DataFileWriterBase::makeSync()
{
    boost::mt19937 random(static_cast<uint32_t>(time(nullptr)));
    avro::DataFileSync sync;
    for (auto & i : sync)
    {
        i = random();
    }
    return sync;
}

using Magic = std::array<uint8_t, 4>;
static Magic magic = { { 'O', 'b', 'j', '\x01' } };

void DataFileWriterBase::writeHeader()
{
    encoder_ptr_->init(*stream_);
    avro::encode(*encoder_ptr_, magic);
    avro::encode(*encoder_ptr_, metadata_);
    avro::encode(*encoder_ptr_, sync_);
    encoder_ptr_->flush();
}

void DataFileWriterBase::setMetadata(const std::string & key, const std::string & value)
{
    std::vector<uint8_t> v(value.size());
    copy(value.begin(), value.end(), v.begin());
    metadata_[key] = v;
}

}

}
#endif
