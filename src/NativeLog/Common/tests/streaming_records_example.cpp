
#include <flatbuffers/flatbuffers.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <NativeLog/Schemas/Record.h>
#include <base/ClockUtils.h>

#include <algorithm>
#include <cassert>

std::string HEADER_KEY_TEMPLATE = "key";
std::vector<int8_t> HEADER_VALUE_TEMPLATE = {'v', 'a', 'l', 'u', 'e'};
std::vector<int8_t> KEY_DATA_TEMPLATE = {'a', 'b', 'c', 'd', 'e', 'f'};
std::vector<int8_t> VALUE_DATA_TEMPLATE = {'i', 'j', 'k', 'l', 'm', 'n'};

flatbuffers::Offset<nlog::Record> createRecord(flatbuffers::FlatBufferBuilder & fbb, int8_t batch_delta, int8_t record_delta)
{
    /// Build header
    std::vector<int8_t> head_value{HEADER_VALUE_TEMPLATE};
    std::for_each(head_value.begin(), head_value.end(), [batch_delta, record_delta](auto & k) { k += batch_delta + record_delta; });
    auto header = nlog::CreateRecordHeader(
        fbb,
        fbb.CreateString(HEADER_KEY_TEMPLATE + std::to_string(batch_delta) + std::to_string(record_delta)),
        fbb.CreateVector(head_value));

    /// Build record
    std::vector<int8_t> key_data{KEY_DATA_TEMPLATE};
    std::for_each(key_data.begin(), key_data.end(), [batch_delta, record_delta](auto & k) { k += batch_delta + record_delta; });

    std::vector<int8_t> value_data{VALUE_DATA_TEMPLATE};
    std::for_each(value_data.begin(), value_data.end(), [batch_delta, record_delta](auto & k) { k += batch_delta + record_delta; });

    return nlog::CreateRecord(
        fbb,
        DB::UTCMilliseconds::now(),
        0 + batch_delta + record_delta,
        fbb.CreateVector(key_data),
        fbb.CreateVector(value_data),
        fbb.CreateVector(std::vector<decltype(header)>{header}));
}

flatbuffers::Offset<nlog::RecordBatch> createBatch(flatbuffers::FlatBufferBuilder & fbb, int8_t batch_delta)
{
    auto record1 = createRecord(fbb, batch_delta, 0);
    auto record2 = createRecord(fbb, batch_delta, 1);

    /// Build record batch
    return nlog::CreateRecordBatch(
        fbb,
        123 + batch_delta,
        2 + batch_delta,
        1000000 + batch_delta,
        1 + batch_delta,
        2 + batch_delta,
        0 + batch_delta,
        0 + batch_delta,
        0 + batch_delta,
        -1 + batch_delta,
        -1 + batch_delta,
        -1 + batch_delta,
        fbb.CreateVector(std::vector<decltype(record1)>{record1, record2}));
}

void validateBatch(const nlog::RecordBatch * batch, int8_t batch_delta)
{
    assert(batch->crc() == 123ul + batch_delta);
    assert(batch->flags() == 2ul + batch_delta);
    assert(batch->base_offset() == 1000000 + batch_delta);
    assert(batch->shard_leader_epoch() == 1 + batch_delta);
    assert(batch->last_offset_delta() == 2 + batch_delta);
    assert(batch->append_timestamp() == 0 + batch_delta);
    assert(batch->first_timestamp() == 0 + batch_delta);
    assert(batch->max_timestamp() == 0 + batch_delta);
    assert(batch->producer_id() == -1 + batch_delta);
    assert(batch->producer_epoch() == -1 + batch_delta);
    assert(batch->base_sequence() == -1 + batch_delta);

    int8_t record_delta = 0;
    for (const auto * record : *batch->records())
    {
        assert(record->offset_delta() == 0 + batch_delta + record_delta);
        assert(record->key()->size() == KEY_DATA_TEMPLATE.size());
        size_t i = 0;
        std::for_each(record->key()->begin(), record->key()->end(), [batch_delta, record_delta, &i](const auto k) {
            assert(k == KEY_DATA_TEMPLATE[i++] + batch_delta + record_delta);
            (void)batch_delta;
            (void)record_delta;
            (void)i;
            (void)k;
        });

        i = 0;
        assert(record->value()->size() == VALUE_DATA_TEMPLATE.size());
        std::for_each(record->value()->begin(), record->value()->end(), [batch_delta, record_delta, &i](const auto k) {
            assert(k == VALUE_DATA_TEMPLATE[i++] + batch_delta + record_delta);
            (void)batch_delta;
            (void)record_delta;
            (void)i;
            (void)k;
        });

        /// Validate header
        assert(record->headers()->size() == 1);
        std::for_each(record->headers()->begin(), record->headers()->end(), [batch_delta, record_delta](const auto * h) {
            assert(h->key()->str() == HEADER_KEY_TEMPLATE + std::to_string(batch_delta) + std::to_string(record_delta));

            size_t j = 0;
            std::for_each(h->value()->begin(), h->value()->end(), [batch_delta, record_delta, &j](const auto k) {
                assert(k == HEADER_VALUE_TEMPLATE[j++] + batch_delta + record_delta);
                (void)batch_delta;
                (void)record_delta;
                (void)j;
                (void)k;
            });
        });

        ++record_delta;
    }
}

void streamingFlatRecords()
{
    {
        DB::WriteBufferFromFile wbuf{"records.bin"};

        {
            flatbuffers::FlatBufferBuilder fbb;
            auto batch = createBatch(fbb, 0);
            nlog::FinishSizePrefixedRecordBatchBuffer(fbb, batch);
            std::cout << "Batch 0 size: " << fbb.GetSize() << std::endl;
            wbuf.write(reinterpret_cast<const char *>(fbb.GetBufferPointer()), fbb.GetSize());
        }

        {
            flatbuffers::FlatBufferBuilder fbb;
            auto batch = createBatch(fbb, 1);
            nlog::FinishSizePrefixedRecordBatchBuffer(fbb, batch);
            std::cout << "Batch 1 size: " << fbb.GetSize() << std::endl;
            wbuf.write(reinterpret_cast<const char *>(fbb.GetBufferPointer()), fbb.GetSize());
        }

        wbuf.sync();
    }

    /// Read record batches back
    DB::ReadBufferFromFile rbuf("records.bin");

    std::vector<char> data(4096, '0');
    auto n = rbuf.read(data.data(), data.size());
    /// Correctness test, read only 1 byte at a time
    /// std::vector<char> data(1, '0');
    /// auto n = rbuf.read(data.data(), 1);
    size_t consumed = 0;
    size_t unconsumed = n;

    std::cout << "File size: " << rbuf.size() << ", first read: " << n << std::endl;

    /// Return true done, else false
    auto copy_and_read = [&](bool resize = false) -> bool {
        if (rbuf.eof())
            return true;

        if (likely(!resize))
        {
            /// First copy unconsumed to the beginning of the buf if necessary
            if (data.size() == consumed + unconsumed)
            {
                for (auto start = consumed, index = 0ul; start < data.size(); ++start, ++index)
                    data[index] = data[start];

                consumed = 0;
            }
        }
        else
            data.resize(data.size() * 2);

        /// Read more data
        auto more = rbuf.read(data.data() + unconsumed + consumed, data.size() - consumed - unconsumed);
        /// Correctness test, read one byte at at time
        /// auto more = rbuf.read(data.data() + unconsumed + consumed, 1);
        unconsumed += more;

        std::cout << "Reading " << more << " more bytes, unconsumed: " << unconsumed << " consumed: " << consumed << std::endl;
        return false;
    };

    int8_t batch_index = 0;

    while (1)
    {
        if (unlikely(unconsumed < sizeof(flatbuffers::uoffset_t)))
        {
            if (copy_and_read())
                break;
            else
                continue;
        }

        auto batch_size = flatbuffers::ReadScalar<flatbuffers::uoffset_t>(data.data() + consumed);

        std::cout << "Batch " << static_cast<int32_t>(batch_index) << " size: " << batch_size + sizeof(flatbuffers::uoffset_t)
                  << " consumed: " << consumed << " unconsumed: " << unconsumed << ", parsing at: " << consumed << std::endl;

        if (batch_size + sizeof(flatbuffers::uoffset_t) > unconsumed)
        {
            if (copy_and_read(unconsumed >= data.size()))
                break;
            else
                continue;
        }

        auto batch = nlog::GetSizePrefixedRecordBatch(data.data() + consumed);
        unconsumed -= (batch_size + sizeof(flatbuffers::uoffset_t));
        consumed += (batch_size + sizeof(flatbuffers::uoffset_t));
        validateBatch(batch, batch_index);
        batch_index++;
    }
}

int main(int, char **)
{
    streamingFlatRecords();
}
