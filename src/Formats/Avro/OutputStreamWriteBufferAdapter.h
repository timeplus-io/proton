#pragma once

#include "config.h"
#if USE_AVRO
#include <IO/WriteBuffer.h>

#include <Stream.hh>

namespace DB
{

namespace Avro
{

class OutputStreamWriteBufferAdapter : public avro::OutputStream
{
public:
    explicit OutputStreamWriteBufferAdapter(WriteBuffer & out_) : out(out_) {}

    bool next(uint8_t ** data, size_t * len) override
    {
        out.nextIfAtEnd();
        *data = reinterpret_cast<uint8_t *>(out.position());
        *len = out.available();
        out.position() += out.available();

        return true;
    }

    void backup(size_t len) override { out.position() -= len; }

    uint64_t byteCount() const override { return out.count(); }
    void flush() override { }

private:
    WriteBuffer & out;
};

}

}

#endif
