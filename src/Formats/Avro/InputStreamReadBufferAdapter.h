#pragma once

#include "config.h"
#if USE_AVRO
#    include <IO/ReadBuffer.h>
#    include <IO/WriteBuffer.h>

#    include <Stream.hh>

namespace DB
{

namespace Avro
{

class InputStreamReadBufferAdapter : public avro::InputStream
{
public:
    explicit InputStreamReadBufferAdapter(ReadBuffer & in_) : in(in_) { }

    bool next(const uint8_t ** data, size_t * len) override
    {
        if (in.eof())
        {
            *len = 0;
            return false;
        }

        *data = reinterpret_cast<const uint8_t *>(in.position());
        *len = in.available();

        in.position() += in.available();
        return true;
    }

    void backup(size_t len) override { in.position() -= len; }

    void skip(size_t len) override { in.tryIgnore(len); }

    size_t byteCount() const override { return in.count(); }

private:
    ReadBuffer & in;
};

}

}

#endif
