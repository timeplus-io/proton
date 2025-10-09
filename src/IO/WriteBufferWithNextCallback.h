#pragma once

#include <IO/WriteBufferFromFileDecorator.h>
#include "IO/SwapHelper.h"

namespace DB
{

class WriteBufferWithNextCallback final : public WriteBufferFromFileDecorator
{
public:
    using NextCallback = std::function<void(size_t /*new_data_size*/)>;

    WriteBufferWithNextCallback(std::unique_ptr<WriteBuffer> write_buffer, NextCallback next_callback_)
        : WriteBufferFromFileDecorator(std::move(write_buffer))
        , next_callback(next_callback_)
    {
    }

private:
    void nextImpl() override {
        /// `WriteBufferFromFileDecorator::nextImpl()` is a private function, can't be called here.
        {
            SwapHelper swap(*this, *impl);
            impl->next();
        }

        next_callback(count());
    }

    /// A callback to be called when the next function is called.
    NextCallback next_callback;
};

}
