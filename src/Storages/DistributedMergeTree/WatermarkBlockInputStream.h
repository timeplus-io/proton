#pragma once

#include "Watermark.h"

#include <DataStreams/IBlockInputStream.h>

class DateLUTImpl;

namespace DB
{
/**
 * WatermarkBlockInputStream projects watermark according to watermark strategies
 * by observing the events in its `input`.
 */

struct SelectQueryInfo;

class WatermarkBlockInputStream final: public IBlockInputStream
{
public:
    WatermarkBlockInputStream(
        BlockInputStreamPtr input_, SelectQueryInfo & query_info, const String & partition_key_, Poco::Logger * log_);

    ~WatermarkBlockInputStream() override = default;

    String getName() const override { return "WatermarkBlockInputStream"; }

    Block getHeader() const override { return input->getHeader(); }

    void readPrefix() override { input->readPrefix(); }

    void cancel(bool kill) override;

private:
    void readPrefixImpl() override;
    Block readImpl() override;

    void initWatermark(SelectQueryInfo & query_info, const String & partition_key, Poco::Logger * log);

    BlockInputStreamPtr input;
    WatermarkPtr watermark;
};
}
