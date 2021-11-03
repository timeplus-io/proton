#include "WatermarkBlockInputStream.h"
#include "HopWatermark.h"
#include "TumbleWatermark.h"

/// FIXME: convert to Watermark transform ?
/// FIXME: Week / Month / Quarter / Year cases don't work yet
namespace DB
{
void WatermarkBlockInputStream::readPrefixImpl()
{
    assert(watermark);
    watermark->preProcess();
}

WatermarkBlockInputStream::WatermarkBlockInputStream(
    BlockInputStreamPtr input_, SelectQueryInfo & query_info, const String & partition_key_, Poco::Logger * log_)
    : input(input_)
{
    assert(input);
    initWatermark(query_info, partition_key_, log_);
}

Block WatermarkBlockInputStream::readImpl()
{
    if (isCancelled())
    {
        return {};
    }

    /// We assume it is `StreamingBlockInputStream` here
    /// which emits a header only block if there is no records from streaming store
    /// we leverage this side effect as a timer to check idleness source
    Block block = input->read();
    watermark->process(block);
    return block;
}

void WatermarkBlockInputStream::initWatermark(SelectQueryInfo & query_info, const String & partition_key, Poco::Logger * log)
{
    WatermarkSettings watermark_settings(query_info);
    if (watermark_settings.func_name == "__TUMBLE")
    {
        watermark = std::make_shared<TumbleWatermark>(std::move(watermark_settings), partition_key, log);
    }
    else if (watermark_settings.func_name == "__HOP")
    {
        watermark = std::make_shared<HopWatermark>(std::move(watermark_settings), partition_key, log);
    }
    else
    {
        watermark = std::make_shared<Watermark>(std::move(watermark_settings), partition_key, log);
    }
}

void WatermarkBlockInputStream::cancel(bool kill)
{
    input->cancel(kill);
    IBlockInputStream::cancel(kill);
}
}
