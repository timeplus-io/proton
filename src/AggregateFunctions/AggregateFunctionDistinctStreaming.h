#pragma once

#include "AggregateFunctionDistinct.h"

/// proton: starts.
namespace DB
{

template <typename Data>
struct AggregateFunctionDistinctStreamingData
{
    Data extra_data{};

    /// last set size, which used to confirm whether the data is extra if exist
    std::optional<UInt64> last_size;
};

/** Adaptor for aggregate functions.
  * Adding -DistinctStreaming suffix to aggregate function for streaming query
**/
template <typename Data>
class AggregateFunctionDistinctStreaming : public AggregateFunctionDistinct<Data>
{
private:
    using Base = AggregateFunctionDistinct<Data>;
    using StreamingData = AggregateFunctionDistinctStreamingData<Data>;
    static constexpr auto streaming_size = sizeof(StreamingData);

    static StreamingData & streamingData(AggregateDataPtr __restrict place) { return *reinterpret_cast<StreamingData *>(place); }
    static const StreamingData & streamingData(ConstAggregateDataPtr __restrict place)
    {
        return *reinterpret_cast<const StreamingData *>(place);
    }

    AggregateDataPtr getStreamingPlace(AggregateDataPtr __restrict place) const noexcept { return place + Base::sizeOfData(); }

    ConstAggregateDataPtr getStreamingPlace(ConstAggregateDataPtr __restrict place) const noexcept { return place + Base::sizeOfData(); }

public:
    using Base::Base;

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & streaming_data = this->streamingData(getStreamingPlace(place));

        this->data(place).add(columns, this->arguments_num, row_num, arena);

        /// Add extra distinct streaming data to result for global streaming
        if (streaming_data.last_size && streaming_data.last_size.value() != this->data(place).set.size())
        {
            streaming_data.extra_data.add(columns, this->arguments_num, row_num, arena);
            streaming_data.last_size = this->data(place).set.size();
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & streaming_data = this->streamingData(getStreamingPlace(place));

        this->data(place).merge(this->data(rhs), arena);

        /// Merge extra distinct streaming data to result for global streaming
        if (streaming_data.last_size && streaming_data.last_size.value() != this->data(place).set.size())
        {
            streaming_data.extra_data.merge(this->data(rhs), arena);
            streaming_data.last_size = this->data(place).set.size();
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        auto & streaming_data = this->streamingData(getStreamingPlace(place));

        /// Add distinct streaming data to result if global streaming
        auto arguments = (streaming_data.last_size ? streaming_data.extra_data : this->data(place)).getArguments(this->argument_types);
        ColumnRawPtrs arguments_raw(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            arguments_raw[i] = arguments[i].get();

        assert(!arguments.empty());
        this->nested_func->addBatchSinglePlace(arguments[0]->size(), Base::getNestedPlace(place), arguments_raw.data(), arena);
        this->nested_func->insertResultInto(Base::getNestedPlace(place), to, arena);

        /// Clear streaming set for each streaming emit
        if (streaming_data.last_size)
            streaming_data.extra_data.set.clear();

        streaming_data.last_size = this->data(place).set.size();
    }

    String getName() const override { return Base::getName() + "Streaming"; }

    size_t sizeOfData() const override { return Base::sizeOfData() + streaming_size; }

    void create(AggregateDataPtr __restrict place) const override
    {
        Base::create(place);
        new (getStreamingPlace(place)) StreamingData;
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        Base::destroy(place);
        this->streamingData(getStreamingPlace(place)).~StreamingData();
    }
};


}
/// proton: ends.
