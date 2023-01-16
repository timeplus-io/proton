#pragma once

#include <Interpreters/UserDefinedFunctionConfiguration.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <v8.h>

namespace DB
{
struct JavaScriptAggrFunctionState
{
    v8::Isolate * isolate; /// The ownership of isolate is the AggregateFunctionAdapter, no need to delete
    v8::Persistent<v8::Context> context;
    v8::Persistent<v8::Object> object;
    v8::Persistent<v8::Function> initialize_func;
    v8::Persistent<v8::Function> process_func;
    v8::Persistent<v8::Function> finalize_func;
    v8::Persistent<v8::Function> merge_func;
    v8::Persistent<v8::Function> serialize_func;
    v8::Persistent<v8::Function> deserialize_func;

    /// Cached rows
    MutableColumns columns;

    /// Whether the current group should emit
    bool should_finalize = false;

    /// JavaScript UDA code looks like:
    ///     {
    ///        // Definitions of state variables
    ///        state1: ...,
    ///        ...,
    ///        stateN: ...,
    ///
    ///        // Definitions of functions
    ///        initialize: function() {...}, // optional. Called when the function object is created
    ///        process: function (...) {...},  // required. the main function, with the same name of UDF, proton calls this function with batch of input rows
    ///        finalize: function (...) {...},  // required. the main function which returns the aggregation results to caller
    ///        serialize: function() {...},  // required. returns the serialized state of all internal states of UDA in string
    ///        deserialize: function() {...}, // required. recover the aggregation function state with the persisted internal state
    ///        merge: function(state_str) {...}  // required. merge two JavaScript UDA aggregation states into one. Used for multiple shards processing
    ///        has_customized_emit : false /// Define if the the aggregation has user defined emit strategy
    ///     }
    JavaScriptAggrFunctionState(
        const std::string & name,
        const std::string & source,
        const std::vector<UserDefinedFunctionConfiguration::Argument> & arguments,
        v8::Isolate * isolate_);

    ~JavaScriptAggrFunctionState();

    void add(const IColumn ** src_columns, size_t row_num);

    void reinitCache();
};

class AggregateFunctionJavaScriptAdapter final : public IAggregateFunctionHelper<AggregateFunctionJavaScriptAdapter>
{
public:
    using Data = JavaScriptAggrFunctionState;

    struct DataDeleter
    {
        void operator()(Data * data_) const { free(data_); }
    };

private:

    static Data & data(AggregateDataPtr __restrict place) { return *reinterpret_cast<Data *>(place); }
    static const Data & data(ConstAggregateDataPtr __restrict place) { return *reinterpret_cast<const Data *>(place); }

    struct IsolateDeleter
    {
        void operator()(v8::Isolate * isolate_) const { isolate_->Dispose(); }
    };

    const UserDefinedFunctionConfiguration & config;
    std::unique_ptr<v8::Isolate, AggregateFunctionJavaScriptAdapter::IsolateDeleter> isolate;
    size_t num_arguments;
    size_t max_v8_heap_size_in_bytes;
    std::string source;
    bool has_user_defined_emit_strategy = false;

public:
    AggregateFunctionJavaScriptAdapter(
        const UserDefinedFunctionConfiguration & config_,
        const DataTypes & types,
        const Array & params_,
        size_t max_v8_heap_size_in_bytes_);

    String getName() const override;

    DataTypePtr getReturnType() const override;

    bool allocatesMemoryInArena() const override { return false; }

    /// create instance of UDF via function_builder
    void create(AggregateDataPtr __restrict place) const override;

    /// destroy instance of UDF
    void destroy(AggregateDataPtr __restrict place) const noexcept override;

    bool hasTrivialDestructor() const override;

    size_t sizeOfData() const override;

    size_t alignOfData() const override;

    /// get instance of UDF from AggregateData and execute UDF
    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override;

    /// Merge with other Aggregate Data, maybe used before finalize result
    void merge(AggregateDataPtr __restrict /*place*/, ConstAggregateDataPtr /*rhs*/, Arena *) const override;

    /// Whether or not the aggregation should emit
    bool shouldFinalize(AggregateDataPtr __restrict /*place*/) const override;

    /// Send the cached rows to User Defined Aggregate function
    bool flush(AggregateDataPtr __restrict /*place*/) const override;

    bool hasUserDefinedEmit() const override { return has_user_defined_emit_strategy; }

    UDFType udfType() const override { return UDFType::Javascript; }

    /// Serialize the result related field of Aggregate Data
    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override;

    /// Deserialize the result related field of Aggregate Data
    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override;

    /// Insert the result row to the 'to' Column, later the state stored in place might get destroyed.
    void insertResultInto(AggregateDataPtr __restrict /*place*/, IColumn & to, Arena *) const override;

    void addBatchLookupTable8(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * map,
        size_t place_offset,
        std::function<void(AggregateDataPtr &)> init,
        const UInt8 * key,
        const IColumn ** columns,
        Arena * arena) const override;
};
}
