#pragma once

#include "config.h"

#if USE_PYTHON_UDF

#include <AggregateFunctions/IAggregateFunction.h>
#include <CPython/PyObjectPtr.h>
#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>

#include <Python.h>


namespace DB
{


struct PythonAggrFunctionState
{
    PythonAggrFunctionState(
        const cluster::protocol::UserDefinedFunctionDescriptorPtr & udf_desc_, bool is_changelog_input_, const std::string & module_name_);

    /// Cached rows
    MutableColumns columns;

    /// the number of emits, 0 means no emit, >1 means it has some aggregate results to emit
    size_t emit_times = 0;
    /// whether or not it is used in changelog input stream
    bool is_changelog_input = false;


    void add(const IColumn ** src_columns, size_t row_num);
    void negate(const IColumn ** src_columns, size_t row_num);

    void reinitCache();
    cpython::PyObjectPtr py_instance;
    cpython::PyObjectPtr py_initialize_func;
    cpython::PyObjectPtr py_process_func;
    cpython::PyObjectPtr py_finalize_func;
    cpython::PyObjectPtr py_merge_func;
    cpython::PyObjectPtr py_serialize_func;
    cpython::PyObjectPtr py_deserialize_func;
    cluster::protocol::UserDefinedFunctionDescriptorPtr udf_desc;
    std::string module_name;

    ~PythonAggrFunctionState();
};

class AggregateFunctionPythonAdapter final : public IAggregateFunctionHelper<AggregateFunctionPythonAdapter>
{
public:
    using Data = PythonAggrFunctionState;


private:
    static Data & data(AggregateDataPtr __restrict place) { return *reinterpret_cast<Data *>(place); }
    static const Data & data(ConstAggregateDataPtr __restrict place) { return *reinterpret_cast<const Data *>(place); }

    const cluster::protocol::UserDefinedFunctionDescriptorPtr udf_desc;
    size_t num_arguments;
    bool is_changelog_input;
    // const PythonUserDefinedFunctionConfigurationPtr config;
    bool using_numpy = false;
    bool has_user_defined_emit_strategy = false;

    std::string module_name;

public:
    AggregateFunctionPythonAdapter(
        cluster::protocol::UserDefinedFunctionDescriptorPtr && udf_desc_,
        const DataTypes & argument_types_,
        const Array & params_,
        bool is_changelog_input_);

    ~AggregateFunctionPythonAdapter() override;

    String getName() const override { return udf_desc->name; }

    bool allocatesMemoryInArena() const override { return false; }

    void create(AggregateDataPtr __restrict place) const override;

    void destroy(AggregateDataPtr __restrict place) const noexcept override { data(place).~Data(); }

    bool hasTrivialDestructor() const override { return std::is_trivially_destructible_v<Data>; }

    size_t sizeOfData() const override { return sizeof(Data); }

    size_t alignOfData() const override { return alignof(Data); }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override;

    void negate(AggregateDataPtr __restrict place, const IColumn ** columns, size_t /*row_num*/, Arena * /*arena*/) const override;

    void merge(AggregateDataPtr __restrict /*place*/, ConstAggregateDataPtr /*rhs*/, Arena *) const override;

    size_t getEmitTimes(ConstAggregateDataPtr __restrict place) const override;

    bool hasUserDefinedEmit() const override { return has_user_defined_emit_strategy; }

    size_t flush(AggregateDataPtr __restrict /*place*/) const override;

    UDFType udfType() const override { return UDFType::Python; }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override;

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override;

    void insertResultInto(AggregateDataPtr __restrict /*place*/, IColumn & to, Arena *) const override;

};
}
#endif
