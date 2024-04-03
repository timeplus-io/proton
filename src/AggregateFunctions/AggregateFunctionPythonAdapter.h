#pragma once

#include <Functions/UserDefined/UserDefinedFunctionConfiguration.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Python.h>


namespace DB
{


struct PythonAggrFunctionState
{
    PythonAggrFunctionState(const PythonUserDefinedFunctionConfigurationPtr & config_);
    /// Cached rows
    MutableColumns columns;

    /// the number of emits, 0 means no emit, >1 means it has some aggregate results to emit
    size_t emit_times = 0;
    /// whether or not it is used in changelog input stream
    bool is_changelog_input = false;

    void add(const IColumn ** src_columns, size_t row_num);
    void negate(const IColumn ** src_columns, size_t row_num);

    void reinitCache();
    PyObject * py_instance = nullptr;
    PyObject * py_initialize_func = nullptr;
    PyObject * py_process_func = nullptr;   
    PyObject * py_finalize_func = nullptr;
    PyObject * py_merge_func = nullptr;
    PyObject * py_serialize_func = nullptr;
    PyObject * py_deserialize_func = nullptr;
    const PythonUserDefinedFunctionConfigurationPtr config;
    PyThreadState * mainstate = nullptr;
    PyThreadState * substate = nullptr;
    ~PythonAggrFunctionState();

};

class AggregateFunctionPythonAdapter final : public IAggregateFunctionHelper<AggregateFunctionPythonAdapter>
{
public:

    using Data = PythonAggrFunctionState;


private:
    static Data & data(AggregateDataPtr __restrict place) { return *reinterpret_cast<Data *>(place); }
    static const Data & data(ConstAggregateDataPtr __restrict place) { return *reinterpret_cast<const Data *>(place); }

    size_t num_arguments;
    bool is_changelog_input;
    const PythonUserDefinedFunctionConfigurationPtr config;

public:
     AggregateFunctionPythonAdapter(
        const PythonUserDefinedFunctionConfigurationPtr & config_,
        const DataTypes & argument_types_,
        const Array & params_,
        bool is_changelog_input_);

    String getName() const override { return config->name; }

    DataTypePtr getReturnType() const override { return config->result_type; }

    bool allocatesMemoryInArena() const override { return false; }

    void create(AggregateDataPtr __restrict place) const override;

    void destroy(AggregateDataPtr __restrict place) const noexcept override { data(place).~Data(); }

    bool hasTrivialDestructor() const override { return std::is_trivially_destructible_v<Data>; }

    size_t sizeOfData() const override { return sizeof(Data); }

    size_t alignOfData() const override { return alignof(Data); }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override;

    void negate(AggregateDataPtr __restrict place, const IColumn ** columns, size_t /*row_num*/, Arena * /*arena*/) const override;

    void merge(AggregateDataPtr __restrict /*place*/, ConstAggregateDataPtr /*rhs*/, Arena *) const override;

    size_t getEmitTimes(AggregateDataPtr __restrict /*place*/) const override { return 0;}

    size_t flush(AggregateDataPtr __restrict /*place*/) const override;

    bool hasUserDefinedEmit() const override { return false; }

    UDFType udfType() const override { return UDFType::Python; }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override;

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override;

    void insertResultInto(AggregateDataPtr __restrict /*place*/, IColumn & to, Arena *) const override;

    void addBatchLookupTable8(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * map,
        size_t place_offset,
        std::function<void(AggregateDataPtr &)> init,
        const UInt8 * key,
        const IColumn ** columns,
        Arena * arena,
        const IColumn * delta_col = nullptr) const override;
};
}
