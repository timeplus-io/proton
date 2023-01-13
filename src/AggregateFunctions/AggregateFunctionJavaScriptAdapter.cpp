#include "AggregateFunctionJavaScriptAdapter.h"

#include <Interpreters/UserDefinedFunctionConfiguration.h>
#include <Interpreters/V8Utils.h>

#include <Core/DecimalFunctions.h>
#include <DataTypes/ConvertV8DataTypes.h>
#include <Functions/FunctionsConversion.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int AGGREGATE_FUNCTION_THROW;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int CANNOT_CONVERT_TYPE;
extern const int UDF_COMPILE_ERROR;
}

JavaScriptAggrFunctionState::JavaScriptAggrFunctionState(
    const std::string & name,
    const std::string & source,
    const std::vector<UserDefinedFunctionConfiguration::Argument> & arguments,
    v8::Isolate * isolate_)
    : isolate(isolate_)
{
    assert(isolate);

    columns.reserve(arguments.size());
    for (const auto & arg : arguments)
    {
        auto col = arg.type->createColumn();
        col->reserve(8);
        columns.emplace_back(std::move(col));
    }

    auto init_functions = [&](v8::Local<v8::Context> & ctx, v8::TryCatch & try_catch, v8::Local<v8::Value> & obj) {
        object.Reset(isolate_, obj.As<v8::Object>());

        /// get functions defined in JavaScript UDA code
        {
            v8::Local<v8::Value> function_val;
            if (!obj.As<v8::Object>()->Get(ctx, V8::to_v8(isolate_, "init")).ToLocal(&function_val) || !function_val->IsFunction())
                LOG_DEBUG(&Poco::Logger::get("JavaScriptAggregateFunction"), "'init' function is not defined in JavaScript UDA");
            else
                init_func.Reset(isolate_, function_val.As<v8::Function>());
        }

        {
            v8::Local<v8::Value> function_val;
            if (!obj.As<v8::Object>()->Get(ctx, V8::to_v8(isolate_, "add")).ToLocal(&function_val) || !function_val->IsFunction())
                throw Exception(ErrorCodes::UDF_COMPILE_ERROR, "'add' function is required in JavaScript UDA");
            add_func.Reset(isolate_, function_val.As<v8::Function>());
        }

        {
            v8::Local<v8::Value> function_val;
            if (!obj.As<v8::Object>()->Get(ctx, V8::to_v8(isolate_, "finalize")).ToLocal(&function_val) || !function_val->IsFunction())
                throw Exception(ErrorCodes::UDF_COMPILE_ERROR, "'finalize' function is required in JavaScript UDA");
            finalize_func.Reset(isolate_, function_val.As<v8::Function>());
        }

        {
            v8::Local<v8::Value> function_val;
            if (!obj.As<v8::Object>()->Get(ctx, V8::to_v8(isolate_, "merge")).ToLocal(&function_val) || !function_val->IsFunction())
                throw Exception(ErrorCodes::UDF_COMPILE_ERROR, "'merge' function is required in JavaScript UDA");

            merge_func.Reset(isolate_, function_val.As<v8::Function>());
        }

        {
            v8::Local<v8::Value> function_val;
            if (!obj.As<v8::Object>()->Get(ctx, V8::to_v8(isolate_, "serialize")).ToLocal(&function_val) || !function_val->IsFunction())
                LOG_DEBUG(&Poco::Logger::get("JavaScriptAggregateFunction"), "'serialize' function is not defined in JavaScript UDA");
            else
                serialize_func.Reset(isolate_, function_val.As<v8::Function>());
        }

        {
            v8::Local<v8::Value> function_val;
            if (!obj.As<v8::Object>()->Get(ctx, V8::to_v8(isolate_, "deserialize")).ToLocal(&function_val) || !function_val->IsFunction())
                LOG_DEBUG(&Poco::Logger::get("JavaScriptAggregateFunction"), "'deserialize' function is not defined in JavaScript UDA");
            else
                deserialize_func.Reset(isolate_, function_val.As<v8::Function>());
        }

        context.Reset(isolate_, ctx);
    };

    V8::compileSource(isolate, name, fmt::format("var {}={};", name, source), init_functions);

    /// If init function is there, call it
    if (!init_func.IsEmpty())
    {
        auto init_callback = [&](v8::Local<v8::Context> & ctx, v8::TryCatch & try_catch) {
            v8::Local<v8::Object> local_obj = v8::Local<v8::Object>::New(isolate, object);
            v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate, init_func);

            v8::Local<v8::Value> res;
            if (!local_func->CallAsFunction(ctx, local_obj, 0, nullptr).ToLocal(&res))
                V8::throwException(
                    isolate, try_catch, ErrorCodes::AGGREGATE_FUNCTION_THROW, "Failed to invoke 'init' function of JavaScript UDA");
        };
        V8::run(isolate, context, init_callback);
    }
}

JavaScriptAggrFunctionState::~JavaScriptAggrFunctionState()
{
    deserialize_func.Reset();
    serialize_func.Reset();
    merge_func.Reset();
    finalize_func.Reset();
    add_func.Reset();
    init_func.Reset();
    object.Reset();
    context.Reset();
}

void JavaScriptAggrFunctionState::add(const IColumn ** src_columns, size_t row_num)
{
    for (size_t i = 0; auto & col : columns)
    {
        col->insertFrom(*src_columns[i], row_num);
        i++;
    }
}

void JavaScriptAggrFunctionState::reinitCache()
{
    MutableColumns new_columns;
    new_columns.reserve(columns.size());
    for (const auto & col : columns)
    {
        auto new_col = col->cloneEmpty();
        new_col->reserve(8);
        new_columns.emplace_back(std::move(new_col));
    }
    columns.swap(new_columns);
}

String AggregateFunctionJavaScriptAdapter::getName() const
{
    return config.name;
}

DataTypePtr AggregateFunctionJavaScriptAdapter::getReturnType() const
{
    return config.result_type;
}

/// create instance of UDF via function_builder
void AggregateFunctionJavaScriptAdapter::create(AggregateDataPtr __restrict place) const
{
    V8::checkHeapLimit(isolate.get(), max_v8_heap_size_in_bytes);
    new (place) Data(config.name, config.source, config.arguments, isolate.get());
}

/// destroy instance of UDF
void AggregateFunctionJavaScriptAdapter::destroy(AggregateDataPtr __restrict place) const noexcept
{
    data(place).~Data();
}

bool AggregateFunctionJavaScriptAdapter::hasTrivialDestructor() const
{
    return std::is_trivially_destructible_v<Data>;
}

size_t AggregateFunctionJavaScriptAdapter::sizeOfData() const
{
    return sizeof(Data);
}

size_t AggregateFunctionJavaScriptAdapter::alignOfData() const
{
    return alignof(Data);
}

void AggregateFunctionJavaScriptAdapter::addBatchLookupTable8(
    size_t row_begin,
    size_t row_end,
    AggregateDataPtr * map,
    size_t place_offset,
    std::function<void(AggregateDataPtr &)> init,
    const UInt8 * key,
    const IColumn ** columns,
    Arena * /*arena*/) const
{
    /// Will use UNROLL_COUNT number of lookup tables.

    static constexpr size_t UNROLL_COUNT = 4;

    std::unique_ptr<Data[], AggregateFunctionJavaScriptAdapter::DataDeleter> places{
        static_cast<Data *>(malloc(256 * UNROLL_COUNT * sizeof(Data))), AggregateFunctionJavaScriptAdapter::DataDeleter{}};
    bool has_data[256 * UNROLL_COUNT]{}; /// Separate flags array to avoid heavy initialization.

    size_t i = row_begin;

    /// Aggregate data into different lookup tables.

    size_t size_unrolled = (row_end - row_begin) / UNROLL_COUNT * UNROLL_COUNT;
    for (; i < size_unrolled; i += UNROLL_COUNT)
    {
        for (size_t j = 0; j < UNROLL_COUNT; ++j)
        {
            size_t idx = j * 256 + key[i + j];
            if (unlikely(!has_data[idx]))
            {
                new (&places[idx]) Data(config.name, config.source, config.arguments, isolate.get());
                has_data[idx] = true;
            }
            add(reinterpret_cast<char *>(&places[idx]), columns, i + j, nullptr);
        }
    }

    /// Merge data from every lookup table to the final destination.

    for (size_t k = 0; k < 256; ++k)
    {
        for (size_t j = 0; j < UNROLL_COUNT; ++j)
        {
            size_t idx = j * 256 + k;
            if (has_data[idx])
            {
                AggregateDataPtr & place = map[k];
                if (unlikely(!place))
                    init(place);

                merge(place + place_offset, reinterpret_cast<const char *>(&places[idx]), nullptr);
            }
        }
    }

    /// Process tails and add directly to the final destination.
    for (; i < row_end; ++i)
    {
        AggregateDataPtr & place = map[key[i]];
        if (unlikely(!place))
            init(place);

        add(place + place_offset, columns, i, nullptr);
    }

    for (size_t cur = row_begin; cur < row_end; ++cur)
    {
        AggregateDataPtr & place = map[key[cur]];
        flush(place + place_offset);
    }
}

void AggregateFunctionJavaScriptAdapter::add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const
{
    this->data(place).add(columns, row_num);
}

void AggregateFunctionJavaScriptAdapter::merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const
{
    auto & data = this->data(place);
    const auto & other = this->data(rhs);

    if (other.serialize_func.IsEmpty() || data.merge_func.IsEmpty())
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "JavaScript UDA doesn't define 'serialize' or 'merge' function. Can't do proper state merge");

    String state;
    auto get_other_state_func = [&](v8::Local<v8::Context> & ctx, v8::TryCatch & try_catch) {
        /// Get the state from rhs. isolate, context shall use (rhs) others
        v8::Local<v8::Object> other_local_obj = v8::Local<v8::Object>::New(other.isolate, other.object);
        v8::Local<v8::Context> other_local_ctx = v8::Local<v8::Context>::New(other.isolate, other.context);
        v8::Local<v8::Function> other_local_func = v8::Local<v8::Function>::New(other.isolate, other.serialize_func);

        /// Execute the state and get aggregate state
        v8::Local<v8::Value> res;
        if (!other_local_func->CallAsFunction(other_local_ctx, other_local_obj, 0, nullptr).ToLocal(&res))
            V8::throwException(
                other.isolate,
                try_catch,
                ErrorCodes::AGGREGATE_FUNCTION_THROW,
                "Failed to invoke 'serialize' function of JavaScript UDA in 'merge' function");

        state = V8::from_v8<String>(isolate.get(), res);
    };
    V8::run(isolate.get(), data.context, get_other_state_func);

    /// Merge the state
    auto merge_func = [&](v8::Local<v8::Context> & ctx, v8::TryCatch & try_catch) {
        v8::Local<v8::Object> local_obj = v8::Local<v8::Object>::New(isolate.get(), data.object);
        v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate.get(), data.merge_func);

        std::vector<v8::Local<v8::Value>> argv;
        argv.reserve(1);
        argv.emplace_back(V8::to_v8(isolate.get(), state));

        v8::Local<v8::Value> res;
        if (!local_func->CallAsFunction(ctx, local_obj, static_cast<int>(argv.size()), argv.data()).ToLocal(&res))
            V8::throwException(
                isolate.get(), try_catch, ErrorCodes::AGGREGATE_FUNCTION_THROW, "Failed to invoke 'merge' function of JavaScript UDA");
    };
    V8::run(isolate.get(), data.context, merge_func);
}

bool AggregateFunctionJavaScriptAdapter::shouldEmit(AggregateDataPtr __restrict place) const
{
    return this->data(place).should_emit;
}

bool AggregateFunctionJavaScriptAdapter::flush(AggregateDataPtr __restrict place) const
{
    /// First, get instance of UDF (part of the Aggregate Data) and prepare JavaScript execution context
    auto & data = this->data(place);
    bool should_emit = false;

    if (data.columns.empty() || data.columns[0]->empty())
        return should_emit;

    auto add_func = [&](v8::Local<v8::Context> & ctx, v8::TryCatch & try_catch) {
        v8::Local<v8::Object> local_obj = v8::Local<v8::Object>::New(isolate.get(), data.object);
        v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate.get(), data.add_func);

        /// Second, convert the input column into the corresponding object used by UDF
        auto argv = V8::prepareArguments(isolate.get(), config.arguments, data.columns);

        /// Third, execute the UDF and get aggregate state (only support the final state now, intermediate state is not supported
        v8::Local<v8::Value> res;
        if (!local_func->CallAsFunction(ctx, local_obj, static_cast<int>(config.arguments.size()), argv.data()).ToLocal(&res))
            V8::throwException(
                isolate.get(),
                try_catch,
                ErrorCodes::AGGREGATE_FUNCTION_THROW,
                "Failed to invoke JavaScript user defined aggregation function : {}",
                config.name);

        /// Forth, Check if the UDA should emit. We are expecting true / false returning from `add(...)` function
        if (!res->IsUndefined())
            should_emit = V8::from_v8<bool>(isolate.get(), res);
    };

    V8::run(isolate.get(), data.context, add_func);
    data.should_emit = should_emit;
    data.reinitCache();
    return should_emit;
}

/// Serialize the result related field of Aggregate Data
void AggregateFunctionJavaScriptAdapter::serialize(
    ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const
{
    const auto & data = this->data(place);

    if (data.serialize_func.IsEmpty())
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "JavaScript UDA doesn't define 'serialize' function. Can't do proper state serialization");

    auto serialize_func = [&](v8::Local<v8::Context> & ctx, v8::TryCatch & try_catch) {
        v8::Local<v8::Object> local_obj = v8::Local<v8::Object>::New(isolate.get(), data.object);
        v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate.get(), data.serialize_func);

        /// Execute the serialize() func and get aggregate state (only support the final state now, intermediate state is not supported
        v8::Local<v8::Value> res;
        if (!local_func->CallAsFunction(ctx, local_obj, 0, nullptr).ToLocal(&res))
            V8::throwException(
                isolate.get(), try_catch, ErrorCodes::AGGREGATE_FUNCTION_THROW, "Failed to invoke 'serialize' function of JavaScript UDA");

        String state = V8::from_v8<String>(isolate.get(), res);
        writeStringBinary(state, buf);
    };
    V8::run(isolate.get(), data.context, serialize_func);
}

/// Deserialize the result related field of Aggregate Data
void AggregateFunctionJavaScriptAdapter::deserialize(
    AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const
{
    auto & data = this->data(place);

    if (data.deserialize_func.IsEmpty())
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "JavaScript UDA doesn't define 'deserialize' function. Can't do proper state deserialization");

    String state;
    readStringBinary(state, buf);

    auto deserialize_func = [&](v8::Local<v8::Context> & ctx, v8::TryCatch & try_catch) {
        v8::Local<v8::Object> local_obj = v8::Local<v8::Object>::New(isolate.get(), data.object);
        v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate.get(), data.deserialize_func);

        /// convert state to v8 string
        std::vector<v8::Local<v8::Value>> argv;
        argv.reserve(1);
        argv.emplace_back(V8::to_v8(isolate.get(), state));

        /// init the UDF with state function
        v8::Local<v8::Value> res;
        if (!local_func->CallAsFunction(ctx, local_obj, static_cast<int>(argv.size()), argv.data()).ToLocal(&res))
            V8::throwException(
                isolate.get(),
                try_catch,
                ErrorCodes::AGGREGATE_FUNCTION_THROW,
                "Failed to invoke 'deserialize' function of JavaScript UDA");
    };
    V8::run(isolate.get(), data.context, deserialize_func);
}

void AggregateFunctionJavaScriptAdapter::insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const
{
    auto & data = this->data(place);

    auto finalize_func = [&](v8::Local<v8::Context> & ctx, v8::TryCatch & try_catch) {
        v8::Local<v8::Object> local_obj = v8::Local<v8::Object>::New(isolate.get(), data.object);
        v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate.get(), data.finalize_func);

        /// Execute the state() func and get aggregate state (only support the final state now, intermediate state is not supported
        v8::Local<v8::Value> res;
        if (!local_func->CallAsFunction(ctx, local_obj, 0, nullptr).ToLocal(&res))
            V8::throwException(
                isolate.get(), try_catch, ErrorCodes::AGGREGATE_FUNCTION_THROW, "Failed to invoke 'finalize' function of JavaScript UDA");

        V8::insertResult(isolate.get(), to, config.result_type, true, res);
    };
    V8::run(isolate.get(), data.context, finalize_func);
    data.should_emit = false;
}
}
