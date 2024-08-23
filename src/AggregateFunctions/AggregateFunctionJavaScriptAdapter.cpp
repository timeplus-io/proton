#include "AggregateFunctionJavaScriptAdapter.h"

#include <Core/DecimalFunctions.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/UserDefined/UserDefinedFunctionConfiguration.h>
#include <V8/ConvertDataTypes.h>
#include <V8/Utils.h>
#include <Common/logger_useful.h>
#include <span>

namespace DB
{
namespace ErrorCodes
{
extern const int AGGREGATE_FUNCTION_THROW;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int CANNOT_CONVERT_TYPE;
extern const int UDF_COMPILE_ERROR;
extern const int UDF_INTERNAL_ERROR;
}

JavaScriptBlueprint::JavaScriptBlueprint(const String & name, const String & source)
{
    /// FIXME, create isolate from V8::V8 global isolates pool
    v8::Isolate::CreateParams isolate_params;

    auto v8_max_heap_bytes = static_cast<size_t>(getMemoryAmountOrZero() * 0.6);
    isolate_params.constraints.ConfigureDefaultsFromHeapSize(0, v8_max_heap_bytes);
    isolate_params.constraints.set_max_old_generation_size_in_bytes(v8_max_heap_bytes);

    isolate_params.array_buffer_allocator_shared
        = std::shared_ptr<v8::ArrayBuffer::Allocator>(v8::ArrayBuffer::Allocator::NewDefaultAllocator());
    isolate = std::unique_ptr<v8::Isolate, IsolateDeleter>(v8::Isolate::New(isolate_params), IsolateDeleter());

    auto * logger = &Poco::Logger::get("JavaScriptAggregateFunction");

    /// Analyze if this UDA's definition to initialize the blueprint
    auto init_and_validate = [&](v8::Isolate * isolate_, v8::Local<v8::Context> & local_ctx, v8::TryCatch & try_catch, v8::Local<v8::Value> & blueprint) {
        if (!blueprint->IsObject())
        {
            LOG_ERROR(logger, "Missing UDA object definition or the variable '{}' is not an object.", name);
            throw Exception(ErrorCodes::UDF_COMPILE_ERROR, "JavaScript UDA is not valid");
        }

        auto obj = blueprint.As<v8::Object>();

        /// Validate functions / properties defined in JavaScript UDA code
        {
            v8::Local<v8::Value> function_val;
            if (!obj->Get(local_ctx, V8::to_v8(isolate_, "initialize")).ToLocal(&function_val)
                || !function_val->IsFunction())
                LOG_DEBUG(logger, "'initialize' function is not defined in JavaScript UDA");
        }

        {
            v8::Local<v8::Value> function_val;
            if (!obj->Get(local_ctx, V8::to_v8(isolate_, "process")).ToLocal(&function_val) || !function_val->IsFunction())
                throw Exception(ErrorCodes::UDF_COMPILE_ERROR, "'process' function is required in JavaScript UDA");
        }

        {
            v8::Local<v8::Value> function_val;
            if (!obj->Get(local_ctx, V8::to_v8(isolate_, "finalize")).ToLocal(&function_val)
                || !function_val->IsFunction())
                throw Exception(ErrorCodes::UDF_COMPILE_ERROR, "'finalize' function is required in JavaScript UDA");
        }

        {
            v8::Local<v8::Value> function_val;
            if (!obj->Get(local_ctx, V8::to_v8(isolate_, "merge")).ToLocal(&function_val) || !function_val->IsFunction())
                LOG_WARNING(logger, "'merge' function is not defined in JavaScript UDA. Multiple shard processing may not be supported.");
        }

        {
            v8::Local<v8::Value> function_val;
            if (!obj->Get(local_ctx, V8::to_v8(isolate_, "serialize")).ToLocal(&function_val)
                || !function_val->IsFunction())
                LOG_WARNING(
                    logger,
                    "'serialize' function is not defined in JavaScript UDA. Multiple shard processing or checkpoint may be not supported");
        }

        {
            v8::Local<v8::Value> function_val;
            if (!obj->Get(local_ctx, V8::to_v8(isolate_, "deserialize")).ToLocal(&function_val)
                || !function_val->IsFunction())
                LOG_WARNING(
                    logger,
                    "'deserialize' function is not defined in JavaScript UDA. Multiple shard processing or checkpoint may be not "
                    "supported");
        }

        {
            v8::Local<v8::Value> val;
            if (obj->Get(local_ctx, V8::to_v8(isolate_, "has_customized_emit")).ToLocal(&val) && !val->IsUndefined())
            {
                has_user_defined_emit_strategy = V8::from_v8<bool>(isolate_, val);
                if (has_user_defined_emit_strategy)
                    LOG_INFO(
                        &Poco::Logger::get("JavaScriptAggregateFunction"), "JavaScript UDA '{}' has defined its own emit strategy", name);
            }
        }

        global_context.Reset(isolate_, local_ctx);
        assert(!global_context.IsEmpty());
        uda_object_blueprint.Reset(isolate_, obj);
    };

    /// We rewrite the source code as follows to make it compile
    /// var <uda_name> = {...};
    V8::compileSource(isolate.get(), name, fmt::format("var {}={};", name, source), std::move(init_and_validate));
}

JavaScriptBlueprint::~JavaScriptBlueprint() noexcept
{
    uda_object_blueprint.Reset();
    global_context.Reset();
}

JavaScriptAggrFunctionState::JavaScriptAggrFunctionState(
    const JavaScriptBlueprint & blueprint,
    const std::vector<UserDefinedFunctionConfiguration::Argument> & arguments,
    const bool is_changelog_input_)
    : is_changelog_input(is_changelog_input_)
{
    columns.reserve(arguments.size());

    /// check _tp_delta column
    if (unlikely(arguments.back().type->getTypeId() != TypeIndex::Int8))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Tha last argument of JavaScript UDA is '_tp_delta' column, which should be 'int8'. Invalid type.");

    for (const auto & arg : arguments)
    {
        auto col = arg.type->createColumn();
        col->reserve(8);
        columns.emplace_back(std::move(col));
    }

    auto init_functions = [&](v8::Isolate * isolate_, v8::Local<v8::Context> & local_ctx, v8::TryCatch & try_catch) {
        auto blueprint_obj = v8::Local<v8::Object>::New(isolate_, blueprint.uda_object_blueprint);

        /// Just clone from blueprint which acts like prototype
        auto obj = blueprint_obj->Clone();

        uda_instance.Reset(isolate_, obj);

        /// get functions defined in JavaScript UDA code
        {
            v8::Local<v8::Value> function_val;
            if (obj->Get(local_ctx, V8::to_v8(isolate_, "initialize")).ToLocal(&function_val)
                && function_val->IsFunction())
                initialize_func.Reset(isolate_, function_val.As<v8::Function>());
        }

        {
            v8::Local<v8::Value> function_val;
            if (obj->Get(local_ctx, V8::to_v8(isolate_, "process")).ToLocal(&function_val) && function_val->IsFunction())
            {
                assert(function_val->IsFunction());
                process_func.Reset(isolate_, function_val.As<v8::Function>());
            }
        }

        {
            v8::Local<v8::Value> function_val;
            if (obj->Get(local_ctx, V8::to_v8(isolate_, "finalize")).ToLocal(&function_val) && function_val->IsFunction())
            {
                assert(function_val->IsFunction());
                finalize_func.Reset(isolate_, function_val.As<v8::Function>());
            }
        }

        {
            v8::Local<v8::Value> function_val;
            if (obj->Get(local_ctx, V8::to_v8(isolate_, "merge")).ToLocal(&function_val) && function_val->IsFunction())
                merge_func.Reset(isolate_, function_val.As<v8::Function>());
        }

        {
            v8::Local<v8::Value> function_val;
            if (obj->Get(local_ctx, V8::to_v8(isolate_, "serialize")).ToLocal(&function_val) && function_val->IsFunction())
                serialize_func.Reset(isolate_, function_val.As<v8::Function>());
        }

        {
            v8::Local<v8::Value> function_val;
            if (obj->Get(local_ctx, V8::to_v8(isolate_, "deserialize")).ToLocal(&function_val)
                && function_val->IsFunction())
                deserialize_func.Reset(isolate_, function_val.As<v8::Function>());
        }

        /// If init function is there, call it
        if (!initialize_func.IsEmpty())
        {
            v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate_, initialize_func);

            v8::Local<v8::Value> res;
            if (!local_func->Call(local_ctx, obj, 0, nullptr).ToLocal(&res))
            {
                LOG_ERROR(&Poco::Logger::get("JavaScriptAggregateFunction"), "Failed to initialize UDA");

                V8::throwException(
                    isolate_, try_catch, ErrorCodes::UDF_INTERNAL_ERROR, "Failed to invoke 'initialize' function of JavaScript UDA");
            }
        }
    };

    /// Everybody use global_context to run the script
    V8::run(blueprint.isolate.get(), blueprint.global_context, std::move(init_functions));
}

JavaScriptAggrFunctionState::~JavaScriptAggrFunctionState()
{
    deserialize_func.Reset();
    serialize_func.Reset();
    merge_func.Reset();
    finalize_func.Reset();
    process_func.Reset();
    initialize_func.Reset();
    uda_instance.Reset();
}

void JavaScriptAggrFunctionState::add(const IColumn ** src_columns, size_t row_num)
{
    assert(columns.size() >= 1);
    size_t num_of_input_columns = columns.size() - 1;

    for (size_t i = 0; i < num_of_input_columns; i++)
        columns[i]->insertFrom(*src_columns[i], row_num);

    /// _tp_delta column
    if (is_changelog_input)
        columns.back()->insert(1);
}

void JavaScriptAggrFunctionState::negate(const IColumn ** src_columns, size_t row_num)
{
    assert(columns.size() >= 1);
    size_t num_of_input_columns = columns.size() - 1;

    for (size_t i = 0; i < num_of_input_columns; i++)
        columns[i]->insertFrom(*src_columns[i], row_num);

    /// _tp_delta column
    if (is_changelog_input)
        columns.back()->insert(-1);
}

void JavaScriptAggrFunctionState::reinitCache()
{
    MutableColumns new_columns;
    new_columns.reserve(columns.size());
    for (const auto & col : columns)
    {
        auto new_col = col->cloneEmpty();
        /// The following reserve is just a guess.
        /// For historical data processing, it can be very wrong since they have very big batch like 40 k rows
        /// We will need benchmark if caching to a v8::Array directly makes more sense.
        /// Pros : no double buffering
        /// Cons : introduce GC pressure ?
        new_col->reserve(8);
        new_columns.emplace_back(std::move(new_col));
    }

    columns.swap(new_columns);
    assert(columns[0]);
}

AggregateFunctionJavaScriptAdapter::AggregateFunctionJavaScriptAdapter(
    JavaScriptUserDefinedFunctionConfigurationPtr config_,
    const DataTypes & types,
    const Array & params_,
    bool is_changelog_input_,
    size_t max_v8_heap_size_in_bytes_)
    : IAggregateFunctionHelper<AggregateFunctionJavaScriptAdapter>(types, params_)
    , config(config_)
    , num_arguments(types.size())
    , is_changelog_input(is_changelog_input_)
    , max_v8_heap_size_in_bytes(max_v8_heap_size_in_bytes_)
    , blueprint(config->name, config->source)
{
}

String AggregateFunctionJavaScriptAdapter::getName() const
{
    return config->name;
}

DataTypePtr AggregateFunctionJavaScriptAdapter::getReturnType() const
{
    return config->result_type;
}

/// create instance of UDF via function_builder
void AggregateFunctionJavaScriptAdapter::create(AggregateDataPtr __restrict place) const
{
    V8::checkHeapLimit(blueprint.isolate.get(), max_v8_heap_size_in_bytes);
    new (place) Data(blueprint, config->arguments, is_changelog_input);
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
    Arena * arena,
    const IColumn * delta_col) const
{
    IAggregateFunctionHelper<AggregateFunctionJavaScriptAdapter>::addBatchLookupTable8(
        row_begin, row_end, map, place_offset, init, key, columns, arena, delta_col);

    for (size_t cur = row_begin; cur < row_end; ++cur)
        flush(map[key[cur]] + place_offset);
}

void AggregateFunctionJavaScriptAdapter::add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const
{
    this->data(place).add(columns, row_num);
}

void AggregateFunctionJavaScriptAdapter::negate(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const
{
    this->data(place).negate(columns, row_num);
}

void AggregateFunctionJavaScriptAdapter::merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const
{
    auto & data = this->data(place);
    const auto & other = this->data(rhs);

    if (other.serialize_func.IsEmpty() || data.merge_func.IsEmpty())
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "JavaScript UDA doesn't define 'serialize' or 'merge' function. Can't do proper state merge");

    String state;
    auto get_other_state_func = [&](v8::Isolate * isolate_, v8::Local<v8::Context> & local_ctx, v8::TryCatch & try_catch) {
        /// Get the state from rhs. isolate, context shall use (rhs) others
        /// FIXME, for now, all threads share the same isolate and context
        /// When we support multiple isolates, we may need find another way to merge
        v8::Local<v8::Object> other_local_obj = v8::Local<v8::Object>::New(isolate_, other.uda_instance);
        v8::Local<v8::Context> other_local_ctx = v8::Local<v8::Context>::New(isolate_, local_ctx);
        v8::Local<v8::Function> other_local_func = v8::Local<v8::Function>::New(isolate_, other.serialize_func);

        /// Execute the state and get aggregate state
        v8::Local<v8::Value> res;
        if (!other_local_func->Call(other_local_ctx, other_local_obj, 0, nullptr).ToLocal(&res))
            V8::throwException(
                isolate_,
                try_catch,
                ErrorCodes::UDF_INTERNAL_ERROR,
                "Failed to invoke 'serialize' function of JavaScript UDA in 'merge' function");

        state = V8::from_v8<String>(isolate_, res);
    };
    V8::run(blueprint.isolate.get(), blueprint.global_context, std::move(get_other_state_func));

    /// Merge the state
    auto merge_func = [&](v8::Isolate * isolate_, v8::Local<v8::Context> & ctx, v8::TryCatch & try_catch) {
        v8::Local<v8::Object> local_obj = v8::Local<v8::Object>::New(isolate_, data.uda_instance);
        v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate_, data.merge_func);

        std::vector<v8::Local<v8::Value>> argv;
        argv.reserve(1);
        argv.emplace_back(V8::to_v8(isolate_, state));

        v8::Local<v8::Value> res;
        if (!local_func->Call(ctx, local_obj, static_cast<int>(argv.size()), argv.data()).ToLocal(&res))
            V8::throwException(
                isolate_, try_catch, ErrorCodes::UDF_INTERNAL_ERROR, "Failed to invoke 'merge' function of JavaScript UDA");
    };
    V8::run(blueprint.isolate.get(), blueprint.global_context, std::move(merge_func));
}

size_t AggregateFunctionJavaScriptAdapter::getEmitTimes(AggregateDataPtr __restrict place) const
{
    /// Only when UDA has its own emit strategy, it then make sense to check `emit_times`
    return blueprint.has_user_defined_emit_strategy ? this->data(place).emit_times : 0;
}

size_t AggregateFunctionJavaScriptAdapter::flush(AggregateDataPtr __restrict place) const

{
    /// First, get instance of UDF (part of the Aggregate Data) and prepare JavaScript execution context
    auto & data = this->data(place);
    size_t emit_times = 0;

    if (data.columns.empty() || data.columns[0]->empty())
        return emit_times;

    auto process_func = [&](v8::Isolate * isolate_, v8::Local<v8::Context> & ctx, v8::TryCatch & try_catch) {
        v8::Local<v8::Object> local_obj = v8::Local<v8::Object>::New(isolate_, data.uda_instance);
        v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate_, data.process_func);

        /// Second, convert the input column into the corresponding object used by UDF
        /// remove the _tp_delta column if the input stream is not changelog
        auto column_size = is_changelog_input ? config->arguments.size() : config->arguments.size() - 1;
        auto argv = V8::prepareArguments(isolate_, std::span(config->arguments.begin(), column_size), data.columns);

        /// Third, execute the UDF and get aggregate state (only support the final state now, intermediate state is not supported
        v8::Local<v8::Value> res;
        if (!local_func->Call(ctx, local_obj, static_cast<int>(column_size), argv.data()).ToLocal(&res))
            V8::throwException(
                isolate_,
                try_catch,
                ErrorCodes::AGGREGATE_FUNCTION_THROW,
                "Failed to invoke JavaScript user defined aggregation function : {}",
                config->name);

        /// Forth, check if the UDA should emit only it has emit strategy
        if (blueprint.has_user_defined_emit_strategy && !res->IsUndefined())
            emit_times = V8::from_v8<size_t>(isolate_, res);
    };

    V8::run(blueprint.isolate.get(), blueprint.global_context, std::move(process_func));

    data.emit_times = emit_times;
    data.reinitCache();
    return emit_times;
}

/// Serialize the result related field of Aggregate Data
void AggregateFunctionJavaScriptAdapter::serialize(
    ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const
{
    const auto & data = this->data(place);

    if (data.serialize_func.IsEmpty())
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "JavaScript UDA doesn't define 'serialize' function. Can't do proper state serialization");

    auto serialize_func = [&](v8::Isolate * isolate_, v8::Local<v8::Context> & ctx, v8::TryCatch & try_catch) {
        v8::Local<v8::Object> local_obj = v8::Local<v8::Object>::New(isolate_, data.uda_instance);
        v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate_, data.serialize_func);

        /// Execute the serialize() func and get aggregate state (only support the final state now, intermediate state is not supported
        v8::Local<v8::Value> res;
        if (!local_func->Call(ctx, local_obj, 0, nullptr).ToLocal(&res))
            V8::throwException(
                isolate_, try_catch, ErrorCodes::AGGREGATE_FUNCTION_THROW, "Failed to invoke 'serialize' function of JavaScript UDA");

        String state = V8::from_v8<String>(blueprint.isolate.get(), res);
        writeStringBinary(state, buf);
    };
    V8::run(blueprint.isolate.get(), blueprint.global_context, serialize_func);
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

    auto deserialize_func = [&](v8::Isolate * isolate_, v8::Local<v8::Context> & ctx, v8::TryCatch & try_catch) {
        v8::Local<v8::Object> local_obj = v8::Local<v8::Object>::New(isolate_, data.uda_instance);
        v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate_, data.deserialize_func);

        /// convert state to v8 string
        std::vector<v8::Local<v8::Value>> argv;
        argv.reserve(1);
        argv.emplace_back(V8::to_v8(isolate_, state));

        /// init the UDF with state function
        v8::Local<v8::Value> res;
        if (!local_func->Call(ctx, local_obj, static_cast<int>(argv.size()), argv.data()).ToLocal(&res))
            V8::throwException(
                isolate_,
                try_catch,
                ErrorCodes::UDF_INTERNAL_ERROR,
                "Failed to invoke 'deserialize' function of JavaScript UDA");
    };
    V8::run(blueprint.isolate.get(), blueprint.global_context, std::move(deserialize_func));
}

void AggregateFunctionJavaScriptAdapter::insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const
{
    auto & data = this->data(place);
    if (blueprint.has_user_defined_emit_strategy && data.emit_times == 0)
        return;

    /// Flush what we have if there is still buffered data
    /// Actually we shall be able to assert, the buffer data is empty
    assert(data.columns[0]->empty());
    /// flush(place);

    auto finalize_func = [&](v8::Isolate * isolate_, v8::Local<v8::Context> & ctx, v8::TryCatch & try_catch) {
        v8::Local<v8::Object> local_obj = v8::Local<v8::Object>::New(isolate_, data.uda_instance);
        v8::Local<v8::Function> local_func = v8::Local<v8::Function>::New(isolate_, data.finalize_func);

        /// Execute the state() func and get aggregate state (only support the final state now, intermediate state is not supported
        v8::Local<v8::Value> res;
        if (!local_func->Call(ctx, local_obj, 0, nullptr).ToLocal(&res))
            V8::throwException(
                isolate_, try_catch, ErrorCodes::UDF_INTERNAL_ERROR, "Failed to invoke 'finalize' function of JavaScript UDA");

        if (res->IsNullOrUndefined())
            throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Javascript UDA does not return the aggregate result");

        /// FIXME: in future, it should support array as return type, in that case UDA should always return Array(Array) and should document this requirement of UDA.

        /// Whether or not it is a UDA with own emit strategy
        if (hasUserDefinedEmit())
        {
            if (!res->IsArray())
                throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Javascript UDA does not return the aggregate results in array");

            size_t num_of_results = res.As<v8::Array>()->Length();
            if (num_of_results != data.emit_times)
                throw Exception(
                    ErrorCodes::UDF_INTERNAL_ERROR,
                    "Javascript UDA should emit {} times but only return {} results",
                    data.emit_times,
                    num_of_results);
        }

        V8::insertResult(isolate_, to, config->result_type, res, hasUserDefinedEmit());
    };

    V8::run(blueprint.isolate.get(), blueprint.global_context, std::move(finalize_func));
    data.emit_times = 0;
}
}
