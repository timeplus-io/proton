#include <V8/ConvertDataTypes.h>
#include <V8/Modules/Console.h>
#include <V8/Modules/DictionaryAccess/DictionaryAccess.h>
#include <V8/PkuSupport.h>
#include <V8/Utils.h>

#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/FunctionsConversion.h>
#include <base/getMemoryAmount.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>

#include <random>


#define FOR_V8_BASIC_NUMERIC_TYPES(M) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64) \
    M(Int8) \
    M(Int16) \
    M(Int32) \
    M(Int64) \
    M(Float32) \
    M(Float64)

#define FOR_INTERNAL_DATE_TYPES(M) \
    M(Date, UInt16) \
    M(Date32, Int32) \
    M(DateTime, UInt32)

namespace DB
{
namespace ErrorCodes
{
extern const int AGGREGATE_FUNCTION_THROW;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int NOT_IMPLEMENTED;
extern const int UDF_COMPILE_ERROR;
extern const int UDF_MEMORY_THRESHOLD_EXCEEDED;
}

namespace V8
{
template <typename In>
DateTime64::NativeType toDateTime64(In source)
{
    const auto & time_zone = DateLUT::instance();
    ToDateTime64Transform transform(3);
    return transform.execute(static_cast<In>(source), time_zone);
}

template DateTime64::NativeType DB::V8::toDateTime64<UInt16>(UInt16);

DateTime64 toDateTime64(int64_t value, uint32_t source_scale, uint32_t target_scale)
{
    int32_t delta_scale = target_scale - source_scale;
    if (delta_scale >= 0)
        return DecimalUtils::decimalFromComponents<Decimal64>(value, 0, delta_scale);
    else
    {
        int64_t target = value / DecimalUtils::scaleMultiplier<int64_t>(-delta_scale);
        return DecimalUtils::decimalFromComponents<Decimal64>(target, 0, 0);
    }
}

v8::Local<v8::Value> toV8Date(v8::Isolate * isolate, double dt)
{
    v8::EscapableHandleScope scope(isolate);
    v8::Local<v8::Context> context = isolate->GetCurrentContext();
    v8::Local<v8::Value> date = v8::Date::New(context, dt).ToLocalChecked();
    return scope.Escape(date);
}

namespace
{

/// convert the input column to v8::Array
v8::Local<v8::Array>
fillV8Array(v8::Isolate * isolate, const DataTypePtr & arg_type, const MutableColumnPtr & column, uint64_t offset, uint64_t size)
{
    /// Map proton data type to v8 data type.
    auto arg_type_id = arg_type->getTypeId();

    v8::EscapableHandleScope scope(isolate);
    v8::Local<v8::Context> context = isolate->GetCurrentContext();
    v8::Local<v8::Array> result = v8::Array::New(isolate, static_cast<int>(size));
    constexpr auto string_size_threshold = 50 * 1024 * 1024;

    assert(offset + size <= column->size());

    switch (arg_type_id)
    {
        case TypeIndex::UInt8:
        {
            if (arg_type->getName() == "bool")
            {
                for (uint32_t i = 0; i < size; i++)
                    result->Set(context, i, to_v8<bool>(isolate, column->getBool(offset + i))).FromJust();
            }
            else
            {
                const auto & internal_data = assert_cast<const ColumnVector<UInt8> &>(*column).getData();
                result = to_v8(isolate, internal_data.begin() + offset, internal_data.begin() + offset + size);
            }
            break;
        }
        case TypeIndex::String:
        case TypeIndex::FixedString:
        {
            for (uint32_t i = 0; i < size; i++)
            {
                auto str = column->getDataAt(offset + i);
                if (str.size > string_size_threshold)
                    LOG_INFO(
                        getLogger("V8"),
                        "V8::fillV8Array try to allocate big string size={} bytes, threshold={} bytes",
                        str.size,
                        string_size_threshold);
                result->Set(context, i, to_v8(isolate, str.data, str.size)).FromJust();
            }
            break;
        }
        case TypeIndex::DateTime64:
        {
            const auto * source = checkAndGetColumn<ColumnDecimal<DateTime64>>(column.get());
            const auto & scale = reinterpret_cast<const DataTypeDateTime64 *>(arg_type.get())->getScale();
            for (uint32_t i = 0; i < size; i++)
            {
                auto dt64 = static_cast<Int64>((*source).getElement(offset + i));
                result->Set(context, i, toV8Date(isolate, toDateTime64(dt64, scale, 3).value)).FromJust();
            }
            break;
        }
        case TypeIndex::Array:
        {
            const auto * array_type_ptr = reinterpret_cast<const DataTypeArray *>(arg_type.get());
            const auto * col_arr = checkAndGetColumn<ColumnArray>(column.get());
            assert(col_arr && array_type_ptr);
            for (uint32_t i = 0; i < size; i++)
            {
                uint64_t elem_offset = col_arr->getOffsets()[offset + i - 1];
                uint64_t elem_size = col_arr->getOffsets()[offset + i] - elem_offset;
                v8::Local<v8::Value> val = fillV8Array(
                    isolate, array_type_ptr->getNestedType(), IColumn::mutate(col_arr->getData().getPtr()), elem_offset, elem_size);
                result->Set(context, i, val).FromJust();
            }
            break;
        }
        case TypeIndex::Nullable:
        {
            const auto * col_nullable = checkAndGetColumn<ColumnNullable>(column.get());
            auto & nested_column = col_nullable->getNestedColumn();
            const auto & null_map = col_nullable->getNullMapData();
            auto nested_type = assert_cast<const DataTypeNullable *>(arg_type.get())->getNestedType();
            MutableColumnPtr mutable_nested_column = IColumn::mutate(nested_column.convertToFullIfNeeded());

            for (uint32_t i = 0; i < size;)
            {
                // deal with continous non-null elements
                uint32_t start = i;
                while (i < size && !null_map[offset + i])
                    i++;
                if (start < i)
                {
                    v8::Local<v8::Array> temp_array = fillV8Array(isolate, nested_type, mutable_nested_column, offset + start, i - start);
                    for (uint32_t j = start; j < i; j++)
                    {
                        result->Set(context, j, temp_array->Get(context, j - start).ToLocalChecked()).FromJust();
                    }
                }

                // deal with continous null elements
                start = i;
                while (i < size && null_map[offset + i])
                    i++;
                for (uint32_t j = start; j < i; j++)
                {
                    result->Set(context, j, v8::Null(isolate)).FromJust();
                }
            }
            break;
        }

#define FOR_DATE(DATE_TYPE_ID, DATE_TYPE_INTERNAL) \
    case (TypeIndex::DATE_TYPE_ID): \
    { \
        const auto * col_date = checkAndGetColumn<ColumnVector<DATE_TYPE_INTERNAL>>(column.get()); \
        for (uint32_t i = 0; i < size; i++) \
        { \
            auto d = toDateTime64<DATE_TYPE_INTERNAL>((*col_date).getElement(offset + i)); \
            result->Set(context, i, toV8Date(isolate, d)).FromJust(); \
        } \
        break; \
    }
            FOR_INTERNAL_DATE_TYPES(FOR_DATE)
#undef FOR_DATE

#define DISPATCH(NUMERIC_TYPE_ID) \
    case (TypeIndex::NUMERIC_TYPE_ID): \
    { \
        const auto & internal_data = assert_cast<const ColumnVector<NUMERIC_TYPE_ID> &>(*column).getData(); \
        result = to_v8(isolate, internal_data.begin() + offset, internal_data.begin() + offset + size); \
        break; \
    }
            FOR_V8_BASIC_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "JavaScript UDF does not support data type: {}", arg_type->getName());
    }

    return scope.Escape(result);
}
}

std::vector<v8::Local<v8::Value>> prepareArguments(
    v8::Isolate * isolate,
    const std::span<const cluster::protocol::UserDefinedFunctionDescriptor::Argument> arguments,
    const MutableColumns & columns)
{
    std::vector<v8::Local<v8::Value>> argv;
    argv.reserve(arguments.size());

    /// input is v8::Array
    for (int i = 0; const auto & arg : arguments)
    {
        argv.emplace_back(fillV8Array(isolate, DB::DataTypeFactory::instance().get(arg.type), columns[i], 0, columns[i]->size()));
        ++i;
    }
    return argv;
}

void insertResult(v8::Isolate * isolate, IColumn & to, const DataTypePtr & result_type, v8::Local<v8::Value> & result, bool is_result_array)
{
    v8::HandleScope scope(isolate);
    v8::Local<v8::Context> context = isolate->GetCurrentContext();

    if (!is_result_array)
    {
        auto result_type_id = result_type->getTypeId();
        switch (result_type_id)
        {
            case TypeIndex::UInt8:
            {
                if (result_type->getName() == "bool" && result->IsBoolean())
                {
                    /// Internally we store bool as UInt8
                    to.insert(from_v8<bool>(isolate, result));
                }
                else if (result->IsBoolean())
                {
                    // Special case for boolean values in UInt8 columns that aren't explicitly bool type
                    to.insert(result->BooleanValue(isolate));
                }
                else
                {
                    to.insert(from_v8<uint8_t>(isolate, result));
                }
                break;
            }
            case TypeIndex::UInt16:
                to.insert(from_v8<uint16_t>(isolate, result));
                break;
            case TypeIndex::UInt32:
                to.insert(from_v8<uint32_t>(isolate, result));
                break;
            case TypeIndex::UInt64:
                to.insert(from_v8<uint64_t>(isolate, result));
                break;
            case TypeIndex::Int8:
                to.insert(from_v8<int8_t>(isolate, result));
                break;
            case TypeIndex::Int16:
                to.insert(from_v8<int16_t>(isolate, result));
                break;
            case TypeIndex::Int32:
                to.insert(from_v8<int32_t>(isolate, result));
                break;
            case TypeIndex::Int64:
                to.insert(from_v8<int64_t>(isolate, result));
                break;
            case TypeIndex::Float32:
                to.insert(from_v8<float>(isolate, result));
                break;
            case TypeIndex::Float64:
                to.insert(from_v8<double>(isolate, result));
                break;
            case TypeIndex::String:
            case TypeIndex::FixedString:
                to.insert(from_v8<String>(isolate, result));
                break;
            case TypeIndex::Nullable:
            {
                if (result->IsNull())
                {
                    /// Directly handle null value
                    to.insert(Null());
                }
                else if (result->IsArray())
                {
                    /// Handle array case as before
                    auto array_result = result.As<v8::Array>();
                    uint32_t len = array_result->Length();
                    const auto & nested_type = assert_cast<const DataTypeNullable &>(*result_type).getNestedType();
                    for (uint32_t i = 0; i < len; i++)
                    {
                        auto elt_obj = array_result->Get(context, i).ToLocalChecked();
                        if (elt_obj->IsNull())
                            to.insert(Null());
                        else
                            insertResult(isolate, to, nested_type, elt_obj, false);
                    }
                }
                else
                {
                    /// Handle a single non-null value
                    const auto & nested_type = assert_cast<const DataTypeNullable &>(*result_type).getNestedType();
                    insertResult(isolate, to, nested_type, result, false);
                }
                break;
            }
            case TypeIndex::DateTime64:
            {
                const auto & scale = reinterpret_cast<const DataTypeDateTime64 *>(result_type.get())->getScale();
                
                if (result->IsDate())
                {
                    // Handle Date object directly
                    to.insert(toDateTime64(static_cast<int64_t>(result.As<v8::Date>()->ValueOf()), 3, scale));
                }
                else if (result->IsNumber())
                {
                    // If it's a number, assume it's a millisecond timestamp
                    to.insert(toDateTime64(static_cast<int64_t>(result->NumberValue(context).ToChecked()), 3, scale));
                }
                else if (result->IsString())
                {
                    // Parse string as date using JavaScript Date constructor
                    v8::Local<v8::Function> date_constructor = v8::Local<v8::Function>::Cast(
                        context->Global()->Get(context, V8::to_v8(isolate, "Date")).ToLocalChecked());
                    
                    v8::Local<v8::Value> args[] = {result};
                    v8::MaybeLocal<v8::Object> date_obj = date_constructor->NewInstance(context, 1, args);
                    
                    if (!date_obj.IsEmpty())
                    {
                        v8::Local<v8::Object> date = date_obj.ToLocalChecked();
                        if (!date->IsNativeError()) // Check if parsing was successful
                        {
                            v8::Local<v8::Function> get_time = v8::Local<v8::Function>::Cast(
                                date->Get(context, V8::to_v8(isolate, "getTime")).ToLocalChecked());
                            
                            v8::Local<v8::Value> time_value = get_time->Call(context, date, 0, nullptr).ToLocalChecked();
                            if (time_value->IsNumber())
                            {
                                double timestamp_ms = time_value->NumberValue(context).ToChecked();
                                to.insert(toDateTime64(static_cast<int64_t>(timestamp_ms), 3, scale));
                            }
                            else
                            {
                                throw Exception(
                                    ErrorCodes::TYPE_MISMATCH,
                                    "Failed to convert string to DateTime64: invalid time value");
                            }
                        }
                        else
                        {
                            throw Exception(
                                ErrorCodes::TYPE_MISMATCH,
                                "Failed to convert string to DateTime64: invalid date format");
                        }
                    }
                    else
                    {
                        throw Exception(
                            ErrorCodes::TYPE_MISMATCH,
                            "Failed to convert string to DateTime64: could not create Date object");
                    }
                }
                else
                {
                    throw Exception(
                        ErrorCodes::TYPE_MISMATCH,
                        "Expected Date, Number, or String for DateTime64 type, got {}",
                        from_v8<std::string>(isolate, result->TypeOf(isolate)));
                }
                break;
            }
#define STRINGIFY_HELPER(x) #x
#define STRINGIFY(x) STRINGIFY_HELPER(x)
#define FOR_DATE(DATE_TYPE_ID, DATE_TYPE_INTERNAL) \
    case (TypeIndex::DATE_TYPE_ID): \
    { \
        if (result->IsDate()) \
        { \
            double datetime64_v = result.As<v8::Date>()->NumberValue(context).ToChecked(); \
            const auto & time_zone = DateLUT::instance("UTC"); \
            FromDateTime64Transform<To##DATE_TYPE_ID##Impl> transform(3); \
            DATE_TYPE_INTERNAL res = transform.execute(static_cast<DateTime64::NativeType>(datetime64_v), time_zone); \
            to.insert(res); \
        } \
        else if (result->IsNumber()) \
        { \
            double ms_timestamp = result->NumberValue(context).ToChecked(); \
            const auto & time_zone = DateLUT::instance("UTC"); \
            FromDateTime64Transform<To##DATE_TYPE_ID##Impl> transform(3); \
            DATE_TYPE_INTERNAL res = transform.execute(static_cast<DateTime64::NativeType>(ms_timestamp), time_zone); \
            to.insert(res); \
        } \
        else if (result->IsString()) \
        { \
            /* Parse string as date using JavaScript Date constructor */ \
            v8::Local<v8::Function> date_constructor = v8::Local<v8::Function>::Cast( \
                context->Global()->Get(context, V8::to_v8(isolate, "Date")).ToLocalChecked()); \
            \
            v8::Local<v8::Value> args[] = {result}; \
            v8::MaybeLocal<v8::Object> date_obj = date_constructor->NewInstance(context, 1, args); \
            \
            if (!date_obj.IsEmpty()) \
            { \
                v8::Local<v8::Object> date = date_obj.ToLocalChecked(); \
                if (!date->IsNativeError()) \
                { \
                    v8::Local<v8::Function> get_time = v8::Local<v8::Function>::Cast( \
                        date->Get(context, V8::to_v8(isolate, "getTime")).ToLocalChecked()); \
                    \
                    v8::Local<v8::Value> time_value = get_time->Call(context, date, 0, nullptr).ToLocalChecked(); \
                    if (time_value->IsNumber()) \
                    { \
                        double timestamp_ms = time_value->NumberValue(context).ToChecked(); \
                        const auto & time_zone = DateLUT::instance("UTC"); \
                        FromDateTime64Transform<To##DATE_TYPE_ID##Impl> transform(3); \
                        DATE_TYPE_INTERNAL res = transform.execute(static_cast<DateTime64::NativeType>(timestamp_ms), time_zone); \
                        to.insert(res); \
                    } \
                    else \
                    { \
                        throw Exception( \
                            ErrorCodes::TYPE_MISMATCH, \
                            "Failed to convert string to " STRINGIFY(DATE_TYPE_ID) ": invalid time value"); \
                    } \
                } \
                else \
                { \
                    throw Exception( \
                        ErrorCodes::TYPE_MISMATCH, \
                        "Failed to convert string to " STRINGIFY(DATE_TYPE_ID) ": invalid date format"); \
                } \
            } \
            else \
            { \
                throw Exception( \
                    ErrorCodes::TYPE_MISMATCH, \
                    "Failed to convert string to " STRINGIFY(DATE_TYPE_ID) ": could not create Date object"); \
            } \
        } \
        else \
        { \
            throw Exception(ErrorCodes::TYPE_MISMATCH, \
                "Expected Date, Number, or String for " STRINGIFY(DATE_TYPE_ID) " type, got {}", \
                from_v8<std::string>(isolate, result->TypeOf(isolate))); \
        } \
        break; \
    }
                FOR_INTERNAL_DATE_TYPES(FOR_DATE)
#undef FOR_DATE
#undef STRINGIFY
#undef STRINGIFY_HELPER

            case TypeIndex::Tuple:
            {
                const auto * tuple_type_ptr = reinterpret_cast<const DataTypeTuple *>(result_type.get());
                if (!result->IsObject())
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "the result result is Tuple type, but the UDF does not return Object");

                auto & column_tuple = assert_cast<ColumnTuple &>(to);
                int i = 0;
                for (const auto & name : tuple_type_ptr->getElementNames())
                {
                    v8::Local<v8::Value> val;
                    if (!result.As<v8::Object>()->Get(isolate->GetCurrentContext(), to_v8(isolate, name)).ToLocal(&val)
                        || val->IsUndefined())
                        throw Exception(ErrorCodes::TYPE_MISMATCH, "invalid result, the UDF does not return object with {} property", name);

                    insertResult(isolate, column_tuple.getColumn(i), tuple_type_ptr->getElement(i), val, is_result_array);
                    i++;
                }
                break;
            }
            case TypeIndex::Array:
            {
                const auto * array_type_ptr = reinterpret_cast<const DataTypeArray *>(result_type.get());
                if (result.IsEmpty() || !result->IsArray())
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "UDF expect return Array but got {}",
                        from_v8<std::string>(isolate, result->TypeOf(isolate)));

                auto & column_array = assert_cast<ColumnArray &>(to);
                insertResult(isolate, column_array.getData(), array_type_ptr->getNestedType(), result, true);
                column_array.getOffsets().push_back(column_array.getOffsets().back() + result.As<v8::Array>()->Length());
                break;
            }
            default:
                to.insert(result_type->getDefault());
        }
    }
    else
    {
        if (result.IsEmpty() || !result->IsArray())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "UDF should return Array but got {}",
                from_v8<std::string>(isolate, result->TypeOf(isolate)));

        v8::Local<v8::Array> array = result.As<v8::Array>();
        uint32_t len = array->Length();
        for (uint32_t idx = 0; idx < len; idx++)
        {
            v8::Local<v8::Value> val = array->Get(context, idx).ToLocalChecked();
            insertResult(isolate, to, result_type, val, false);
        }
    }
}

void compileSource(
    v8::Isolate * isolate,
    const std::string & func_name,
    const std::string & source,
    std::function<void(v8::Isolate *, v8::Local<v8::Context> &, v8::TryCatch &, v8::Local<v8::Value> &)> func)
{
    [[maybe_unused]] PkuSupport::ThreadPkruGuard pkru_guard;
    v8::Locker locker(isolate);
    v8::Isolate::Scope isolate_scope(isolate);
    v8::HandleScope handle_scope(isolate);
    v8::TryCatch try_catch(isolate);
    /// try_catch.SetVerbose(true);
    try_catch.SetCaptureMessage(true);

    /// Setup global object
    v8::Local<v8::ObjectTemplate> global = v8::ObjectTemplate::New(isolate);
    v8::Local<v8::Context> local_ctx = v8::Context::New(isolate, nullptr, global);
    v8::Context::Scope context_scope(local_ctx);

    /// Setup 'console' object
    installConsole(isolate, local_ctx, func_name);

    /// Setup dictionary access functions (getValue, setValue, getCardinality)
    DB::V8::DictionaryAccess::installJS(isolate, local_ctx);

    v8::Local<v8::String> script_code
        = v8::String::NewFromUtf8(isolate, source.data(), v8::NewStringType::kNormal, static_cast<int>(source.size())).ToLocalChecked();

    v8::Local<v8::Script> compiled_script;
    if (!v8::Script::Compile(local_ctx, script_code).ToLocal(&compiled_script))
        throw Exception(
            ErrorCodes::UDF_COMPILE_ERROR,
            "the JavaScript UDF {} is invalid. Detail error: {}",
            func_name,
            V8::from_v8<String>(isolate, try_catch.Message()->Get()));

    /// Run js code in first time and init some data from them.
    v8::Local<v8::Value> res;
    if (!compiled_script->Run(local_ctx).ToLocal(&res))
        V8::throwException(
            isolate,
            try_catch,
            ErrorCodes::UDF_COMPILE_ERROR,
            "The JavaScript UDF {} is invalid. Detail error: {}",
            func_name,
            V8::from_v8<String>(isolate, try_catch.Message()->Get()));

    v8::Local<v8::Value> func_val;
    if (!local_ctx->Global()->Get(local_ctx, to_v8(isolate, func_name)).ToLocal(&func_val))
        throw Exception(
            ErrorCodes::UDF_COMPILE_ERROR,
            "the JavaScript UDF {} is invalid. Detail error: {}",
            func_name,
            V8::from_v8<String>(isolate, try_catch.Message()->Get()));

    if (func)
        func(isolate, local_ctx, try_catch, func_val);
}

void validateFunctionSource(
    const std::string & func_name,
    const std::string & source,
    std::function<void(v8::Isolate *, v8::Local<v8::Context> &, v8::TryCatch &, v8::Local<v8::Value> &)> func)
{
    /// FIXME, switch to global isolate allocation / pooling
    UInt64 max_heap_size_in_bytes = static_cast<size_t>(getMemoryAmountOrZeroCached() * 0.6);
    UInt64 max_old_gen_size_in_bytes = static_cast<size_t>(getMemoryAmountOrZeroCached() * 0.6);

    v8::Isolate::CreateParams isolate_params;
    isolate_params.array_buffer_allocator_shared
        = std::shared_ptr<v8::ArrayBuffer::Allocator>(v8::ArrayBuffer::Allocator::NewDefaultAllocator());
    isolate_params.constraints.ConfigureDefaultsFromHeapSize(0, max_heap_size_in_bytes);
    isolate_params.constraints.set_max_old_generation_size_in_bytes(max_old_gen_size_in_bytes);

    [[maybe_unused]] PkuSupport::ThreadPkruGuard pkru_guard;

    auto isolate_deleter = [](v8::Isolate * isolate_) {
        [[maybe_unused]] PkuSupport::ThreadPkruGuard dispose_pkru_guard;
        isolate_->Dispose();
    };
    std::unique_ptr<v8::Isolate, void (*)(v8::Isolate *)> isolate_ptr(v8::Isolate::New(isolate_params), isolate_deleter);

    compileSource(
        isolate_ptr.get(),
        func_name,
        source,
        [&](v8::Isolate * isolate_, v8::Local<v8::Context> & local_ctx, v8::TryCatch & try_catch, v8::Local<v8::Value> & local_obj) {
            func(isolate_, local_ctx, try_catch, local_obj);
        });
}

void validateAggregationFunctionSource(
    const std::string & func_name, const std::vector<std::string> & required_member_funcs, const std::string & source)
{
    auto validate_member_functions =
        [&](v8::Isolate * isolate_, v8::Local<v8::Context> & local_ctx, v8::TryCatch &, v8::Local<v8::Value> & obj) {
            if (!obj->IsObject())
                throw Exception(
                    ErrorCodes::UDF_COMPILE_ERROR,
                    "the JavaScript UDA {} is invalid. The UDA object '{}' is not defined",
                    func_name,
                    func_name);

            auto local_obj = obj.As<v8::Object>();
            for (const auto & member_func_name : required_member_funcs)
            {
                v8::Local<v8::Value> function_val;
                if (!local_obj->Get(local_ctx, V8::to_v8(isolate_, member_func_name)).ToLocal(&function_val) || !function_val->IsFunction())
                    throw Exception(
                        ErrorCodes::UDF_COMPILE_ERROR,
                        "the JavaScript UDA {} is invalid. Missing required member function: '{}'. Please refer to the JavaScript UDA "
                        "documents for details",
                        func_name,
                        member_func_name);
            }

            /// For aggregate function, we like to limit the member functions and `has_customized_emit` boolean or function only
            /// The reason behind this is `mutable data member like [], {} are not safe for aggregation in the same context
            auto property_names = local_obj->GetOwnPropertyNames(local_ctx).ToLocalChecked();
            for (uint32_t i = 0; i < property_names->Length(); ++i)
            {
                auto property_name = property_names->Get(local_ctx, i).ToLocalChecked();
                auto property = local_obj->Get(local_ctx, property_name).ToLocalChecked();
                auto native_property_name = V8::from_v8<String>(isolate_, property_name);
                if (!property->IsFunction() && native_property_name != "has_customized_emit")
                    throw Exception(
                        ErrorCodes::UDF_COMPILE_ERROR,
                        "the JavaScript UDA {} is invalid. It contains non-function data member : '{}'. JavaScript UDA can contain only "
                        "required data members and member functions. Please refer to the JavaScript UDA documents for details",
                        func_name,
                        native_property_name);
            }
        };

    /// For aggregate UDA, it is defined as an object. For example
    /// var second_max = {
    ///    init : function() { ... },
    ///    other member functions ...
    /// }
    auto src = fmt::format("var {}={};", func_name, source);
    validateFunctionSource(func_name, src, validate_member_functions);
}

void validateStatelessFunctionSource(const std::string & func_name, const std::string & source)
{
    auto validate_function = [&](v8::Isolate * isolate, v8::Local<v8::Context> & ctx, v8::TryCatch &, v8::Local<v8::Value> &) {
        v8::Local<v8::Value> function_val;
        if (!ctx->Global()->Get(ctx, V8::to_v8(isolate, func_name)).ToLocal(&function_val) || !function_val->IsFunction())
            throw Exception(
                ErrorCodes::UDF_COMPILE_ERROR,
                "the JavaScript UDF '{}' is invalid since the function '{}' is not defined",
                func_name,
                func_name);
    };

    /// For stateless UDF, it is defined as a free function. For example
    /// function plus_one(...) { }
    /// }
    validateFunctionSource(func_name, source, validate_function);
}

std::string getHeapStatisticsString(v8::HeapStatistics & heap_statistics)
{
    return fmt::format(
        "Total Heap Size: {}\t"
        "Total Heap Size Executable: {}\t"
        "Total Physical Size: {}\t"
        "Total Available Size: {}\t"
        "Used Heap Size: {}\t"
        "Heap Size Limit: {}\t"
        "Malloced Memory: {}\t"
        "External Memory: {}\t"
        "Peak Malloced Memory: {}\t"
        "Does Zap Garbage: {}\t"
        "Number Of Native Contexts: {}\t"
        "Number Of Detached Contexts: {}\t"
        "Total Global Handles Size: {}\t"
        "Used Global Handles Size: {}",
        heap_statistics.total_heap_size(),
        heap_statistics.total_heap_size_executable(),
        heap_statistics.total_physical_size(),
        heap_statistics.total_available_size(),
        heap_statistics.used_heap_size(),
        heap_statistics.heap_size_limit(),
        heap_statistics.malloced_memory(),
        heap_statistics.external_memory(),
        heap_statistics.peak_malloced_memory(),
        heap_statistics.does_zap_garbage(),
        heap_statistics.number_of_native_contexts(),
        heap_statistics.number_of_detached_contexts(),
        heap_statistics.total_global_handles_size(),
        heap_statistics.used_global_handles_size());
}

void checkHeapLimit(v8::Isolate * isolate, size_t max_v8_heap_size_in_bytes, bool log_v8_memory, LoggerPtr logger)
{
    v8::HeapStatistics heap_statistics;
    {
        [[maybe_unused]] PkuSupport::ThreadPkruGuard pkru_guard;
        v8::Locker locker(isolate);
        v8::Isolate::Scope isolate_scope(isolate);
        v8::HandleScope handle_scope(isolate);

        /// TODO(qijun): add v8 heap stat metrics to system profile event capture
        isolate->GetHeapStatistics(&heap_statistics);
    }

    size_t used = heap_statistics.used_heap_size();
    size_t total = heap_statistics.heap_size_limit();
    size_t limit = std::min(total, max_v8_heap_size_in_bytes);

    /// If the used heap size exceeds the soft limit, directly throw an exception
    if (used > limit)
        throw Exception(
            ErrorCodes::UDF_MEMORY_THRESHOLD_EXCEEDED,
            "Current V8 heap size used={} bytes, total={} bytes, javascript_max_memory_bytes={}, exceed the limit={} bytes, v8 heap "
            "stat={{{}}}",
            used,
            total,
            max_v8_heap_size_in_bytes,
            limit,
            V8::getHeapStatisticsString(heap_statistics));

    /// Check if the actual heap limit is lower than the soft limit
    if (total < max_v8_heap_size_in_bytes)
    {
        LOG_WARNING(
            logger,
            "V8 engine cannot allocate the requested memory: actual limit={} bytes, "
            "requested={} bytes. Consider recreating with `settings javascript_max_memory_bytes` set to a lower value. "
            "V8 heap statistics: {{{}}}",
            total,
            max_v8_heap_size_in_bytes,
            getHeapStatisticsString(heap_statistics));
    }
    else if (used > (limit * 0.8))
    {
        /// This is the case where we should notify users to increase settings
        LOG_WARNING(
            logger,
            "V8 heap usage is approaching the limit: used={} bytes ({:.1f}% of limit). "
            "Consider increasing settings javascript_max_memory_bytes (currently {} bytes) "
            "to avoid potential memory errors. V8 heap statistics: {{{}}}",
            used,
            (static_cast<double>(used) / limit) * 100,
            max_v8_heap_size_in_bytes,
            getHeapStatisticsString(heap_statistics));
    }
    else if (log_v8_memory)
        LOG_INFO(
            logger,
            "V8 heap is within the acceptable range: used={} bytes, javascript_max_memory_bytes={} bytes, hard limit={} bytes. "
            "V8 heap statistics: {{{}}}",
            used,
            max_v8_heap_size_in_bytes,
            total,
            getHeapStatisticsString(heap_statistics));
}

size_t nearHeapLimitCallback(void * isolate_, size_t current_heap_limit, size_t initial_heap_limit)
{
    v8::Isolate * isolate = reinterpret_cast<v8::Isolate *>(isolate_);
    [[maybe_unused]] PkuSupport::ThreadPkruGuard pkru_guard;
    v8::Locker locker(isolate);
    v8::Isolate::Scope isolate_scope(isolate);
    v8::HandleScope handle_scope(isolate);

    v8::HeapStatistics heap_statistics;
    isolate->GetHeapStatistics(&heap_statistics);

    size_t used = heap_statistics.used_heap_size();
    size_t total = heap_statistics.heap_size_limit();

    LOG_WARNING(
        getLogger("V8"),
        "V8 heap near limit: used={} bytes, current_heap_limit={} bytes, initial_heap_limit={} bytes. \nV8 heap statistics: {{{}}}",
        used,
        current_heap_limit,
        total,
        getHeapStatisticsString(heap_statistics));
    return 0;
}

void fatalErrorHandle(const char * location, const char * message)
{
    LOG_ERROR(getLogger("V8"), "V8 Fatal: location={}, message={}", location, message);
}
}
}
