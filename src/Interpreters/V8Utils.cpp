#include "V8Utils.h"

#include <DataTypes/ConvertV8DataTypes.h>

#include <Core/DecimalFunctions.h>
#include <Functions/FunctionsConversion.h>


#define FOR_BASIC_NUMERIC_TYPES(M) \
    M(UInt8) \
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
namespace
{
template <typename In>
DateTime64::NativeType toDateTime64(In source)
{
    const auto & time_zone = DateLUT::instance();
    ToDateTime64Transform transform(3);
    return transform.execute(static_cast<In>(source), time_zone);
}

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

/// convert the input column to v8::Array
v8::Local<v8::Array> fillV8Array(v8::Isolate * isolate, const DataTypePtr & arg_type, const MutableColumnPtr & column)
{
    /// Map proton data type to v8 data type.
    auto arg_type_id = arg_type->getTypeId();

    v8::EscapableHandleScope scope(isolate);
    v8::Local<v8::Context> context = isolate->GetCurrentContext();
    v8::Local<v8::Array> result = v8::Array::New(isolate);

    switch (arg_type_id)
    {
        case TypeIndex::Bool: {
            for (int i = 0; i < column->size(); i++)
                result->Set(context, i, to_v8(isolate, column->getBool(i))).FromJust();
            break;
        }
        case TypeIndex::String:
        case TypeIndex::FixedString: {
            for (int i = 0; i < column->size(); i++)
                result->Set(context, i, to_v8(isolate, column->getDataAt(i).data, (*column).getDataAt(i).size)).FromJust();
            break;
        }
        case TypeIndex::DateTime64: {
            const auto * source = checkAndGetColumn<ColumnDecimal<DateTime64>>(column.get());
            const auto & scale = reinterpret_cast<const DataTypeDateTime64 *>(arg_type.get())->getScale();
            for (int i = 0; i < column->size(); i++)
            {
                auto dt64 = static_cast<Int64>((*source).getElement(i));
                result->Set(context, i, toV8Date(isolate, toDateTime64(dt64, scale, 3).value)).FromJust();
            }
            break;
        }

#define FOR_DATE(DATE_TYPE_ID, DATE_TYPE_INTERNAL) \
    case (TypeIndex::DATE_TYPE_ID): { \
        const auto * col_date = checkAndGetColumn<ColumnVector<DATE_TYPE_INTERNAL>>(column.get()); \
        for (int i = 0; i < column->size(); i++) \
        { \
            auto d = toDateTime64<DATE_TYPE_INTERNAL>((*col_date).getElement(i)); \
            result->Set(context, i, toV8Date(isolate, d)).FromJust(); \
        } \
        break; \
    }
            FOR_INTERNAL_DATE_TYPES(FOR_DATE)
#undef FOR_DATE

#define DISPATCH(NUMERIC_TYPE_ID) \
    case (TypeIndex::NUMERIC_TYPE_ID): { \
        const auto & internal_data = assert_cast<const ColumnVector<NUMERIC_TYPE_ID> &>(*column).getData(); \
        result = to_v8(isolate, internal_data.begin(), internal_data.end()); \
        break; \
    }
            FOR_BASIC_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "JavaScript UDF does not support data type: {}", arg_type->getName());
    }

    return scope.Escape(result);
}
}

std::vector<v8::Local<v8::Value>> prepareArguments(
    v8::Isolate * isolate, const std::vector<UserDefinedFunctionConfiguration::Argument> & arguments, const MutableColumns & columns)
{
    std::vector<v8::Local<v8::Value>> argv;
    argv.reserve(arguments.size());

    /// input is v8::Array
    for (int i = 0; const auto & arg : arguments)
    {
        argv.emplace_back(fillV8Array(isolate, arg.type, columns[i]));
        i++;
    }
    return argv;
}

void insertResult(v8::Isolate * isolate, IColumn & to, const DataTypePtr & type, bool is_array, v8::Local<v8::Value> & result)
{
    v8::HandleScope scope(isolate);
    v8::Local<v8::Context> context = isolate->GetCurrentContext();

    if (!is_array)
    {
        switch (type->getTypeId())
        {
            case TypeIndex::Bool:
                to.insert(from_v8<bool>(isolate, result));
                break;
            case TypeIndex::UInt8:
                to.insert(from_v8<uint8_t>(isolate, result));
                break;
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
            case TypeIndex::DateTime64: {
                const auto & scale = reinterpret_cast<const DataTypeDateTime64 *>(type.get())->getScale();
                if (!result->IsDate())
                    throw Exception(ErrorCodes::TYPE_MISMATCH, "the UDA does not return 'datetime64' type");
                to.insert(toDateTime64(static_cast<int64_t>(result.As<v8::Date>()->NumberValue(context).ToChecked()), 3, scale));
                break;
            }
            case TypeIndex::Tuple: {
                const auto * tuple_type_ptr = reinterpret_cast<const DataTypeTuple *>(type.get());
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

                    insertResult(isolate, column_tuple.getColumn(i), tuple_type_ptr->getElement(i), is_array, val);
                    i++;
                }
                break;
            }
            default:
                to.insert(type->getDefault());
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
        if (array->Length() == 0)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "UDF return empty Array");

        for (int idx = 0; idx < array->Length(); idx++)
        {
            v8::Local<v8::Value> val = array->Get(context, idx).ToLocalChecked();
            insertResult(isolate, to, type, false, val);
        }
    }
}

void compileSource(
    v8::Isolate * isolate,
    const std::string & func_name,
    const std::string & source,
    const std::function<void(v8::Local<v8::Context> &, v8::TryCatch &, v8::Local<v8::Value> &)> & func)
{
    v8::Locker locker(isolate);
    v8::Isolate::Scope isolate_scope(isolate);
    v8::HandleScope handle_scope(isolate);
    v8::TryCatch try_catch(isolate);
    v8::Local<v8::Context> local_ctx = v8::Context::New(isolate);
    v8::Context::Scope context_scope(local_ctx);

    v8::Local<v8::String> script_code
        = v8::String::NewFromUtf8(isolate, source.data(), v8::NewStringType::kNormal, static_cast<int>(source.size())).ToLocalChecked();

    v8::Local<v8::Script> compiled_script;
    if (!v8::Script::Compile(local_ctx, script_code).ToLocal(&compiled_script))
        throw Exception(ErrorCodes::UDF_COMPILE_ERROR, "the JavaScript UDF {} is invalid", func_name);

    /// Run js code in first time and init some data from them.
    v8::Local<v8::Value> res;
    if (!compiled_script->Run(local_ctx).ToLocal(&res))
        V8::throwException(isolate, try_catch, ErrorCodes::UDF_COMPILE_ERROR, "The JavaScript UDF {} is invalid", func_name);

    v8::Local<v8::Value> obj;
    if (!local_ctx->Global()->Get(local_ctx, to_v8(isolate, func_name)).ToLocal(&obj))
        throw Exception(ErrorCodes::UDF_COMPILE_ERROR, "the JavaScript UDF {} is invalid", func_name);

    if (func)
        func(local_ctx, try_catch, obj);
}

void validateFunctionSource(
    const std::string & func_name,
    const std::string & source,
    std::function<void(v8::Isolate *, v8::Local<v8::Context> &, v8::TryCatch &, v8::Local<v8::Value> &)> func)
{
    UInt64 max_heap_size_in_bytes = 10 * 1024 * 1024;
    UInt64 max_old_gen_size_in_bytes = 8 * 1024 * 1024;

    v8::Isolate::CreateParams isolate_params;
    isolate_params.array_buffer_allocator_shared
        = std::shared_ptr<v8::ArrayBuffer::Allocator>(v8::ArrayBuffer::Allocator::NewDefaultAllocator());
    isolate_params.constraints.ConfigureDefaultsFromHeapSize(0, max_heap_size_in_bytes);
    isolate_params.constraints.set_max_old_generation_size_in_bytes(max_old_gen_size_in_bytes);

    auto deleter = [](v8::Isolate * isolate_) { isolate_->Dispose(); };
    std::unique_ptr<v8::Isolate, void (*)(v8::Isolate *)> isolate_ptr(v8::Isolate::New(isolate_params), deleter);

    compileSource(isolate_ptr.get(), func_name, source, [&](auto & ctx, auto & try_catch, auto & local_obj) {
        func(isolate_ptr.get(), ctx, try_catch, local_obj);
    });
}

void validateAggregationFunctionSource(
    const std::string & func_name, const std::vector<std::string> & required_member_funcs, const std::string & source)
{
    auto validate_member_functions = [&](v8::Isolate * isolate, v8::Local<v8::Context> & ctx, v8::TryCatch &, v8::Local<v8::Value> & obj) {
        if (!obj->IsObject())
            throw Exception(ErrorCodes::UDF_COMPILE_ERROR, "the JavaScript UDA {} is invalid", func_name);

        for (const auto & member_func_name : required_member_funcs)
        {
            v8::Local<v8::Value> function_val;
            if (!obj.As<v8::Object>()->Get(ctx, V8::to_v8(isolate, member_func_name)).ToLocal(&function_val) || !function_val->IsFunction())
                throw Exception(
                    ErrorCodes::UDF_COMPILE_ERROR,
                    "the JavaScript UDA {} is invalid. Missing required member function '{}'",
                    func_name,
                    member_func_name);
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
            throw Exception(ErrorCodes::UDF_COMPILE_ERROR, "the JavaScript UDF {} is invalid", func_name);
    };

    /// For stateless UDF, it is defined as a free function. For example
    /// function plus_one(...) { }
    /// }
    validateFunctionSource(func_name, source, validate_function);
}

void checkHeapLimit(v8::Isolate * isolate, size_t max_v8_heap_size_in_bytes)
{
    v8::Locker locker(isolate);
    v8::Isolate::Scope isolate_scope(isolate);
    v8::HandleScope handle_scope(isolate);
    v8::HandleScope scope(isolate);
    v8::HeapStatistics heap_statistics;
    isolate->GetHeapStatistics(&heap_statistics);

    auto used = heap_statistics.used_heap_size();
    if (used > max_v8_heap_size_in_bytes)
        throw Exception(
            ErrorCodes::UDF_MEMORY_THRESHOLD_EXCEEDED,
            "Current V8 heap size used={} bytes, exceed the javascript_max_memory_bytes={} bytes",
            used,
            max_v8_heap_size_in_bytes);
}
}
}
