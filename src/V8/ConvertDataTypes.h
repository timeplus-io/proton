#pragma once

/// steal from https://github.com/pmed/v8pp/blob/master/v8pp/convert.hpp
#include <Common/Exception.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <typeinfo>
#include <variant>
#include <v8.h>

namespace DB
{
namespace ErrorCodes
{
extern const int TYPE_MISMATCH;
}
namespace V8
{

template <typename T, typename Enable = void>
struct convert;

template <typename T, typename Traits>
class class_;

template <typename T>
struct is_wrapped_class;

// Generic convertor
/*
template<typename T, typename Enable = void>
struct convert
{
	using from_type = T;
	using to_type = v8::Local<v8::Value>;

	static bool is_valid(v8::Isolate* isolate, v8::Local<v8::Value> value);

	static from_type from_v8(v8::Isolate* isolate, v8::Local<v8::Value> value);
	static to_type to_v8(v8::Isolate* isolate, T const& value);
};
*/

struct invalid_argument : DB::Exception
{
    invalid_argument(v8::Isolate * isolate, v8::Local<v8::Value> value, const String & expected_type);
};

// converter specializations for string types
template <typename Str>
struct convert<Str, typename std::enable_if<fmt::detail::is_string<Str>::value>::type>
{
    using Char = typename Str::value_type;
    using Traits = typename Str::traits_type;

    static_assert(sizeof(Char) <= sizeof(uint16_t), "only UTF-8 and UTF-16 strings are supported");

    // A string that converts to Char const*
    struct convertible_string : std::basic_string<Char, Traits>
    {
        using base_class = std::basic_string<Char, Traits>;
        using base_class::base_class;
        operator Char const *() const { return this->c_str(); }
    };

    using from_type = convertible_string;
    using to_type = v8::Local<v8::String>;

    static bool is_valid(v8::Isolate *, v8::Local<v8::Value> value) { return !value.IsEmpty() && (value->IsString() || value->IsNumber()); }

    static from_type from_v8(v8::Isolate * isolate, v8::Local<v8::Value> value)
    {
        if (!is_valid(isolate, value))
        {
            throw invalid_argument(isolate, value, "String");
        }

        if constexpr (sizeof(Char) == 1)
        {
            v8::String::Utf8Value const str(isolate, value);
            return from_type(reinterpret_cast<Char const *>(*str), str.length());
        }
        else
        {
            v8::String::Value const str(isolate, value);
            return from_type(reinterpret_cast<Char const *>(*str), str.length());
        }
    }

    static to_type to_v8(v8::Isolate * isolate, std::basic_string_view<Char, Traits> value)
    {
        if constexpr (sizeof(Char) == 1)
        {
            return v8::String::NewFromUtf8(
                       isolate, reinterpret_cast<char const *>(value.data()), v8::NewStringType::kNormal, static_cast<int>(value.size()))
                .ToLocalChecked();
        }
        else
        {
            return v8::String::NewFromTwoByte(
                       isolate,
                       reinterpret_cast<uint16_t const *>(value.data()),
                       v8::NewStringType::kNormal,
                       static_cast<int>(value.size()))
                .ToLocalChecked();
        }
    }
};

// converter specializations for null-terminated strings
template <>
struct convert<char const *> : convert<std::basic_string_view<char>>
{
};

template <>
struct convert<char16_t const *> : convert<std::basic_string_view<char16_t>>
{
};

// converter specializations for primitive types
template <>
struct convert<bool>
{
    using from_type = bool;
    using to_type = v8::Local<v8::Boolean>;

    static bool is_valid(v8::Isolate *, v8::Local<v8::Value> value) { return !value.IsEmpty() && value->IsBoolean(); }

    static from_type from_v8(v8::Isolate * isolate, v8::Local<v8::Value> value)
    {
        if (!is_valid(isolate, value))
        {
            throw invalid_argument(isolate, value, "Boolean");
        }
#if (V8_MAJOR_VERSION > 7) || (V8_MAJOR_VERSION == 7 && V8_MINOR_VERSION >= 1)
        return value->BooleanValue(isolate);
#else
        return value->BooleanValue(isolate->GetCurrentContext()).FromJust();
#endif
    }

    static to_type to_v8(v8::Isolate * isolate, bool value) { return v8::Boolean::New(isolate, value); }
};

template <typename T>
struct convert<T, typename std::enable_if<std::is_integral<T>::value>::type>
{
    using from_type = T;
    using to_type = v8::Local<v8::Number>;

    static bool is_valid(v8::Isolate *, v8::Local<v8::Value> value) { return !value.IsEmpty() && value->IsNumber(); }

    static from_type from_v8(v8::Isolate * isolate, v8::Local<v8::Value> value)
    {
        if (!is_valid(isolate, value))
        {
            throw invalid_argument(isolate, value, "Number");
        }

        if constexpr (sizeof(T) <= sizeof(uint32_t))
        {
            if constexpr (std::is_signed_v<T>)
            {
                return static_cast<T>(value->Int32Value(isolate->GetCurrentContext()).FromJust());
            }
            else
            {
                return static_cast<T>(value->Uint32Value(isolate->GetCurrentContext()).FromJust());
            }
        }
        else
        {
            return static_cast<T>(value->IntegerValue(isolate->GetCurrentContext()).FromJust());
        }
    }

    static to_type to_v8(v8::Isolate * isolate, T value)
    {
        if constexpr (sizeof(T) <= sizeof(uint32_t))
        {
            if constexpr (std::is_signed_v<T>)
            {
                return v8::Integer::New(isolate, static_cast<int32_t>(value));
            }
            else
            {
                return v8::Integer::NewFromUnsigned(isolate, static_cast<uint32_t>(value));
            }
        }
        else
        {
            //TODO: check value < (1<<std::numeric_limits<double>::digits)-1 to fit in double?
            return v8::Number::New(isolate, static_cast<double>(value));
        }
    }
};

template <typename T>
struct convert<T, typename std::enable_if<std::is_enum<T>::value>::type>
{
    using underlying_type = typename std::underlying_type<T>::type;

    using from_type = T;
    using to_type = typename convert<underlying_type>::to_type;

    static bool is_valid(v8::Isolate * isolate, v8::Local<v8::Value> value) { return convert<underlying_type>::is_valid(isolate, value); }

    static from_type from_v8(v8::Isolate * isolate, v8::Local<v8::Value> value)
    {
        return static_cast<T>(convert<underlying_type>::from_v8(isolate, value));
    }

    static to_type to_v8(v8::Isolate * isolate, T value)
    {
        return convert<underlying_type>::to_v8(isolate, static_cast<underlying_type>(value));
    }
};

template <typename T>
struct convert<T, typename std::enable_if<std::is_floating_point<T>::value>::type>
{
    using from_type = T;
    using to_type = v8::Local<v8::Number>;

    static bool is_valid(v8::Isolate *, v8::Local<v8::Value> value) { return !value.IsEmpty() && value->IsNumber(); }

    static from_type from_v8(v8::Isolate * isolate, v8::Local<v8::Value> value)
    {
        if (!is_valid(isolate, value))
        {
            throw invalid_argument(isolate, value, "Number");
        }

        return static_cast<T>(value->NumberValue(isolate->GetCurrentContext()).FromJust());
    }

    static to_type to_v8(v8::Isolate * isolate, T value) { return v8::Number::New(isolate, value); }
};

// convert std::tuple <-> Array
template <typename... Ts>
struct convert<std::tuple<Ts...>>
{
    using from_type = std::tuple<Ts...>;
    using to_type = v8::Local<v8::Array>;

    static constexpr size_t N = sizeof...(Ts);

    static bool is_valid(v8::Isolate *, v8::Local<v8::Value> value)
    {
        return !value.IsEmpty() && value->IsArray() && value.As<v8::Array>()->Length() == N;
    }

    static from_type from_v8(v8::Isolate * isolate, v8::Local<v8::Value> value)
    {
        if (!is_valid(isolate, value))
        {
            throw invalid_argument(isolate, value, "Tuple");
        }
        return from_v8_impl(isolate, value, std::make_index_sequence<N>{});
    }

    static to_type to_v8(v8::Isolate * isolate, from_type const & value)
    {
        return to_v8_impl(isolate, value, std::make_index_sequence<N>{});
    }

private:
    template <size_t... Is>
    static from_type from_v8_impl(v8::Isolate * isolate, v8::Local<v8::Value> value, std::index_sequence<Is...>)
    {
        v8::HandleScope scope(isolate);
        v8::Local<v8::Context> context = isolate->GetCurrentContext();
        v8::Local<v8::Array> array = value.As<v8::Array>();

        return std::tuple<Ts...>{convert<Ts>::from_v8(isolate, array->Get(context, Is).ToLocalChecked())...};
    }

    template <size_t... Is>
    static to_type to_v8_impl(v8::Isolate * isolate, std::tuple<Ts...> const & value, std::index_sequence<Is...>)
    {
        v8::EscapableHandleScope scope(isolate);
        v8::Local<v8::Context> context = isolate->GetCurrentContext();
        v8::Local<v8::Array> result = v8::Array::New(isolate, N);

        (void)std::initializer_list<bool>{result->Set(context, Is, convert<Ts>::to_v8(isolate, std::get<Is>(value))).FromJust()...};

        return scope.Escape(result);
    }
};

template <typename T>
struct convert<v8::Local<T>>
{
    using from_type = v8::Local<T>;
    using to_type = v8::Local<T>;

    static bool is_valid(v8::Isolate *, v8::Local<v8::Value> value) { return !value.As<T>().IsEmpty(); }

    static v8::Local<T> from_v8(v8::Isolate *, v8::Local<v8::Value> value) { return value.As<T>(); }

    static v8::Local<T> to_v8(v8::Isolate *, v8::Local<T> value) { return value; }
};

template <typename T>
struct convert<T &> : convert<T>
{
};

template <typename T>
struct convert<T const &> : convert<T>
{
};

template <typename T>
auto from_v8(v8::Isolate * isolate, v8::Local<v8::Value> value) -> decltype(convert<T>::from_v8(isolate, value))
{
    return convert<T>::from_v8(isolate, value);
}

template <typename T, typename U>
auto from_v8(v8::Isolate * isolate, v8::Local<v8::Value> value, U const & default_value) -> decltype(convert<T>::from_v8(isolate, value))
{
    using return_type = decltype(convert<T>::from_v8(isolate, value));
    return convert<T>::is_valid(isolate, value) ? convert<T>::from_v8(isolate, value) : static_cast<return_type>(default_value);
}

inline v8::Local<v8::String> to_v8(v8::Isolate * isolate, char const * str)
{
    return convert<std::string_view>::to_v8(isolate, std::string_view(str));
}

inline v8::Local<v8::String> to_v8(v8::Isolate * isolate, char const * str, size_t len)
{
    return convert<std::string_view>::to_v8(isolate, std::string_view(str, len));
}

template <size_t N>
v8::Local<v8::String> to_v8(v8::Isolate * isolate, char const (&str)[N], size_t len = N - 1)
{
    return convert<std::string_view>::to_v8(isolate, std::string_view(str, len));
}

inline v8::Local<v8::String> to_v8(v8::Isolate * isolate, char16_t const * str)
{
    return convert<std::u16string_view>::to_v8(isolate, std::u16string_view(str));
}

inline v8::Local<v8::String> to_v8(v8::Isolate * isolate, char16_t const * str, size_t len)
{
    return convert<std::u16string_view>::to_v8(isolate, std::u16string_view(str, len));
}

template <size_t N>
v8::Local<v8::String> to_v8(v8::Isolate * isolate, char16_t const (&str)[N], size_t len = N - 1)
{
    return convert<std::u16string_view>::to_v8(isolate, std::u16string_view(str, len));
}

template <typename T>
auto to_v8(v8::Isolate * isolate, T const & value)
{
    return convert<T>::to_v8(isolate, value);
}

template <typename Iterator>
v8::Local<v8::Array> to_v8(v8::Isolate * isolate, Iterator begin, Iterator end)
{
    v8::EscapableHandleScope scope(isolate);
    v8::Local<v8::Context> context = isolate->GetCurrentContext();
    v8::Local<v8::Array> result = v8::Array::New(isolate, static_cast<int>(std::distance(begin, end)));
    for (uint32_t idx = 0; begin != end; ++begin, ++idx)
    {
        result->Set(context, idx, to_v8(isolate, *begin)).FromJust();
    }
    return scope.Escape(result);
}

template <typename T>
v8::Local<v8::Array> to_v8(v8::Isolate * isolate, std::initializer_list<T> const & init)
{
    return to_v8(isolate, init.begin(), init.end());
}

template <typename T>
v8::Local<T> to_local(v8::Isolate * isolate, v8::PersistentBase<T> const & handle)
{
    if (handle.IsWeak())
    {
        return v8::Local<T>::New(isolate, handle);
    }
    else
    {
        return *reinterpret_cast<v8::Local<T> *>(const_cast<v8::PersistentBase<T> *>(&handle));
    }
}

inline invalid_argument::invalid_argument(v8::Isolate * isolate, v8::Local<v8::Value> value, const DB::String & expected_type)
    : DB::Exception(
        ErrorCodes::TYPE_MISMATCH,
        "Expected {}, typeof={}",
        expected_type,
        value.IsEmpty() ? "<empty>" : from_v8<std::string>(isolate, value->TypeOf(isolate)))
{
}
}
}