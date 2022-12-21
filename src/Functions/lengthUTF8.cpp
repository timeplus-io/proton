#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringOrArrayToT.h>
#include <Common/UTF8Helpers.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/** If the string is UTF-8 encoded text, it returns the length of the text in code points.
  * (not in characters: the length of the text "ё" can be either 1 or 2, depending on the normalization)
 * (not in characters: the length of the text "" can be either 1 or 2, depending on the normalization)
  * Otherwise, the behavior is undefined.
  */
struct LengthUTF8Impl
{
    static constexpr auto is_fixed_to_constant = false;

    static void vector(const ColumnString::Chars & data, const ColumnString::Offsets & offsets, PaddedPODArray<UInt64> & res)
    {
        size_t size = offsets.size();

        ColumnString::Offset prev_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            res[i] = UTF8::countCodePoints(&data[prev_offset], offsets[i] - prev_offset - 1);
            prev_offset = offsets[i];
        }
    }

    static void vectorFixedToConstant(const ColumnString::Chars & /*data*/, size_t /*n*/, UInt64 & /*res*/)
    {
    }

    static void vectorFixedToVector(const ColumnString::Chars & data, size_t n, PaddedPODArray<UInt64> & res)
    {
        size_t size = data.size() / n;

        for (size_t i = 0; i < size; ++i)
        {
            res[i] = UTF8::countCodePoints(&data[i * n], n);
        }
    }

    [[noreturn]] static void array(const ColumnString::Offsets &, PaddedPODArray<UInt64> &)
    {
        throw Exception("Cannot apply function length_utf8 to array argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    [[noreturn]] static void uuid(const ColumnUUID::Container &, size_t &, PaddedPODArray<UInt64> &)
    {
        throw Exception("Cannot apply function length_utf8 to uuid argument", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
};

struct NameLengthUTF8
{
    static constexpr auto name = "length_utf8";
};
using FunctionLengthUTF8 = FunctionStringOrArrayToT<LengthUTF8Impl, NameLengthUTF8, UInt64>;

}

REGISTER_FUNCTION(LengthUTF8)
{
    factory.registerFunction<FunctionLengthUTF8>();

    /// Compatibility aliases.
    factory.registerAlias("char_length", "length_utf8", FunctionFactory::CaseInsensitive);
    factory.registerAlias("character_length", "length_utf8", FunctionFactory::CaseInsensitive);
}

}
