#include <IO/PrefixTreeEncode.h>
#include <Common/FloatBits.h>

#include <Common/Exception.h>

#include <cassert>
#include <cmath>

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_DATA;
extern const int NOT_IMPLEMENTED;
}

namespace PrefixTreeEncode
{
namespace
{
const std::string KEY_MAX = "\xff\xff";
constexpr int32_t INT_MIN_BASE = 0x80;
constexpr int32_t INT_MAX_WIDTH = 8;
constexpr int32_t INT_ZERO = INT_MIN_BASE + INT_MAX_WIDTH; /// 136
constexpr int32_t INT_MAX_BASE = 0xfd; /// 253
constexpr int32_t INT_SMALL = INT_MAX_BASE - INT_ZERO - INT_MAX_WIDTH; /// 109

[[maybe_unused]] constexpr int32_t ENCODED_NULL = 0x00;
/// A marker greater than NULL but lower than any other value.
/// This value is not actually ever present in a stored key, but
/// it's used in keys used as span boundaries for index scans.
constexpr char ENCODED_NOT_NULL = 0x01;

constexpr char FLOAT_NAN = ENCODED_NOT_NULL + 1;
constexpr char FLOAT_NEG = FLOAT_NAN + 1;
constexpr char FLOAT_ZERO = FLOAT_NEG + 1;
constexpr char FLOAT_POS = FLOAT_ZERO + 1;
constexpr char FLOAT_NAN_DESC = FLOAT_POS + 1; /// NaN encoded descendingly

constexpr char BYTES_MARKER = 0x12;
constexpr char BYTES_DESC_MARKER = BYTES_MARKER + 1;

/// <term> will be encoded as \x00<term> \x00\x01 for example
/// \x00 will be escaped as \x00\xff
constexpr char ESCAPE = 0x00;
constexpr char ESCAPED_TERM = 0x01;
constexpr char ESCAPED_00 = 0xff;
constexpr char ESCAPED_FF = 0x00;

struct Escapes
{
    char escape;
    char escaped_term;
    char escaped_00;
    char escaped_ff;
    char marker;
};

constexpr Escapes ASCENDING_BYTES_ESCAPES{
    .escape = ESCAPE, .escaped_term = ESCAPED_TERM, .escaped_00 = ESCAPED_00, .escaped_ff = ESCAPED_FF, .marker = BYTES_MARKER};

constexpr Escapes DESCENDING_BYTES_ESCAPES{
    .escape = static_cast<char>(~ESCAPE),
    .escaped_term = static_cast<char>(~ESCAPED_TERM),
    .escaped_00 = static_cast<char>(~ESCAPED_00),
    .escaped_ff = static_cast<char>(~ESCAPED_FF),
    .marker = static_cast<char>(BYTES_DESC_MARKER)};

void onesComplement(char * data, size_t n)
{
    auto w = n / 8;
    if (w > 0)
    {
        auto * bw = reinterpret_cast<uint64_t *>(data);
        for (size_t i = 0; i < w; ++i)
            bw[i] = ~bw[i];
    }

    for (size_t i = w * 8; i < n; ++i)
        data[i] = ~data[i];
}

/// Encodes data using an escaped-based encoding.
inline void encodeStringAscendingWithoutTerminatorOrPrefix(std::string_view data, std::string & encoded)
{
    std::string_view::size_type start_pos = 0, end_pos = 0;
    while (true)
    {
        end_pos = data.find(ESCAPE, start_pos);
        if (end_pos == std::string_view::npos)
        {
            encoded.append(data.data() + start_pos);
            break;
        }

        encoded.append(data.data() + start_pos, (end_pos - start_pos) + 1);
        encoded.push_back(ESCAPED_00);

        start_pos = end_pos + 1;
    }
}

/// The encoded value is terminated with the sequence "\x00\terminator"
inline void encodeStringAscendingWithTerminator(std::string_view data, char terminator, std::string & encoded)
{
    encodeStringAscendingWithoutTerminatorOrPrefix(data, encoded);

    encoded.push_back(ESCAPE);
    encoded.push_back(terminator);
}

inline void encodeStringAscendingWithTerminatorAndPrefix(std::string_view data, char terminator, char prefix, std::string & encoded)
{
    encoded.reserve(data.size() + 8);
    encoded.push_back(prefix);

    encodeStringAscendingWithTerminator(data, terminator, encoded);
}

/// decodes encoded data and appends it to decoded, the remainder of the input data will be carried back
/// by the data input / output param.
/// \return string view of the decoded data
inline std::string_view
decodeStringInternal(std::string_view & data, Escapes escapes, bool expect_marker, bool deep_copy, std::string & decoded)
{
    if (expect_marker)
    {
        if (data.empty() || data[0] != escapes.marker)
            throw Exception(
                ErrorCodes::INVALID_DATA,
                "Invalided string prefix tree encoded data='{}'. Missing the marker=0x{:x}",
                data,
                escapes.marker);

        data = data.substr(1);
    }

    std::string_view result;
    std::string_view::size_type end_pos = 0;

    while (true)
    {
        end_pos = data.find(escapes.escape);
        if (end_pos == std::string_view::npos)
            throw Exception(
                ErrorCodes::INVALID_DATA, "Didn't find terminator=0x{:x} in prefix tree encoded data='{}'", escapes.escape, data);

        if (end_pos + 1 >= data.size())
            throw Exception(ErrorCodes::INVALID_DATA, "Malformed escape in prefix tree encoded data='{}'", data);

        auto v = data[end_pos + 1];
        if (v == escapes.escaped_term)
        {
            if (decoded.empty() && !deep_copy)
            {
                /// There is no escaped data encountered so far, it is a happy path we can avoid
                /// deep copying the source data by slicing the origin encoded data
                result = data.substr(0, end_pos);
            }
            else
            {
                decoded.append(data.substr(0, end_pos));
                result = std::string_view{decoded};
            }

            /// Update data to point the remainder bytes
            data = data.substr(end_pos + 2);
            return result;
        }

        if (v != escapes.escaped_00)
            throw Exception(ErrorCodes::INVALID_DATA, "Unknown escape sequence 0x{:x} 0x{:x} in prefix tree", escapes.escape, v);

        /// decoded [0, end_pos), append the decoded data
        decoded.append(data.substr(0, end_pos));
        decoded.push_back(escapes.escaped_ff);

        /// Move to the next escaped segment
        data = data.substr(end_pos + 2);
    }
}

inline void decodeStringInternalDiscard(std::string_view & data, Escapes escapes, bool expect_marker)
{
    if (expect_marker)
    {
        if (data.empty() || data[0] != escapes.marker)
            throw Exception(
                ErrorCodes::INVALID_DATA,
                "Invalided string prefix tree encoded data='{}'. Missing the marker=0x{:x}",
                data,
                escapes.marker);

        data = data.substr(1);
    }

    std::string_view::size_type end_pos = 0;

    while (true)
    {
        end_pos = data.find(escapes.escape);
        if (end_pos == std::string_view::npos)
            throw Exception(
                ErrorCodes::INVALID_DATA, "Didn't find terminator=0x{:x} in prefix tree encoded data='{}'", escapes.escape, data);

        if (end_pos + 1 >= data.size())
            throw Exception(ErrorCodes::INVALID_DATA, "Malformed escape in prefix tree encoded data='{}'", data);

        auto v = data[end_pos + 1];
        if (v == escapes.escaped_term)
        {
            /// Update data to point the remainder bytes
            data = data.substr(end_pos + 2);
            return;
        }

        if (v != escapes.escaped_00)
            throw Exception(ErrorCodes::INVALID_DATA, "Unknown escape sequence 0x{:x} 0x{:x} in prefix tree", escapes.escape, v);

        /// Move to the next escaped segment
        data = data.substr(end_pos + 2);
    }
}
}

std::string_view encodeStringAscending(std::string_view data, std::string & encoded)
{
    auto current_size = encoded.size();
    encodeStringAscendingWithTerminatorAndPrefix(data, ASCENDING_BYTES_ESCAPES.escaped_term, ASCENDING_BYTES_ESCAPES.marker, encoded);

    return std::string_view{encoded.data() + current_size, encoded.size() - current_size};
}

std::string_view encodeStringDescending(std::string_view data, std::string & encoded)
{
    auto n = encoded.size();
    encodeStringAscending(data, encoded);
    assert(encoded[n] == BYTES_MARKER);
    encoded[n] = BYTES_DESC_MARKER;
    onesComplement(encoded.data() + n + 1, encoded.size() - n - 1);

    return std::string_view{encoded.data() + n, encoded.size() - n};
}

std::string_view decodeStringAscending(std::string_view & data, std::string & decoded)
{
    return decodeStringInternal(data, ASCENDING_BYTES_ESCAPES, /*expect_marker=*/true, /*deep_copy=*/false, decoded);
}

std::string_view decodeStringDescending(std::string_view & data, std::string & decoded)
{
    /// For descending decoding, we always requires a deep copy since we will need further do
    /// ones complement on the decoded data
    decodeStringInternal(data, DESCENDING_BYTES_ESCAPES, /*expect_marker=*/true, /*deep_copy=*/true, decoded);
    onesComplement(decoded.data(), decoded.size());
    return std::string_view{decoded};
}

void decodeStringAscendingDiscard(std::string_view & data)
{
    decodeStringInternalDiscard(data, ASCENDING_BYTES_ESCAPES, /*expect_marker=*/true);
}

void decodeStringDescendingDiscard(std::string_view & data)
{
    decodeStringInternalDiscard(data, DESCENDING_BYTES_ESCAPES, /*expect_marker=*/true);
}

std::string_view encodeVarIntAscending(int64_t n, std::string & encoded)
{
    size_t current_size = encoded.size();
    if (n < 0)
    {
        if (n >= -0xffll)
        {
            encoded.append({static_cast<char>(INT_MIN_BASE + 7), static_cast<char>(n)});
        }
        else if (n >= -0xffffll)
        {
            encoded.append({static_cast<char>(INT_MIN_BASE + 6), static_cast<char>(n >> 8), static_cast<char>(n)});
        }
        else if (n >= -0xff'ffffll)
        {
            encoded.append(
                {static_cast<char>(INT_MIN_BASE + 5), static_cast<char>(n >> 16), static_cast<char>(n >> 8), static_cast<char>(n)});
        }
        else if (n >= -0xffff'ffffll)
        {
            encoded.append(
                {static_cast<char>(INT_MIN_BASE + 4),
                 static_cast<char>(n >> 24),
                 static_cast<char>(n >> 16),
                 static_cast<char>(n >> 8),
                 static_cast<char>(n)});
        }
        else if (n >= -0xff'ffff'ffffll)
        {
            encoded.append(
                {static_cast<char>(INT_MIN_BASE + 3),
                 static_cast<char>(n >> 32),
                 static_cast<char>(n >> 24),
                 static_cast<char>(n >> 16),
                 static_cast<char>(n >> 8),
                 static_cast<char>(n)});
        }
        else if (n >= -0xffff'ffff'ffffll)
        {
            encoded.append(
                {static_cast<char>(INT_MIN_BASE + 2),
                 static_cast<char>(n >> 40),
                 static_cast<char>(n >> 32),
                 static_cast<char>(n >> 24),
                 static_cast<char>(n >> 16),
                 static_cast<char>(n >> 8),
                 static_cast<char>(n)});
        }
        else if (n >= -0xff'ffff'ffff'ffffll)
        {
            encoded.append(
                {static_cast<char>(INT_MIN_BASE + 1),
                 static_cast<char>(n >> 48),
                 static_cast<char>(n >> 40),
                 static_cast<char>(n >> 32),
                 static_cast<char>(n >> 24),
                 static_cast<char>(n >> 16),
                 static_cast<char>(n >> 8),
                 static_cast<char>(n)});
        }
        else
        {
            encoded.append(
                {static_cast<char>(INT_MIN_BASE),
                 static_cast<char>(n >> 56),
                 static_cast<char>(n >> 48),
                 static_cast<char>(n >> 40),
                 static_cast<char>(n >> 32),
                 static_cast<char>(n >> 24),
                 static_cast<char>(n >> 16),
                 static_cast<char>(n >> 8),
                 static_cast<char>(n)});
        }

        return std::string_view{encoded.data() + current_size, encoded.size() - current_size};
    }

    return encodeVarUIntAscending(static_cast<uint64_t>(n), encoded);
}

std::string_view encodeVarIntDescending(int64_t n, std::string & encoded)
{
    return encodeVarIntAscending(~n, encoded);
}

int64_t decodeVarIntAscending(std::string_view & data)
{
    if (data.empty())
        throw Exception(ErrorCodes::INVALID_DATA, "Insufficient bytes to decode varint value");

    /// char is signed, convert to uint8_t first for length
    auto length = static_cast<int32_t>(static_cast<uint8_t>(data[0])) - INT_ZERO;
    if (length < 0)
    {
        length = -length;
        data = data.substr(1);
        if (static_cast<int32_t>(data.size()) < length)
            throw Exception(ErrorCodes::INVALID_DATA, "Insufficient bytes to decode varint value");

        int64_t v = 0;

        /// Use the ones-complement of each encoded byte in order to build
        /// up a positive number, then take the one-complement again to
        /// arrive at our negative value
        /// Use uint8_t to loop data to avoid the the highest bit (negative bit) propagate to int64_t
        /// automatically
        for (uint8_t ch : data.substr(0, length))
            /// ~ch => auto promote to a short
            v = (v << 8) | static_cast<int64_t>(static_cast<uint8_t>(~ch));

        data = data.substr(length);
        return ~v;
    }

    auto uv = decodeVarUIntAscending(data);
    if (uv > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()))
        throw Exception(ErrorCodes::INVALID_DATA, "varint overflowed int64_t");

    return static_cast<int64_t>(uv);
}

int64_t decodeVarIntDescending(std::string_view & data)
{
    auto v = decodeVarIntAscending(data);
    return ~v;
}

std::string_view encodeVarUIntAscending(uint64_t n, std::string & encoded)
{
    auto current_size = encoded.size();
    if (n <= static_cast<uint64_t>(INT_SMALL))
    {
        encoded.push_back(static_cast<char>(INT_ZERO + n));
    }
    else if (n <= 0xffull)
    {
        encoded.append({
            static_cast<char>(INT_MAX_BASE - 7),
            static_cast<char>(n),
        });
    }
    else if (n <= 0xffffull)
    {
        encoded.append({
            static_cast<char>(INT_MAX_BASE - 6),
            static_cast<char>(n >> 8),
            static_cast<char>(n),
        });
    }
    else if (n <= 0xff'ffffull)
    {
        encoded.append({
            static_cast<char>(INT_MAX_BASE - 5),
            static_cast<char>(n >> 16),
            static_cast<char>(n >> 8),
            static_cast<char>(n),
        });
    }
    else if (n <= 0xffff'ffffull)
    {
        encoded.append({
            static_cast<char>(INT_MAX_BASE - 4),
            static_cast<char>(n >> 24),
            static_cast<char>(n >> 16),
            static_cast<char>(n >> 8),
            static_cast<char>(n),
        });
    }
    else if (n <= 0xff'ffff'ffffull)
    {
        encoded.append({
            static_cast<char>(INT_MAX_BASE - 3),
            static_cast<char>(n >> 32),
            static_cast<char>(n >> 24),
            static_cast<char>(n >> 16),
            static_cast<char>(n >> 8),
            static_cast<char>(n),
        });
    }
    else if (n <= 0xffff'ffff'ffffull)
    {
        encoded.append({
            static_cast<char>(INT_MAX_BASE - 2),
            static_cast<char>(n >> 40),
            static_cast<char>(n >> 32),
            static_cast<char>(n >> 24),
            static_cast<char>(n >> 16),
            static_cast<char>(n >> 8),
            static_cast<char>(n),
        });
    }
    else if (n <= 0xff'ffff'ffff'ffffull)
    {
        encoded.append({
            static_cast<char>(INT_MAX_BASE - 1),
            static_cast<char>(n >> 48),
            static_cast<char>(n >> 40),
            static_cast<char>(n >> 32),
            static_cast<char>(n >> 24),
            static_cast<char>(n >> 16),
            static_cast<char>(n >> 8),
            static_cast<char>(n),
        });
    }
    else
    {
        encoded.append({
            static_cast<char>(INT_MAX_BASE),
            static_cast<char>(n >> 56),
            static_cast<char>(n >> 48),
            static_cast<char>(n >> 40),
            static_cast<char>(n >> 32),
            static_cast<char>(n >> 24),
            static_cast<char>(n >> 16),
            static_cast<char>(n >> 8),
            static_cast<char>(n),
        });
    }

    return std::string_view{encoded.data() + current_size, encoded.size() - current_size};
}

std::string_view encodeVarUIntDescending(uint64_t n, std::string & encoded)
{
    size_t current_size = encoded.size();
    if (n == 0)
    {
        encoded.push_back(static_cast<char>(INT_MIN_BASE + 8));
    }
    else if (n <= 0xffull)
    {
        n = ~n;
        encoded.append({static_cast<char>(INT_MIN_BASE + 7), static_cast<char>(n)});
    }
    else if (n <= 0xffffull)
    {
        n = ~n;
        encoded.append({static_cast<char>(INT_MIN_BASE + 6), static_cast<char>(n >> 8), static_cast<char>(n)});
    }
    else if (n <= 0xff'ffffull)
    {
        n = ~n;
        encoded.append({static_cast<char>(INT_MIN_BASE + 5), static_cast<char>(n >> 16), static_cast<char>(n >> 8), static_cast<char>(n)});
    }
    else if (n <= 0xffff'ffffull)
    {
        n = ~n;
        encoded.append(
            {static_cast<char>(INT_MIN_BASE + 4),
             static_cast<char>(n >> 24),
             static_cast<char>(n >> 16),
             static_cast<char>(n >> 8),
             static_cast<char>(n)});
    }
    else if (n <= 0xff'ffff'ffffull)
    {
        n = ~n;
        encoded.append(
            {static_cast<char>(INT_MIN_BASE + 3),
             static_cast<char>(n >> 32),
             static_cast<char>(n >> 24),
             static_cast<char>(n >> 16),
             static_cast<char>(n >> 8),
             static_cast<char>(n)});
    }
    else if (n <= 0xffff'ffff'ffffull)
    {
        n = ~n;
        encoded.append(
            {static_cast<char>(INT_MIN_BASE + 2),
             static_cast<char>(n >> 40),
             static_cast<char>(n >> 32),
             static_cast<char>(n >> 24),
             static_cast<char>(n >> 16),
             static_cast<char>(n >> 8),
             static_cast<char>(n)});
    }
    else if (n <= 0xff'ffff'ffff'ffffull)
    {
        n = ~n;
        encoded.append(
            {static_cast<char>(INT_MIN_BASE + 1),
             static_cast<char>(n >> 48),
             static_cast<char>(n >> 40),
             static_cast<char>(n >> 32),
             static_cast<char>(n >> 24),
             static_cast<char>(n >> 16),
             static_cast<char>(n >> 8),
             static_cast<char>(n)});
    }
    else
    {
        n = ~n;
        encoded.append(
            {static_cast<char>(INT_MIN_BASE),
             static_cast<char>(n >> 56),
             static_cast<char>(n >> 48),
             static_cast<char>(n >> 40),
             static_cast<char>(n >> 32),
             static_cast<char>(n >> 24),
             static_cast<char>(n >> 16),
             static_cast<char>(n >> 8),
             static_cast<char>(n)});
    }

    return std::string_view{encoded.data() + current_size, encoded.size() - current_size};
}

uint64_t decodeVarUIntAscending(std::string_view & data)
{
    if (data.empty())
        throw Exception(ErrorCodes::INVALID_DATA, "Insufficient bytes to decode VarUInt value");

    /// char is signed, convert to uint8_t first for length
    auto length = static_cast<int32_t>(static_cast<uint8_t>(data[0])) - INT_ZERO;
    data = data.substr(1); /// Skip length byte

    if (length <= INT_SMALL)
        return length;

    length -= INT_SMALL;
    if (length < 0 || length > 8)
        throw Exception(ErrorCodes::INVALID_DATA, "Invalid VarUInt length={}", length);
    else if (static_cast<int32_t>(data.size()) < length)
        throw Exception(ErrorCodes::INVALID_DATA, "Insufficient bytes to decode VarUInt value={}", data);

    uint64_t v = 0;
    for (uint8_t ch : data.substr(0, length))
        v = (v << 8) | static_cast<uint64_t>(ch);

    data = data.substr(length);

    return v;
}

uint64_t decodeVarUIntDescending(std::string_view & data)
{
    if (data.empty())
        throw Exception(ErrorCodes::INVALID_DATA, "Insufficient bytes to decode varuint value");

    /// char is signed, convert to uint8_t first for length
    auto length = INT_ZERO - static_cast<int32_t>(static_cast<uint8_t>(data[0]));
    data = data.substr(1); /// Skip length byte

    if (length < 0 || length > 8)
        throw Exception(ErrorCodes::INVALID_DATA, "Invalid varuint length={}", length);
    else if (static_cast<int32_t>(data.size()) < length)
        throw Exception(ErrorCodes::INVALID_DATA, "Insufficient bytes to decode varuint value={}", data);

    uint64_t v = 0;
    for (uint8_t ch : data.substr(0, length))
        v = (v << 8) | static_cast<uint64_t>(static_cast<uint8_t>(~ch));

    data = data.substr(length);

    return v;
}

std::string_view encodeUInt64Ascending(uint64_t n, std::string & encoded)
{
    encoded.append({
        static_cast<char>(n >> 56),
        static_cast<char>(n >> 48),
        static_cast<char>(n >> 40),
        static_cast<char>(n >> 32),
        static_cast<char>(n >> 24),
        static_cast<char>(n >> 16),
        static_cast<char>(n >> 8),
        static_cast<char>(n),
    });

    return encoded;
}

std::string_view encodeUInt64Descending(uint64_t n, std::string & encoded)
{
    return encodeUInt64Ascending(~n, encoded);
}

uint64_t decodeUInt64Ascending(std::string_view & data)
{
    if (data.size() < 8)
        throw Exception(ErrorCodes::INVALID_DATA, "insufficient {} bytes to decode uint64_t int value", data.size());

    uint64_t n = static_cast<uint64_t>(static_cast<uint8_t>(data[0])) << 56 | static_cast<uint64_t>(static_cast<uint8_t>(data[1])) << 48
        | static_cast<uint64_t>(static_cast<uint8_t>(data[2])) << 40 | static_cast<uint64_t>(static_cast<uint8_t>(data[3])) << 32
        | static_cast<uint64_t>(static_cast<uint8_t>(data[4])) << 24 | static_cast<uint64_t>(static_cast<uint8_t>(data[5])) << 16
        | static_cast<uint64_t>(static_cast<uint8_t>(data[6])) << 8 | static_cast<uint64_t>(static_cast<uint8_t>(data[7]));

    data = data.substr(8);
    return n;
}

uint64_t decodeUInt64Descending(std::string_view & data)
{
    auto n = decodeUInt64Ascending(data);
    return ~n;
}

std::string_view encodeUInt32Ascending(uint32_t n, std::string & encoded)
{
    encoded.append({
        static_cast<char>(n >> 24),
        static_cast<char>(n >> 16),
        static_cast<char>(n >> 8),
        static_cast<char>(n),
    });

    return encoded;
}

std::string_view encodeUInt32Descending(uint32_t n, std::string & encoded)
{
    return encodeUInt32Ascending(~n, encoded);
}

uint32_t decodeUInt32Ascending(std::string_view & data)
{
    if (data.size() < 4)
        throw Exception(ErrorCodes::INVALID_DATA, "insufficient {} bytes to decode uint32_t int value", data.size());

    uint32_t n = static_cast<uint32_t>(static_cast<uint8_t>(data[0])) << 24 | static_cast<uint32_t>(static_cast<uint8_t>(data[1])) << 16
        | static_cast<uint32_t>(static_cast<uint8_t>(data[2])) << 8 | static_cast<uint32_t>(static_cast<uint8_t>(data[3]));

    data = data.substr(4);
    return n;
}

uint32_t decodeUInt32Descending(std::string_view & data)
{
    auto n = decodeUInt32Ascending(data);
    return ~n;
}

std::string_view encodeDoubleAscending(double f, std::string & encoded)
{
    /// Handle the simplistic cases first
    if (f == 0)
    {
        /// This encodes both positive and negative zero the same. Negative zero uses
        /// composite indexes to decode itself correctly.
        encoded.push_back(FLOAT_ZERO);
        return encoded;
    }
    else if (std::isnan(f))
    {
        encoded.push_back(FLOAT_NAN);
        return encoded;
    }

    encoded.reserve(encoded.size() + 16);

    auto u = doubleBits(f);
    if ((u & (1ull << 63)) != 0)
    {
        /// Negative double
        u = ~u;
        encoded.push_back(FLOAT_NEG);
    }
    else
    {
        encoded.push_back(FLOAT_POS);
    }

    return encodeUInt64Ascending(u, encoded);
}

std::string_view encodeDoubleDescending(double f, std::string & encoded)
{
    if (std::isnan(f))
    {
        encoded.push_back(FLOAT_NAN_DESC);
        return encoded;
    }

    return encodeDoubleAscending(-f, encoded);
}

double decodeDoubleAscending(std::string_view & data)
{
    assert(!data.empty());

    auto prefix_byte = data[0];
    data = data.substr(1);

    switch (prefix_byte)
    {
        case FLOAT_NAN:
            [[fallthrough]];
        case FLOAT_NAN_DESC:
            return NAN;
        case FLOAT_ZERO:
            return 0;
        case FLOAT_NEG:
        {
            auto n = decodeUInt64Ascending(data);
            return doubleFromBits(~n);
        }
        case FLOAT_POS:
        {
            auto n = decodeUInt64Ascending(data);
            return doubleFromBits(n);
        }
        default:
            throw Exception(ErrorCodes::INVALID_DATA, "Unknown prefix=0x{:x} of the encoded byte for float", prefix_byte);
    }
}

double decodeDoubleDescending(std::string_view & data)
{
    auto f = decodeDoubleAscending(data);
    if (f != 0 && !std::isnan(f))
        /// All values except for 0 and NaN were negated in encodeDoubleDescending, so
        /// we have to negate them back. Negative zero uses composite indexes to
        /// decode itself correctly.
        return -f;

    return f;
}

std::string_view encodeFloatAscending(float f, std::string & encoded)
{
    /// Handle the simplistic cases first
    if (f == 0)
    {
        /// This encodes both positive and negative zero the same. Negative zero uses
        /// composite indexes to decode itself correctly.
        encoded.push_back(FLOAT_ZERO);
        return encoded;
    }
    else if (std::isnan(f))
    {
        encoded.push_back(FLOAT_NAN);
        return encoded;
    }

    encoded.reserve(encoded.size() + 8);

    auto u = floatBits(f);
    if ((u & (1ull << 31)) != 0)
    {
        /// Negative float
        u = ~u;
        encoded.push_back(FLOAT_NEG);
    }
    else
    {
        encoded.push_back(FLOAT_POS);
    }

    return encodeUInt32Ascending(u, encoded);
}

std::string_view encodeFloatDescending(float f, std::string & encoded)
{
    if (std::isnan(f))
    {
        encoded.push_back(FLOAT_NAN_DESC);
        return encoded;
    }

    return encodeFloatAscending(-f, encoded);
}

float decodeFloatAscending(std::string_view & data)
{
    assert(!data.empty());

    auto prefix_byte = data[0];
    data = data.substr(1);

    switch (prefix_byte)
    {
        case FLOAT_NAN:
            [[fallthrough]];
        case FLOAT_NAN_DESC:
            return NAN;
        case FLOAT_ZERO:
            return 0;
        case FLOAT_NEG:
        {
            auto n = decodeUInt32Ascending(data);
            return floatFromBits(~n);
        }
        case FLOAT_POS:
        {
            auto n = decodeUInt32Ascending(data);
            return floatFromBits(n);
        }
        default:
            throw Exception(ErrorCodes::INVALID_DATA, "Unknown prefix=0x{:x} of the encoded byte for float", prefix_byte);
    }
}

float decodeFloatDescending(std::string_view & data)
{
    auto f = decodeFloatAscending(data);
    if (f != 0 && !std::isnan(f))
        /// All values except for 0 and NaN were negated in encodeDoubleDescending, so
        /// we have to negate them back. Negative zero uses composite indexes to
        /// decode itself correctly.
        return -f;

    return f;
}

uint64_t varIntLength(std::string_view data)
{
    assert(!data.empty());
    auto length = static_cast<int32_t>(data[0]) - INT_ZERO;
    if (length >= 0)
    {
        if (length <= INT_SMALL)
            return 1;

        /// tag and length - INT_SMALL bytes
        length = 1 + length - INT_SMALL;
    }
    else
    {
        /// tag and -length bytes
        length = 1 - length;
    }

    if (length > static_cast<int32_t>(data.size()))
        throw Exception(ErrorCodes::INVALID_DATA, "Insufficient bytes to decode VarUInt, data='{}'", data);

    return length;
}

uint64_t varUIntLength(std::string_view data)
{
    assert(!data.empty());

    auto length = static_cast<int32_t>(data[0]) - INT_ZERO;
    if (length <= INT_SMALL)
        return 1;

    length -= INT_SMALL;
    if (length > 8)
        throw Exception(ErrorCodes::INVALID_DATA, "Invalid VarUInt length={}", length);

    if (static_cast<int32_t>(data.size()) <= length)
        throw Exception(ErrorCodes::INVALID_DATA, "Insufficient bytes to decode VarUInt, data='{}'", data);

    return 1 + length;
}

std::string_view keyPrefixEnd(std::string & prefix)
{
    if (prefix.empty())
    {
        prefix = KEY_MAX;
        return prefix;
    }

    for (int32_t i = static_cast<int32_t>(prefix.size()) - 1; i >= 0; i--)
    {
        prefix[i] += 1;
        if (prefix[i] != 0)
        {
            prefix.resize(i + 1);
            break;
        }
    }

    /// This statement will only be reached if they key is already the maximum key
    /// (i.e \xff\xff)
    return prefix;
}
}
}
