#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace DB::PrefixTreeEncode
{
/// Prefix tree encodes `data` using key encoding and appends to `encoded`.
/// The encoded value is guaranteed to be lexicographically sortable, but not
/// guaranteed to be roundtrip-able during decoding: some values like decimals
/// or collated strings have composite encoding where part of their value lies
/// in the value part of the key / value pair

/// \encodeStringAscending encodes the string value using an escape-based encoding.
/// The encoded data will be appended to output parameter encoded.
/// \return the string view of the newly encoded data
/// Please note the returned view may be invalidated if the output parameter encoded
/// get resized later
std::string_view encodeStringAscending(std::string_view data, std::string & encoded);
/// \encodeStringDescending it is similar to the counterpart encodeStringAscending, but in a descending direction
std::string_view encodeStringDescending(std::string_view data, std::string & encoded);

/// \decodeStringAscending decodes the encoded key data and the decoded data will be appended
/// to the output parameter decoded if an inplace decoding (without inline modifying the
/// source data) is not possible.
/// \return string view of the encoded data. Please note if a deep copy can be avoided after decoding
/// the returned string view points to a segment of the input parameter data; otherwise it
/// points to the output parameter decoded. So keep in mind the life cycles of the objects.
/// Please note the returned view may be invalidated if the output parameter encoded
/// get resized later
std::string_view decodeStringAscending(std::string_view & data, std::string & decoded);
/// \decodeStringDescending it is similar to its counterpart \decodeStringAscending but in descending direction.
/// and it always make a deep copy of the data after decoding.
/// \return string view of the decoded data. Always point to the output parameter decoded
std::string_view decodeStringDescending(std::string_view & data, std::string & decoded);

void decodeStringAscendingDiscard(std::string_view & data);
void decodeStringDescendingDiscard(std::string_view & data);

/// Integers
/// \encodeVarIntAscending encodes the int64_t value using a variable length (length-prefixed) representation.
/// The length is encoded as a single byte. If the value to be encoded is negative, the length is encoded
/// as 8-numBytes. If the value is positive, it is encoded as 8+numBytes. The encoded bytes are appended to
/// the output parameter encoded
/// \return string view of the encoded data which points to the output parameter encoded
/// Please note the returned view may be invalidated if the output parameter encoded
/// get resized later
std::string_view encodeVarIntAscending(int64_t n, std::string & encoded);
std::string_view encodeVarIntDescending(int64_t n, std::string & encoded);

int64_t decodeVarIntAscending(std::string_view & data);
int64_t decodeVarIntDescending(std::string_view & data);

/// \encodeVarUIntAscending it is similar to its counterpart \encodeVarIntAscending but in descending direction.
/// It encodes the uint64_ value using a variable length (length-prefixed) representation.
/// The length is encoded as a single byte indicating the number of encoded bytes (-8) to follow.
/// The encoded bytes are appended to the output parameter encoded
/// \return string view of the encoded data which points to the output parameter encoded
/// Please note the returned view may be invalidated if the output parameter encoded
/// get resized later
std::string_view encodeVarUIntAscending(uint64_t n, std::string & encoded);
std::string_view encodeVarUIntDescending(uint64_t n, std::string & encoded);

uint64_t decodeVarUIntAscending(std::string_view & data);
uint64_t decodeVarUIntDescending(std::string_view & data);

/// EncodeUint64Ascending encodes the uint64_t value using a big-endian 8 byte
/// representation.
/// \return string view of the encoded data which points to the output parameter encoded
std::string_view encodeUInt64Ascending(uint64_t n, std::string & encoded);
std::string_view encodeUInt64Descending(uint64_t n, std::string & encoded);

/// decodeUInt64Ascending decodes an uint64_t from the input buffer, treating
/// the input as a big-endian 8 byte uint64_t representation. The remainder
/// of the input buffer and the decoded uint64_t are returned.
uint64_t decodeUInt64Ascending(std::string_view & data);

/// decodeUInt64Descending decodes an uint64_t value which was encoded
/// using encodeUInt64Descending.
uint64_t decodeUInt64Descending(std::string_view & data);

/// Similar to encodeUInt64Ascending
std::string_view encodeUInt32Ascending(uint32_t n, std::string & encoded);
std::string_view encodeUInt32Descending(uint32_t n, std::string & encoded);

uint32_t decodeUInt32Ascending(std::string_view & data);
uint32_t decodeUInt32Descending(std::string_view & data);

/// \encodeDoubleAscending returns the resulting byte slice with the encoded float64 appended to
/// the output parameter encoded. The encoded format for a double value f is, for positive f,
/// the encoding of the 64bits (in IEEE 754 format) re-interpreted as an uin64_t and encoded using
/// encodeUInt64Ascending. For negative f, we keep the sign bit and invert all other bits, encoding
/// this value using EncodeUInt64Descending.
/// One of the 5 single-byte prefix tags are appended to the front of the encoding.
/// These tags enforce logical ordering of keys for both ascending and descending encoding directions.
/// The tags split the encoded floats into 5 categories:
/// - NaN for an ascending encoding direction
/// - Negative valued floats
/// - Zero (positive and negative)
/// - Positive valued floats
/// - NaN for a descending encoding direction
/// This ordering ensures that NaNs are always sorted first in either encoding direction, and that
/// after them a logical ordering is followed
/// Please note the returned view may be invalidated if the output parameter encoded
/// get resized later
std::string_view encodeDoubleAscending(double f, std::string & encoded);
std::string_view encodeDoubleDescending(double f, std::string & encoded);

double decodeDoubleAscending(std::string_view & data);
double decodeDoubleDescending(std::string_view & data);

/// Similar encodeDoubleAscending, but 32 bits
std::string_view encodeFloatAscending(float f, std::string & encoded);
std::string_view encodeFloatDescending(float f, std::string & encoded);

float decodeFloatAscending(std::string_view & data);
float decodeFloatDescending(std::string_view & data);

/// \returns the length of an encoded varint
uint64_t varIntLength(std::string_view data);

/// getVarUIntLength is similar to decodeVarUIntAscending except it returns
/// the length of the prefix that encodes an uint64 value in bytes without
/// actually decoding the value
uint64_t varUIntLength(std::string_view data);

/// keyPrefixEnd determines the end key given key as a prefix, that is the
/// key that sorts precisely behind all keys starting with prefix: "1"
/// is added to the final byte and the carry propagated. The special
/// case of empty and KeyMin always returns KeyMax
std::string_view keyPrefixEnd(std::string & prefix);
}
