#include <Interpreters/Streaming/ChooseHashMethod.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Core/ColumnNumbers.h>

namespace DB
{
namespace Streaming
{
std::pair<HashType, std::vector<size_t>> chooseHashMethod(const ColumnRawPtrs & key_columns)
{
    size_t keys_size = key_columns.size();
    assert(keys_size > 0);

    bool all_fixed = true;
    size_t keys_bytes = 0;

    HashType type;
    std::vector<size_t> key_sizes;
    key_sizes.resize(keys_size);

    for (size_t j = 0; j < keys_size; ++j)
    {
        if (!key_columns[j]->isFixedAndContiguous())
        {
            all_fixed = false;
            break;
        }
        key_sizes[j] = key_columns[j]->sizeOfValueIfFixed();
        keys_bytes += key_sizes[j];
    }

    /// If there is one numeric key that fits in 64 bits
    if (keys_size == 1 && key_columns[0]->isNumeric())
    {
        size_t size_of_field = key_columns[0]->sizeOfValueIfFixed();
        if (size_of_field == 1)
            type = HashType::key8;
        else if (size_of_field == 2)
            type = HashType::key16;
        else if (size_of_field == 4)
            type = HashType::key32;
        else if (size_of_field == 8)
            type = HashType::key64;
        else if (size_of_field == 16)
            type = HashType::keys128;
        else if (size_of_field == 32)
            type = HashType::keys256;
        else
            throw Exception("Logical error: numeric column has sizeOfField not in 1, 2, 4, 8, 16, 32.", ErrorCodes::LOGICAL_ERROR);

        return {type, std::move(key_sizes)};
    }

    /// If the keys fit in N bits, we will use a hash table for N-bit-packed keys
    if (all_fixed && keys_bytes <= 16)
        return {HashType::keys128, std::move(key_sizes)};

    if (all_fixed && keys_bytes <= 32)
        return {HashType::keys256, std::move(key_sizes)};

    /// If there is single string key, use hash table of it's values.
    if (keys_size == 1
        && (typeid_cast<const ColumnString *>(key_columns[0])
            || (isColumnConst(*key_columns[0])
                && typeid_cast<const ColumnString *>(&assert_cast<const ColumnConst *>(key_columns[0])->getDataColumn()))))
        return {HashType::key_string, std::move(key_sizes)};

    if (keys_size == 1 && typeid_cast<const ColumnFixedString *>(key_columns[0]))
        return {HashType::key_fixed_string, std::move(key_sizes)};

    /// Otherwise, will use set of cryptographic hashes of unambiguously serialized values.
    return {HashType::hashed, std::move(key_sizes)};
}
}
}
