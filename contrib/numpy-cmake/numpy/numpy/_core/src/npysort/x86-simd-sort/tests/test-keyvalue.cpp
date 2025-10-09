/*******************************************
 * * Copyright (C) 2022-2023 Intel Corporation
 * * SPDX-License-Identifier: BSD-3-Clause
 * *******************************************/

#include "rand_array.h"
#include "x86simdsort.h"
#include "x86simdsort-scalar.h"
#include <gtest/gtest.h>

template <typename T>
class simdkvsort : public ::testing::Test {
public:
    simdkvsort()
    {
        std::iota(arrsize.begin(), arrsize.end(), 1);
        arrtype = {"random",
                   "constant",
                   "sorted",
                   "reverse",
                   "smallrange",
                   "max_at_the_end",
                   "random_5d",
                   "rand_max"};
    }
    std::vector<std::string> arrtype;
    std::vector<size_t> arrsize = std::vector<size_t>(1024);
};

TYPED_TEST_SUITE_P(simdkvsort);

TYPED_TEST_P(simdkvsort, test_kvsort)
{
    using T1 = typename std::tuple_element<0, decltype(TypeParam())>::type;
    using T2 = typename std::tuple_element<1, decltype(TypeParam())>::type;
    for (auto type : this->arrtype) {
        bool hasnan = (type == "rand_with_nan") ? true : false;
        for (auto size : this->arrsize) {
            std::vector<T1> key = get_array<T1>(type, size);
            std::vector<T2> val = get_array<T2>(type, size);
            std::vector<T1> key_bckp = key;
            std::vector<T2> val_bckp = val;
            x86simdsort::keyvalue_qsort(key.data(), val.data(), size, hasnan);
            xss::scalar::keyvalue_qsort(
                    key_bckp.data(), val_bckp.data(), size, hasnan);
            ASSERT_EQ(key, key_bckp);
            const bool hasDuplicates
                    = std::adjacent_find(key.begin(), key.end()) != key.end();
            if (!hasDuplicates) { ASSERT_EQ(val, val_bckp); }
            key.clear();
            val.clear();
            key_bckp.clear();
            val_bckp.clear();
        }
    }
}

REGISTER_TYPED_TEST_SUITE_P(simdkvsort, test_kvsort);

#define CREATE_TUPLES(type) \
    std::tuple<double, type>, std::tuple<uint64_t, type>, \
            std::tuple<int64_t, type>, std::tuple<float, type>, \
            std::tuple<uint32_t, type>, std::tuple<int32_t, type>

using QKVSortTestTypes = testing::Types<CREATE_TUPLES(double),
                                        CREATE_TUPLES(uint64_t),
                                        CREATE_TUPLES(int64_t),
                                        CREATE_TUPLES(uint32_t),
                                        CREATE_TUPLES(int32_t),
                                        CREATE_TUPLES(float)>;

INSTANTIATE_TYPED_TEST_SUITE_P(xss, simdkvsort, QKVSortTestTypes);
