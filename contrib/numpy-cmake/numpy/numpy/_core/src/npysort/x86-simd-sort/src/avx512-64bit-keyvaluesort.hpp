/*******************************************************************
 * Copyright (C) 2022 Intel Corporation
 * SPDX-License-Identifier: BSD-3-Clause
 * Authors: Liu Zhuan <zhuan.liu@intel.com>
 *          Tang Xi <xi.tang@intel.com>
 * ****************************************************************/

#ifndef AVX512_QSORT_64BIT_KV
#define AVX512_QSORT_64BIT_KV

#include "avx512-64bit-common.h"
#include "xss-network-keyvaluesort.hpp"

/*
 * Parition one ZMM register based on the pivot and returns the index of the
 * last element that is less than equal to the pivot.
 */
template <typename vtype1,
          typename vtype2,
          typename type_t1 = typename vtype1::type_t,
          typename type_t2 = typename vtype2::type_t,
          typename reg_t1 = typename vtype1::reg_t,
          typename reg_t2 = typename vtype2::reg_t>
X86_SIMD_SORT_INLINE int32_t partition_vec(type_t1 *keys,
                                           type_t2 *indexes,
                                           arrsize_t left,
                                           arrsize_t right,
                                           const reg_t1 keys_vec,
                                           const reg_t2 indexes_vec,
                                           const reg_t1 pivot_vec,
                                           reg_t1 *smallest_vec,
                                           reg_t1 *biggest_vec)
{
    /* which elements are larger than the pivot */
    typename vtype1::opmask_t gt_mask = vtype1::ge(keys_vec, pivot_vec);
    int32_t amount_gt_pivot = _mm_popcnt_u32((int32_t)gt_mask);
    vtype1::mask_compressstoreu(
            keys + left, vtype1::knot_opmask(gt_mask), keys_vec);
    vtype1::mask_compressstoreu(
            keys + right - amount_gt_pivot, gt_mask, keys_vec);
    vtype2::mask_compressstoreu(
            indexes + left, vtype2::knot_opmask(gt_mask), indexes_vec);
    vtype2::mask_compressstoreu(
            indexes + right - amount_gt_pivot, gt_mask, indexes_vec);
    *smallest_vec = vtype1::min(keys_vec, *smallest_vec);
    *biggest_vec = vtype1::max(keys_vec, *biggest_vec);
    return amount_gt_pivot;
}
/*
 * Parition an array based on the pivot and returns the index of the
 * last element that is less than equal to the pivot.
 */
template <typename vtype1,
          typename vtype2,
          typename type_t1 = typename vtype1::type_t,
          typename type_t2 = typename vtype2::type_t,
          typename reg_t1 = typename vtype1::reg_t,
          typename reg_t2 = typename vtype2::reg_t>
X86_SIMD_SORT_INLINE arrsize_t kvpartition(type_t1 *keys,
                                           type_t2 *indexes,
                                           arrsize_t left,
                                           arrsize_t right,
                                           type_t1 pivot,
                                           type_t1 *smallest,
                                           type_t1 *biggest)
{
    /* make array length divisible by vtype1::numlanes , shortening the array */
    for (int32_t i = (right - left) % vtype1::numlanes; i > 0; --i) {
        *smallest = std::min(*smallest, keys[left]);
        *biggest = std::max(*biggest, keys[left]);
        if (keys[left] > pivot) {
            right--;
            std::swap(keys[left], keys[right]);
            std::swap(indexes[left], indexes[right]);
        }
        else {
            ++left;
        }
    }

    if (left == right)
        return left; /* less than vtype1::numlanes elements in the array */

    reg_t1 pivot_vec = vtype1::set1(pivot);
    reg_t1 min_vec = vtype1::set1(*smallest);
    reg_t1 max_vec = vtype1::set1(*biggest);

    if (right - left == vtype1::numlanes) {
        reg_t1 keys_vec = vtype1::loadu(keys + left);
        int32_t amount_gt_pivot;

        reg_t2 indexes_vec = vtype2::loadu(indexes + left);
        amount_gt_pivot = partition_vec<vtype1, vtype2>(keys,
                                                        indexes,
                                                        left,
                                                        left + vtype1::numlanes,
                                                        keys_vec,
                                                        indexes_vec,
                                                        pivot_vec,
                                                        &min_vec,
                                                        &max_vec);

        *smallest = vtype1::reducemin(min_vec);
        *biggest = vtype1::reducemax(max_vec);
        return left + (vtype1::numlanes - amount_gt_pivot);
    }

    // first and last vtype1::numlanes values are partitioned at the end
    reg_t1 keys_vec_left = vtype1::loadu(keys + left);
    reg_t1 keys_vec_right = vtype1::loadu(keys + (right - vtype1::numlanes));
    reg_t2 indexes_vec_left;
    reg_t2 indexes_vec_right;
    indexes_vec_left = vtype2::loadu(indexes + left);
    indexes_vec_right = vtype2::loadu(indexes + (right - vtype1::numlanes));

    // store points of the vectors
    arrsize_t r_store = right - vtype1::numlanes;
    arrsize_t l_store = left;
    // indices for loading the elements
    left += vtype1::numlanes;
    right -= vtype1::numlanes;
    while (right - left != 0) {
        reg_t1 keys_vec;
        reg_t2 indexes_vec;
        /*
         * if fewer elements are stored on the right side of the array,
         * then next elements are loaded from the right side,
         * otherwise from the left side
         */
        if ((r_store + vtype1::numlanes) - right < left - l_store) {
            right -= vtype1::numlanes;
            keys_vec = vtype1::loadu(keys + right);
            indexes_vec = vtype2::loadu(indexes + right);
        }
        else {
            keys_vec = vtype1::loadu(keys + left);
            indexes_vec = vtype2::loadu(indexes + left);
            left += vtype1::numlanes;
        }
        // partition the current vector and save it on both sides of the array
        int32_t amount_gt_pivot;

        amount_gt_pivot
                = partition_vec<vtype1, vtype2>(keys,
                                                indexes,
                                                l_store,
                                                r_store + vtype1::numlanes,
                                                keys_vec,
                                                indexes_vec,
                                                pivot_vec,
                                                &min_vec,
                                                &max_vec);
        r_store -= amount_gt_pivot;
        l_store += (vtype1::numlanes - amount_gt_pivot);
    }

    /* partition and save vec_left and vec_right */
    int32_t amount_gt_pivot;
    amount_gt_pivot = partition_vec<vtype1, vtype2>(keys,
                                                    indexes,
                                                    l_store,
                                                    r_store + vtype1::numlanes,
                                                    keys_vec_left,
                                                    indexes_vec_left,
                                                    pivot_vec,
                                                    &min_vec,
                                                    &max_vec);
    l_store += (vtype1::numlanes - amount_gt_pivot);
    amount_gt_pivot = partition_vec<vtype1, vtype2>(keys,
                                                    indexes,
                                                    l_store,
                                                    l_store + vtype1::numlanes,
                                                    keys_vec_right,
                                                    indexes_vec_right,
                                                    pivot_vec,
                                                    &min_vec,
                                                    &max_vec);
    l_store += (vtype1::numlanes - amount_gt_pivot);
    *smallest = vtype1::reducemin(min_vec);
    *biggest = vtype1::reducemax(max_vec);
    return l_store;
}

template <typename vtype1,
          typename vtype2,
          int num_unroll,
          typename type_t1 = typename vtype1::type_t,
          typename type_t2 = typename vtype2::type_t,
          typename reg_t1 = typename vtype1::reg_t,
          typename reg_t2 = typename vtype2::reg_t>
X86_SIMD_SORT_INLINE arrsize_t kvpartition_unrolled(type_t1 *keys,
                                                    type_t2 *indexes,
                                                    arrsize_t left,
                                                    arrsize_t right,
                                                    type_t1 pivot,
                                                    type_t1 *smallest,
                                                    type_t1 *biggest)
{
    if (right - left <= 8 * num_unroll * vtype1::numlanes) {
        return kvpartition<vtype1, vtype2>(
                keys, indexes, left, right, pivot, smallest, biggest);
    }
    /* make array length divisible by vtype1::numlanes , shortening the array */
    for (int32_t i = ((right - left) % (num_unroll * vtype1::numlanes)); i > 0;
         --i) {
        *smallest = std::min(*smallest, keys[left]);
        *biggest = std::max(*biggest, keys[left]);
        if (keys[left] > pivot) {
            right--;
            std::swap(keys[left], keys[right]);
            std::swap(indexes[left], indexes[right]);
        }
        else {
            ++left;
        }
    }

    if (left == right) return left;

    reg_t1 pivot_vec = vtype1::set1(pivot);
    reg_t1 min_vec = vtype1::set1(*smallest);
    reg_t1 max_vec = vtype1::set1(*biggest);

    // first and last vtype1::numlanes values are partitioned at the end
    reg_t1 key_left[num_unroll], key_right[num_unroll];
    reg_t2 indx_left[num_unroll], indx_right[num_unroll];
    X86_SIMD_SORT_UNROLL_LOOP(8)
    for (int ii = 0; ii < num_unroll; ++ii) {
        indx_left[ii] = vtype2::loadu(indexes + left + vtype2::numlanes * ii);
        key_left[ii] = vtype1::loadu(keys + left + vtype1::numlanes * ii);
        indx_right[ii] = vtype2::loadu(
                indexes + (right - vtype2::numlanes * (num_unroll - ii)));
        key_right[ii] = vtype1::loadu(
                keys + (right - vtype1::numlanes * (num_unroll - ii)));
    }
    // store points of the vectors
    arrsize_t r_store = right - vtype1::numlanes;
    arrsize_t l_store = left;
    // indices for loading the elements
    left += num_unroll * vtype1::numlanes;
    right -= num_unroll * vtype1::numlanes;
    while (right - left != 0) {
        reg_t2 indx_vec[num_unroll];
        reg_t1 curr_vec[num_unroll];
        /*
         * if fewer elements are stored on the right side of the array,
         * then next elements are loaded from the right side,
         * otherwise from the left side
         */
        if ((r_store + vtype1::numlanes) - right < left - l_store) {
            right -= num_unroll * vtype1::numlanes;
            X86_SIMD_SORT_UNROLL_LOOP(8)
            for (int ii = 0; ii < num_unroll; ++ii) {
                indx_vec[ii] = vtype2::loadu(indexes + right
                                             + ii * vtype2::numlanes);
                curr_vec[ii]
                        = vtype1::loadu(keys + right + ii * vtype1::numlanes);
            }
        }
        else {
            X86_SIMD_SORT_UNROLL_LOOP(8)
            for (int ii = 0; ii < num_unroll; ++ii) {
                indx_vec[ii]
                        = vtype2::loadu(indexes + left + ii * vtype2::numlanes);
                curr_vec[ii]
                        = vtype1::loadu(keys + left + ii * vtype1::numlanes);
            }
            left += num_unroll * vtype1::numlanes;
        }
        // partition the current vector and save it on both sides of the array
        X86_SIMD_SORT_UNROLL_LOOP(8)
        for (int ii = 0; ii < num_unroll; ++ii) {
            int32_t amount_gt_pivot
                    = partition_vec<vtype1, vtype2>(keys,
                                                    indexes,
                                                    l_store,
                                                    r_store + vtype1::numlanes,
                                                    curr_vec[ii],
                                                    indx_vec[ii],
                                                    pivot_vec,
                                                    &min_vec,
                                                    &max_vec);
            l_store += (vtype1::numlanes - amount_gt_pivot);
            r_store -= amount_gt_pivot;
        }
    }

    /* partition and save key_left and key_right */
    X86_SIMD_SORT_UNROLL_LOOP(8)
    for (int ii = 0; ii < num_unroll; ++ii) {
        int32_t amount_gt_pivot
                = partition_vec<vtype1, vtype2>(keys,
                                                indexes,
                                                l_store,
                                                r_store + vtype1::numlanes,
                                                key_left[ii],
                                                indx_left[ii],
                                                pivot_vec,
                                                &min_vec,
                                                &max_vec);
        l_store += (vtype1::numlanes - amount_gt_pivot);
        r_store -= amount_gt_pivot;
    }
    X86_SIMD_SORT_UNROLL_LOOP(8)
    for (int ii = 0; ii < num_unroll; ++ii) {
        int32_t amount_gt_pivot
                = partition_vec<vtype1, vtype2>(keys,
                                                indexes,
                                                l_store,
                                                r_store + vtype1::numlanes,
                                                key_right[ii],
                                                indx_right[ii],
                                                pivot_vec,
                                                &min_vec,
                                                &max_vec);
        l_store += (vtype1::numlanes - amount_gt_pivot);
        r_store -= amount_gt_pivot;
    }
    *smallest = vtype1::reducemin(min_vec);
    *biggest = vtype1::reducemax(max_vec);
    return l_store;
}

template <typename vtype1,
          typename vtype2,
          typename type1_t = typename vtype1::type_t,
          typename type2_t = typename vtype2::type_t>
X86_SIMD_SORT_INLINE void
heapify(type1_t *keys, type2_t *indexes, arrsize_t idx, arrsize_t size)
{
    arrsize_t i = idx;
    while (true) {
        arrsize_t j = 2 * i + 1;
        if (j >= size) { break; }
        arrsize_t k = j + 1;
        if (k < size && keys[j] < keys[k]) { j = k; }
        if (keys[j] < keys[i]) { break; }
        std::swap(keys[i], keys[j]);
        std::swap(indexes[i], indexes[j]);
        i = j;
    }
}
template <typename vtype1,
          typename vtype2,
          typename type1_t = typename vtype1::type_t,
          typename type2_t = typename vtype2::type_t>
X86_SIMD_SORT_INLINE void
heap_sort(type1_t *keys, type2_t *indexes, arrsize_t size)
{
    if (size <= 1) return;
    for (arrsize_t i = size / 2 - 1;; i--) {
        heapify<vtype1, vtype2>(keys, indexes, i, size);
        if (i == 0) { break; }
    }
    for (arrsize_t i = size - 1; i > 0; i--) {
        std::swap(keys[0], keys[i]);
        std::swap(indexes[0], indexes[i]);
        heapify<vtype1, vtype2>(keys, indexes, 0, i);
    }
}

template <typename vtype1,
          typename vtype2,
          typename type1_t = typename vtype1::type_t,
          typename type2_t = typename vtype2::type_t>
X86_SIMD_SORT_INLINE void qsort_64bit_(type1_t *keys,
                                       type2_t *indexes,
                                       arrsize_t left,
                                       arrsize_t right,
                                       int max_iters)
{
    /*
     * Resort to std::sort if quicksort isnt making any progress
     */
    if (max_iters <= 0) {
        heap_sort<vtype1, vtype2>(
                keys + left, indexes + left, right - left + 1);
        return;
    }
    /*
     * Base case: use bitonic networks to sort arrays <= 128
     */
    if (right + 1 - left <= 128) {

        kvsort_n<vtype1, vtype2, 128>(
                keys + left, indexes + left, (int32_t)(right + 1 - left));
        return;
    }

    type1_t pivot = get_pivot_blocks<vtype1>(keys, left, right);
    type1_t smallest = vtype1::type_max();
    type1_t biggest = vtype1::type_min();
    arrsize_t pivot_index = kvpartition_unrolled<vtype1, vtype2, 4>(
            keys, indexes, left, right + 1, pivot, &smallest, &biggest);
    if (pivot != smallest) {
        qsort_64bit_<vtype1, vtype2>(
                keys, indexes, left, pivot_index - 1, max_iters - 1);
    }
    if (pivot != biggest) {
        qsort_64bit_<vtype1, vtype2>(
                keys, indexes, pivot_index, right, max_iters - 1);
    }
}

template <typename T1, typename T2>
X86_SIMD_SORT_INLINE void
avx512_qsort_kv(T1 *keys, T2 *indexes, arrsize_t arrsize, bool hasnan = false)
{
    using keytype =
            typename std::conditional<sizeof(T1) != sizeof(T2)
                                              && sizeof(T1) == sizeof(int32_t),
                                      ymm_vector<T1>,
                                      zmm_vector<T1>>::type;
    using valtype =
            typename std::conditional<sizeof(T1) != sizeof(T2)
                                              && sizeof(T2) == sizeof(int32_t),
                                      ymm_vector<T2>,
                                      zmm_vector<T2>>::type;
/*
 * Enable testing the heapsort key-value sort in the CI:
 */
#ifdef XSS_TEST_KEYVALUE_BASE_CASE
    int maxiters = -1;
    bool minarrsize = true;
#else
    int maxiters = 2 * log2(arrsize);
    bool minarrsize = arrsize > 1 ? true : false;
#endif // XSS_TEST_KEYVALUE_BASE_CASE

    if (minarrsize) {
        arrsize_t nan_count = 0;
        if constexpr (xss::fp::is_floating_point_v<T1>) {
            if (UNLIKELY(hasnan)) {
                nan_count = replace_nan_with_inf<zmm_vector<T1>>(keys, arrsize);
            }
        }
        else {
            UNUSED(hasnan);
        }
        qsort_64bit_<keytype, valtype>(keys, indexes, 0, arrsize - 1, maxiters);
        replace_inf_with_nan(keys, arrsize, nan_count);
    }
}
#endif // AVX512_QSORT_64BIT_KV
