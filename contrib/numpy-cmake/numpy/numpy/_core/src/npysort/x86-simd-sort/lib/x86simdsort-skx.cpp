// SKX specific routines:
#include "x86simdsort-static-incl.h"
#include "x86simdsort-internal.h"

#define DEFINE_ALL_METHODS(type) \
    template <> \
    void qsort(type *arr, size_t arrsize, bool hasnan, bool descending) \
    { \
        x86simdsortStatic::qsort(arr, arrsize, hasnan, descending); \
    } \
    template <> \
    void qselect( \
            type *arr, size_t k, size_t arrsize, bool hasnan, bool descending) \
    { \
        x86simdsortStatic::qselect(arr, k, arrsize, hasnan, descending); \
    } \
    template <> \
    void partial_qsort( \
            type *arr, size_t k, size_t arrsize, bool hasnan, bool descending) \
    { \
        x86simdsortStatic::partial_qsort(arr, k, arrsize, hasnan, descending); \
    } \
    template <> \
    std::vector<size_t> argsort( \
            type *arr, size_t arrsize, bool hasnan, bool descending) \
    { \
        return x86simdsortStatic::argsort(arr, arrsize, hasnan, descending); \
    } \
    template <> \
    std::vector<size_t> argselect( \
            type *arr, size_t k, size_t arrsize, bool hasnan) \
    { \
        return x86simdsortStatic::argselect(arr, k, arrsize, hasnan); \
    }

#define DEFINE_KEYVALUE_METHODS(type) \
    template <> \
    void keyvalue_qsort(type *key, uint64_t *val, size_t arrsize, bool hasnan) \
    { \
        x86simdsortStatic::keyvalue_qsort(key, val, arrsize, hasnan); \
    } \
    template <> \
    void keyvalue_qsort(type *key, int64_t *val, size_t arrsize, bool hasnan) \
    { \
        x86simdsortStatic::keyvalue_qsort(key, val, arrsize, hasnan); \
    } \
    template <> \
    void keyvalue_qsort(type *key, double *val, size_t arrsize, bool hasnan) \
    { \
        x86simdsortStatic::keyvalue_qsort(key, val, arrsize, hasnan); \
    } \
    template <> \
    void keyvalue_qsort(type *key, uint32_t *val, size_t arrsize, bool hasnan) \
    { \
        x86simdsortStatic::keyvalue_qsort(key, val, arrsize, hasnan); \
    } \
    template <> \
    void keyvalue_qsort(type *key, int32_t *val, size_t arrsize, bool hasnan) \
    { \
        x86simdsortStatic::keyvalue_qsort(key, val, arrsize, hasnan); \
    } \
    template <> \
    void keyvalue_qsort(type *key, float *val, size_t arrsize, bool hasnan) \
    { \
        x86simdsortStatic::keyvalue_qsort(key, val, arrsize, hasnan); \
    }

namespace xss {
namespace avx512 {
    DEFINE_ALL_METHODS(uint32_t)
    DEFINE_ALL_METHODS(int32_t)
    DEFINE_ALL_METHODS(float)
    DEFINE_ALL_METHODS(uint64_t)
    DEFINE_ALL_METHODS(int64_t)
    DEFINE_ALL_METHODS(double)
    DEFINE_KEYVALUE_METHODS(uint64_t)
    DEFINE_KEYVALUE_METHODS(int64_t)
    DEFINE_KEYVALUE_METHODS(double)
    DEFINE_KEYVALUE_METHODS(uint32_t)
    DEFINE_KEYVALUE_METHODS(int32_t)
    DEFINE_KEYVALUE_METHODS(float)
} // namespace avx512
} // namespace xss
