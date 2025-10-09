#include "mongoc-prelude.h"

#ifndef MONGOC_TS_POOL_PRIVATE_H
#define MONGOC_TS_POOL_PRIVATE_H

#include <bson/bson.h>

#include <stddef.h>

struct _bson_error_t;

/** Type of an object constructor function */
typedef void (*_erased_constructor_fn) (void *self, void *userdata, struct _bson_error_t *error_out);
/** Type of an object destructor function */
typedef void (*_erased_destructor_fn) (void *self, void *userdata);
/** Type of an object pruning predicate */
typedef int (*_erased_prune_predicate) (const void *self, void *userdata);

/**
 * @brief Construction parameters for creating a new object pool.
 */
typedef struct mongoc_ts_pool_params {
   /**
    * @brief The alignment of the objects that are managed by the pool
    */
   size_t element_alignment;
   /**
    * @brief The size of the objects that are managed by the pool
    */
   size_t element_size;
   /**
    * @brief Arbitrary data pointer that is passed to the
    * constructor/destructor/prune_predicate functions
    */
   void *userdata;
   /**
    * @brief A function that is called on a newly-allocated object in the pool.
    *
    * If `NULL`, newly created objects are just zero-initialized.
    *
    * Called as `constructor(item_ptr, userdata_ptr, bson_error_ptr)`.
    *
    * The `bson_error_ptr` is never `NULL`. If the bson_error_ptr->code is
    * non-zero, the pool will consider the constructor to have failed, and will
    * deallocate the item without destroying it, and then report that failure to
    * the caller of `mongoc_ts_pool_get()` that caused the creation of the new
    * item.
    */
   _erased_constructor_fn constructor;
   /**
    * @brief A function that will destroy an item before it is deallocated.
    *
    * If `NULL`, destructing an object is a no-op.
    *
    * Called as `destructor(item_ptr, userdata_ptr)`.
    */
   _erased_destructor_fn destructor;
   /**
    * @brief A predicate function that is used to automatically drop items from
    * the pool.
    *
    * If `NULL`, item are never automatically dropped from the pool, and
    * can only be discarded by use of `mongoc_ts_pool_drop()`. Items will still
    * be dropped when the pool is freed, though.
    *
    * Called as `prune_predicate(item_ptr, userdata_ptr)`.
    *
    * If this function returns non-zero, this informs the pool that the item
    * should not be returned to the pool nor yielded to a pool user. Instead,
    * the item will be given to `mongoc_ts_pool_drop`.
    */
   _erased_prune_predicate prune_predicate;
} mongoc_ts_pool_params;

/**
 * @brief A thread-safe object pool.
 *
 * Object pools act as a thread-safe stack of reusable objects. The pool can be
 * given a prune-predicate function that will be used to discard elements
 * automatically if they meet some user-specified condition.
 *
 * When an item is taken from a pool, the pool will either create a new item or
 * return the *most-recently-returned* *non-pruned* item. i.e. The pool acts as
 * a LIFO stack.
 *
 * Objects are created *automatically* by the pool: Only objects obtained from a
 * pool instance can be returned to that pool, and all objects obtained from
 * a given pool must either be returned to that same pool, or dropped using
 * `mongoc_ts_pool_drop`.
 *
 * Refer to `mongoc_ts_pool_params` for more information on the construction,
 * destruction, and pruning of pool items.
 */
typedef struct mongoc_ts_pool mongoc_ts_pool;

/**
 * @brief Create a new thread-safe pool
 *
 * @param params The operating parameters for the pool. @see
 * mongoc_ts_pool_params
 *
 * @returns A new thread-safe pool constructed with the given parameters.
 *
 * @note The pool must be destroyed using `mongoc_ts_pool_free`
 */
mongoc_ts_pool *
mongoc_ts_pool_new (mongoc_ts_pool_params params);

/**
 * @brief Destroy a pool of objects previously created with `mongo_ts_pool_new`
 *
 * Any objects remaining in the pool will also be destroyed.
 *
 * @note All objects that have been obtained from the pool must be returned to
 * the pool before it is freed.
 */
void
mongoc_ts_pool_free (mongoc_ts_pool *pool);

/**
 * @brief Obtain an object from the pool.
 *
 * If the pool is empty, the pool will try to create a new item and return it.
 * If the constructor of that item fails, this function will return `NULL` and
 * set an error in `error`.
 *
 * @param pool The pool of objects.
 * @param error An error out-parameter. If the constructor of an object is
 * called and fails, it will set an error through this parameter.
 * @returns A pointer to an object associated with the pool, or `NULL` if the
 * object's constructor fails.
 *
 * @note Returns the most-recently-returned non-pruned item from the pool (i.e.
 * the pool is a LIFO stack).
 *
 * @note If the return value is `NULL`, then an error will be set in `*error`.
 * If the return value is non-`NULL`, then the value of `*error` is unspecified.
 *
 * @note A non-NULL returned item MUST be passed to either
 * `mongo_ts_pool_return` or `mongo_ts_pool_drop` BEFORE the pool is destroyed
 * with `mongo_ts_pool_free`.
 */
void *
mongoc_ts_pool_get (mongoc_ts_pool *pool, struct _bson_error_t *error);

/**
 * @brief Attempt to pop an object from the pool.
 *
 * Unlike `mongoc_ts_pool_get`, if the pool is empty, this function returns
 * `NULL` unconditionally.
 *
 * @param pool The pool of objects.
 * @returns A pointer to an object previously passed to `mongo_ts_pool_push`,
 *               or `NULL` if the pool is empty.
 *
 * @note Returns the most-recently-returned non-pruned item from the pool
 * (i.e. the pool is a LIFO stack).
 */
void *
mongoc_ts_pool_get_existing (mongoc_ts_pool *pool);

/**
 * @brief Return an object obtained from a pool back to the pool that manages it
 *
 * @param item A pointer that was previously returned from a call to
 * `mongoc_ts_pool_get` or `mongoc_ts_pool_get_existing`
 */
void
mongoc_ts_pool_return (mongoc_ts_pool *pool, void *item);

/**
 * @brief Obtain the number of elements in the pool.
 *
 * @note If the pool could be modified by another thread simultaneously, then
 * the return value may become immediately stale.
 */
size_t
mongoc_ts_pool_size (const mongoc_ts_pool *pool);

/**
 * @brief Determine whether the pool is empty.
 *
 * @note If the pool could be modified by another thread simultaneously, then
 * the result may become immediately stale.
 */
int
mongoc_ts_pool_is_empty (const mongoc_ts_pool *pool);

/**
 * @brief Destroy all items currently in the given pool.
 *
 * Objects that are "checked-out" of the pool are unaffected.
 *
 * @note This does not free the pool. For that purpose, us `mongoc_ts_pool_free`
 */
void
mongoc_ts_pool_clear (mongoc_ts_pool *pool);

/**
 * @brief Destroy an item that was created by a pool
 *
 * Instead of returning to the pool, the item will be destroyed and deallocated.
 *
 * @param item A pointer returned by `mongoc_ts_pool_get` or
 * `mongo_ts_pool_get_existing`.
 */
void
mongoc_ts_pool_drop (mongoc_ts_pool *pool, void *item);

/**
 * @brief Visit each element of the pool, optionally pruning items.
 *
 * @note While this visit function is executing, all pool operations are
 * blocked on all threads. Attempting to return/get/drop items in the pool from
 * within the visit callback will deadlock.
 *
 * The visit function will be called as:
 *
 *    visit(pool_item, pool_userdata, visit_userdata)
 *
 * If the visitor function returns non-zero for an item, then that item will be
 * removed from the pool.
 *
 * @param pool The pool of items to visit
 * @param visit_userdata Optional additional context for the visitor
 * @param visitor The visit function
 *
 */
void
mongoc_ts_pool_visit_each (mongoc_ts_pool *pool,
                           void *visit_userdata,
                           int (*visitor) (void *item, void *pool_userdata, void *visit_userdata));

/**
 * @brief Declare a thread-safe pool type that contains elements of a specific
 * type. Wraps a `mongoc_ts_pool`.
 *
 * @param ElementType The type of object contained in the pool
 * @param PoolName The name of the pool type. All methods of the pool will be
 * prefixed by this name.
 * @param UserDataType The type of the userdata associated with the constructor,
 * destructor, and prune predicate. May be `void`.
 * @param Constructor A function that constructs new elements for the pool, or
 * `NULL`
 * @param Destructor An element destructor function, or `NULL`
 * @param PrunePredicate A function that checks whether elements should be
 * dropped from the pool, or `NULL`
 */
#define MONGOC_DECL_SPECIAL_TS_POOL(ElementType, PoolName, UserDataType, Constructor, Destructor, PrunePredicate) \
   typedef struct PoolName {                                                                                      \
      mongoc_ts_pool *pool;                                                                                       \
   } PoolName;                                                                                                    \
                                                                                                                  \
   BSON_MAYBE_UNUSED static BSON_INLINE PoolName PoolName##_new_with_params (                                     \
      void (*constructor) (ElementType *, UserDataType *, struct _bson_error_t *),                                \
      void (*destructor) (ElementType *, UserDataType *),                                                         \
      int (*prune_predicate) (ElementType *, UserDataType *),                                                     \
      UserDataType *userdata)                                                                                     \
   {                                                                                                              \
      PoolName ret;                                                                                               \
      mongoc_ts_pool_params params = {0};                                                                         \
      params.userdata = userdata;                                                                                 \
      params.constructor = (_erased_constructor_fn) constructor;                                                  \
      params.destructor = (_erased_destructor_fn) destructor;                                                     \
      params.prune_predicate = (_erased_prune_predicate) prune_predicate;                                         \
      params.element_alignment = BSON_ALIGNOF (ElementType);                                                      \
      params.element_size = sizeof (ElementType);                                                                 \
      ret.pool = mongoc_ts_pool_new (params);                                                                     \
      return ret;                                                                                                 \
   }                                                                                                              \
                                                                                                                  \
   BSON_MAYBE_UNUSED static BSON_INLINE PoolName PoolName##_new (UserDataType *userdata)                          \
   {                                                                                                              \
      return PoolName##_new_with_params (Constructor, Destructor, PrunePredicate, userdata);                      \
   }                                                                                                              \
                                                                                                                  \
   BSON_MAYBE_UNUSED static BSON_INLINE void PoolName##_free (PoolName p)                                         \
   {                                                                                                              \
      mongoc_ts_pool_free (p.pool);                                                                               \
   }                                                                                                              \
                                                                                                                  \
   BSON_MAYBE_UNUSED static BSON_INLINE void PoolName##_clear (PoolName p)                                        \
   {                                                                                                              \
      mongoc_ts_pool_clear (p.pool);                                                                              \
   }                                                                                                              \
                                                                                                                  \
   BSON_MAYBE_UNUSED static BSON_INLINE ElementType *PoolName##_get_existing (PoolName p)                         \
   {                                                                                                              \
      return (ElementType *) mongoc_ts_pool_get_existing (p.pool);                                                \
   }                                                                                                              \
                                                                                                                  \
   BSON_MAYBE_UNUSED static BSON_INLINE ElementType *PoolName##_get (PoolName p, struct _bson_error_t *error)     \
   {                                                                                                              \
      return (ElementType *) mongoc_ts_pool_get (p.pool, error);                                                  \
   }                                                                                                              \
                                                                                                                  \
   BSON_MAYBE_UNUSED static BSON_INLINE void PoolName##_return (PoolName p, ElementType *elem)                    \
   {                                                                                                              \
      mongoc_ts_pool_return (p.pool, elem);                                                                       \
   }                                                                                                              \
                                                                                                                  \
   BSON_MAYBE_UNUSED static BSON_INLINE void PoolName##_drop (PoolName p, ElementType *elem)                      \
   {                                                                                                              \
      mongoc_ts_pool_drop (p.pool, elem);                                                                         \
   }                                                                                                              \
                                                                                                                  \
   BSON_MAYBE_UNUSED static BSON_INLINE size_t PoolName##_size (PoolName p)                                       \
   {                                                                                                              \
      return mongoc_ts_pool_size (p.pool);                                                                        \
   }                                                                                                              \
                                                                                                                  \
   BSON_MAYBE_UNUSED static BSON_INLINE int PoolName##_is_empty (PoolName p)                                      \
   {                                                                                                              \
      return mongoc_ts_pool_is_empty (p.pool);                                                                    \
   }                                                                                                              \
                                                                                                                  \
   BSON_MAYBE_UNUSED static BSON_INLINE void PoolName##_visit_each (                                              \
      PoolName p,                                                                                                 \
      void *visit_userdata,                                                                                       \
      int (*visitor) (ElementType * elem, UserDataType * pool_userdata, void *visit_userdata))                    \
   {                                                                                                              \
      mongoc_ts_pool_visit_each (p.pool, visit_userdata, (int (*) (void *, void *, void *)) visitor);             \
   }

#endif /* MONGOC_TS_POOL_PRIVATE_H */
