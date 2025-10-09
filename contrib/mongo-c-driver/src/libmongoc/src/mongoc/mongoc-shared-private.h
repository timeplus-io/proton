/*
 * Copyright 2021 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "mongoc-prelude.h"

#ifndef MONGOC_SHARED_H
#define MONGOC_SHARED_H

#include <bson/bson.h>

#include <stddef.h>

/**
 * @brief A ref-counted thread-safe shared pointer to arbitrary data.
 *
 * `shared_ptr` instances manage the lifetime of a pointed-to object when the
 * precise time to destroy that managed object is indeterminate, such as with
 * shared state in a multithreaded or asynchronous program.
 *
 * The pointed-to object of a shared_ptr instance can be accessed via
 * the `ptr` member of the shared_ptr object. Assigning-to the `ptr` member of
 * a shared_ptr is legal, and can be done to change the pointed-to object of a
 * shared_ptr without changing which object is being managed. This can be done
 * to return a pointer to a subobject of the managed object without giving
 * out a reference to the full managed object.
 *
 * A new managed object with a `shared_ptr` is created with
 * `mongoc_shared_ptr_create`, which starts with an initial shared reference
 * count of `1`. To take another reference to keep the managed object alive,
 * use `mongoc_shared_ptr_copy`. When one is done with a managed resource, the
 * shared reference should be dropped using `mongoc_shared_ptr_reset_null`.
 *
 * When an operation on a shared_ptr causes the reference count to drop to zero,
 * the deleter that was given to create that shared state will immediately be
 * invoked with the pointed-to-data. The deleter runs in the thread that
 * caused the reference count to drop, so be aware that resetting or assigning
 * a shared_ptr can execute unseen code: Refrain from holding locks while
 * resetting/assigning a shared pointer.
 */
typedef struct mongoc_shared_ptr {
   /** Pointed-to data */
   void *ptr;
   /** Auxilary book-keeping. Do not touch. */
   struct _mongoc_shared_ptr_aux *_aux;
} mongoc_shared_ptr;

/**
 * @brief A "null" pointer constant for a mongoc_shared_ptr.
 */
#define MONGOC_SHARED_PTR_NULL \
   ((mongoc_shared_ptr){       \
      .ptr = NULL,             \
      ._aux = NULL,            \
   })

/**
 * @brief Reassign a shared pointer to manage the given resource
 *
 * @param ptr The shared pointer that will be rebound
 * @param pointee The pointer that we will point to.
 * @param deleter A deleter for `pointee`, to be called when the refcount
 * reaches zero.
 *
 * @note Equivalent to:
 *
 *    mongoc_shared_ptr_reset_null(ptr);
 *    *ptr = mongoc_shared_ptr_create(pointee, deleter);
 */
extern void
mongoc_shared_ptr_reset (mongoc_shared_ptr *ptr, void *pointee, void (*deleter) (void *));

/**
 * @brief Reassign the given shared pointer to manage the same resource as
 * 'from'
 *
 * If `dest` manages an existing object, the reference count of that managed
 * object will be decremented. If this causes its refcount to reach zero, then
 * the deleter function will be executed with the pointee.
 *
 * @param dest The shared pointer to change
 * @param from The shared pointer to take from
 *
 * @note Equivalent to:
 *
 *    mongoc_shared_ptr_reset_null(dest);
 *    *dest = mongoc_shared_ptr_copy(from);
 */
extern void
mongoc_shared_ptr_assign (mongoc_shared_ptr *dest, mongoc_shared_ptr from);

/**
 * @brief Reassign the given shared pointer to manage the same resource as
 * 'from'
 *
 * This atomic function is safe to call between threads when 'dest' may be
 * accessed simultaneously from another thread even if any of those accesses are
 * a write. However: Any potential reads *must* be done using
 * `mongoc_atomic_shared_ptr_load` and any potential writes *must* be done using
 * `mongoc_atomic_shared_ptr_store`.
 *
 * @param dest The shared pointer to change
 * @param from The shared pointer to take from
 *
 * Thread-safe equivalent of `mongoc_shared_ptr_assign`
 */
extern void
mongoc_atomic_shared_ptr_store (mongoc_shared_ptr *dest, mongoc_shared_ptr from);

/**
 * @brief Create a copy of the given shared pointer. Increases the reference
 * count on the object.
 *
 * @param ptr The pointer to copy from
 * @returns a new shared pointer that has the same pointee as ptr
 *
 * @note Must later reset/reassign the returned shared pointer
 */
extern mongoc_shared_ptr
mongoc_shared_ptr_copy (mongoc_shared_ptr ptr);

/**
 * @brief Like `mongoc_shared_ptr_copy`, but is thread-safe in case `*ptr`
 * may be accessed simultaneously from other threads even if any of those
 * accesses are writes. However: such potential writes *must* use
 * `mongoc_atomic_shared_ptr_store`.
 *
 * This is a thread-safe equivalent of `mongoc_shared_ptr_copy`.
 *
 * @note Must later reset/reassign the returned shared pointer
 */
extern mongoc_shared_ptr
mongoc_atomic_shared_ptr_load (mongoc_shared_ptr const *ptr);

/**
 * @brief Release the ownership of the given shared pointer.
 *
 * The shared pointer object and ptr->ptr will be reset to NULL, and the
 * reference count of the managed object will be decremented.
 *
 * If this causes the refcount to reach zero, then the deleter function will
 * be executed with the pointee.
 *
 * @param ptr The pointer to release and set to NULL
 *
 * @note This function is not thread safe if other threads may be
 * writing to `ptr` simultaneously. To do a thread-safe null-reset of a shared
 * pointer, use mongoc_atomic_shared_ptr_store() with a null
 * mongoc_shared_ptr as the 'from' argument
 */
extern void
mongoc_shared_ptr_reset_null (mongoc_shared_ptr *ptr);

/**
 * @brief Obtain the number of hard references to the resource managed by the
 * given shared pointer. This should only be used for diagnostic and assertive
 * purposes.
 *
 * @param ptr A non-null shared pointer to check
 * @return int A positive integer reference count
 */
extern int
mongoc_shared_ptr_use_count (mongoc_shared_ptr ptr);

/**
 * @brief Check whether the given shared pointer is managing a resource.
 *
 * @note The ptr.ptr MAY be NULL while the shared pointer is still managing
 * a resource.
 *
 * @return true If the pointer is managing a resource
 * @return false Otherwise
 */
static BSON_INLINE int
mongoc_shared_ptr_is_null (mongoc_shared_ptr ptr)
{
   return ptr._aux == 0;
}

/**
 * @brief Create a new shared pointer that manages the given resource, or NULL
 *
 * @param pointee The target of the pointer. Should be NULL or a dynamically
 * allocated data segment
 * @param deleter The deleter for the pointer. If `pointee` is non-NULL,
 * `deleter` must be non-NULL. This deleter will be called when the reference
 * count reaches zero. If should release the resources referred-to by `pointee`.
 */
extern mongoc_shared_ptr
mongoc_shared_ptr_create (void *pointee, void (*deleter) (void *));

#endif /* MONGOC_SHARED_H */
