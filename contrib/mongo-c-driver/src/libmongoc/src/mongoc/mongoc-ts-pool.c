#include "mongoc-ts-pool-private.h"
#include "common-thread-private.h"

#include "bson/bson.h"

/**
 * Toggle this to enable/disable checks that all items are returned to the pool
 * before the pool is destroyed
 */
static const bool audit_pool_enabled = false;

/**
 * To support correct alignment of the item allocated within pool_node::data,
 * pool_node has the following data layout:
 *
 * [ next | owner_pool | (padding) | item ]
 * ^                   ^           ^
 * |                   |           |
 * pool_node *         |           first byte of aligned item
 *                     pool_node::data
 *
 * If alignment of the item is not greater than the alignment of pool_node,
 * then pool_node::data already satisfies the alignment requirements and no
 * padding is necessary. The position of the allocated item should be obtained
 * via _pool_node_data_offset.
 */
typedef struct pool_node {
   struct pool_node *next;
   mongoc_ts_pool *owner_pool;
   unsigned char data[];
} pool_node;

// Flexible member array member should not contribute to sizeof result.
BSON_STATIC_ASSERT2 (pool_node_size, sizeof (pool_node) == sizeof (void *) * 2u);

struct mongoc_ts_pool {
   mongoc_ts_pool_params params;
   pool_node *head;
   /* Number of elements in the pool */
   int32_t size;
   bson_mutex_t mtx;
   /* Number of elements that the pool has given to users.
    * If audit_pool_enabled is zero, this member is unused */
   int32_t outstanding_items;
};

/**
 * @brief Return the offset of the item allocated within pool_node::data.
 */
static size_t
_pool_node_data_offset (const mongoc_ts_pool *pool)
{
   BSON_ASSERT_PARAM (pool);

   const size_t alignment = pool->params.element_alignment;

   // If element type has alignment greater than that of pool_node, position of
   // item within storage region must be offset accordingly.
   if (alignment > BSON_ALIGNOF (pool_node)) {
      return alignment - sizeof (pool_node);
   }

   return 0u;
}

/**
 * @brief Allocate a pool_node object with the appropriate flexible array member
 * length to accomodate the alignment and size of the element type.
 */
static pool_node *
_pool_node_new (const mongoc_ts_pool *pool)
{
   BSON_ASSERT_PARAM (pool);

   const size_t alignment = pool->params.element_alignment;
   const size_t size = pool->params.element_size;
   const size_t minimum_size = sizeof (pool_node) + _pool_node_data_offset (pool) + size;

   if (alignment == 0) {
      return bson_malloc0 (minimum_size);
   }

   // aligned_alloc requires allocation size to be a multiple of the alignment.
   const size_t required_size = minimum_size + (alignment - (minimum_size % alignment));

   return bson_aligned_alloc0 (alignment, required_size);
}

/**
 * @brief Return a pointer to the item owned by the given node.
 */
static const void *
_pool_node_get_data_const (const pool_node *node)
{
   BSON_ASSERT_PARAM (node);
   return node->data + _pool_node_data_offset (node->owner_pool);
}

/**
 * @brief Return a pointer to the item owned by the given node.
 */
static void *
_pool_node_get_data (pool_node *node)
{
   BSON_ASSERT_PARAM (node);
   return node->data + _pool_node_data_offset (node->owner_pool);
}

/**
 * @brief Obtain a pointer to the pool_node that owns the given item.
 */
static pool_node *
_pool_node_from_item (void *item, const mongoc_ts_pool *pool)
{
   return (void *) (((unsigned char *) item) - _pool_node_data_offset (pool) - offsetof (pool_node, data));
}

/**
 * @brief Check whether we should drop the given node from the pool
 */
static bool
_should_prune (const pool_node *node)
{
   mongoc_ts_pool *pool = node->owner_pool;
   return pool->params.prune_predicate &&
          pool->params.prune_predicate (_pool_node_get_data_const (node), pool->params.userdata);
}

/**
 * @brief Create a new pool node and contained element.
 *
 * @return pool_node* A pointer to a constructed element, or NULL if the
 * constructor sets `error`
 */
static pool_node *
_new_item (mongoc_ts_pool *pool, bson_error_t *error)
{
   pool_node *node = _pool_node_new (pool);

   node->owner_pool = pool;
   if (pool->params.constructor) {
      /* To construct, we need to know if that constructor fails */
      bson_error_t my_error;
      if (!error) {
         /* Caller doesn't care about the error, but we care in case the
          * constructor might fail */
         error = &my_error;
      }
      /* Clear the error */
      error->code = 0;
      error->domain = 0;
      error->message[0] = 0;
      /* Construct the object */
      pool->params.constructor (_pool_node_get_data (node), pool->params.userdata, error);
      if (error->code != 0) {
         /* Constructor reported an error. Deallocate and drop the node. */
         bson_free (node);
         node = NULL;
      }
   }
   if (node && audit_pool_enabled) {
      bson_atomic_int32_fetch_add (&pool->outstanding_items, 1, bson_memory_order_relaxed);
   }
   return node;
}

/**
 * @brief Destroy the given node and the element that it contains
 */
static void
_delete_item (pool_node *node)
{
   mongoc_ts_pool *pool = node->owner_pool;
   if (pool->params.destructor) {
      pool->params.destructor (_pool_node_get_data (node), pool->params.userdata);
   }
   bson_free (node);
}

/**
 * @brief Try to take a node from the pool. Returns `NULL` if the pool is empty.
 */
static pool_node *
_try_get (mongoc_ts_pool *pool)
{
   pool_node *node;
   bson_mutex_lock (&pool->mtx);
   node = pool->head;
   if (node) {
      pool->head = node->next;
   }
   bson_mutex_unlock (&pool->mtx);
   if (node) {
      bson_atomic_int32_fetch_sub (&pool->size, 1, bson_memory_order_relaxed);
      if (audit_pool_enabled) {
         bson_atomic_int32_fetch_add (&pool->outstanding_items, 1, bson_memory_order_relaxed);
      }
   }
   return node;
}

mongoc_ts_pool *
mongoc_ts_pool_new (mongoc_ts_pool_params params)
{
   mongoc_ts_pool *r = bson_malloc0 (sizeof (mongoc_ts_pool));
   r->params = params;
   r->head = NULL;
   r->size = 0;
   if (audit_pool_enabled) {
      r->outstanding_items = 0;
   }
   bson_mutex_init (&r->mtx);

   // Promote alignment if it is too small to satisfy bson_aligned_alloc
   // requirements.
   {
      const size_t alignment = r->params.element_alignment;
      if (alignment != 0 && alignment < BSON_ALIGN_OF_PTR) {
         r->params.element_alignment = BSON_ALIGN_OF_PTR;
      }
   }

   return r;
}

void
mongoc_ts_pool_free (mongoc_ts_pool *pool)
{
   if (audit_pool_enabled) {
      BSON_ASSERT (pool->outstanding_items == 0 && "Pool was destroyed while there are still items checked out");
   }
   mongoc_ts_pool_clear (pool);
   bson_mutex_destroy (&pool->mtx);
   bson_free (pool);
}

void
mongoc_ts_pool_clear (mongoc_ts_pool *pool)
{
   pool_node *node;
   {
      bson_mutex_lock (&pool->mtx);
      node = pool->head;
      pool->head = NULL;
      pool->size = 0;
      bson_mutex_unlock (&pool->mtx);
   }
   while (node) {
      pool_node *n = node;
      node = n->next;
      _delete_item (n);
   }
}

void *
mongoc_ts_pool_get_existing (mongoc_ts_pool *pool)
{
   pool_node *node;
retry:
   node = _try_get (pool);
   if (node && _should_prune (node)) {
      /* This node should be pruned now. Drop it and try again. */
      mongoc_ts_pool_drop (pool, _pool_node_get_data (node));
      goto retry;
   }
   return node ? _pool_node_get_data (node) : NULL;
}

void *
mongoc_ts_pool_get (mongoc_ts_pool *pool, bson_error_t *error)
{
   pool_node *node;
retry:
   node = _try_get (pool);
   if (node && _should_prune (node)) {
      /* This node should be pruned now. Drop it and try again. */
      mongoc_ts_pool_drop (pool, _pool_node_get_data (node));
      goto retry;
   }
   if (node == NULL) {
      /* We need a new item */
      node = _new_item (pool, error);
   }
   if (node == NULL) {
      /* No item in pool, and we couldn't create one either */
      return NULL;
   }
   return _pool_node_get_data (node);
}

void
mongoc_ts_pool_return (mongoc_ts_pool *pool, void *item)
{
   pool_node *node = _pool_node_from_item (item, pool);

   BSON_ASSERT (pool == node->owner_pool);

   if (_should_prune (node)) {
      mongoc_ts_pool_drop (pool, item);
   } else {
      bson_mutex_lock (&pool->mtx);
      node->next = pool->head;
      pool->head = node;
      bson_mutex_unlock (&pool->mtx);
      bson_atomic_int32_fetch_add (&node->owner_pool->size, 1, bson_memory_order_relaxed);
      if (audit_pool_enabled) {
         bson_atomic_int32_fetch_sub (&node->owner_pool->outstanding_items, 1, bson_memory_order_relaxed);
      }
   }
}

void
mongoc_ts_pool_drop (mongoc_ts_pool *pool, void *item)
{
   pool_node *node = _pool_node_from_item (item, pool);

   BSON_ASSERT (pool == node->owner_pool);

   if (audit_pool_enabled) {
      bson_atomic_int32_fetch_sub (&node->owner_pool->outstanding_items, 1, bson_memory_order_relaxed);
   }
   _delete_item (node);
}

int
mongoc_ts_pool_is_empty (const mongoc_ts_pool *pool)
{
   return mongoc_ts_pool_size (pool) == 0;
}

size_t
mongoc_ts_pool_size (const mongoc_ts_pool *pool)
{
   return bson_atomic_int32_fetch (&pool->size, bson_memory_order_relaxed);
}

void
mongoc_ts_pool_visit_each (mongoc_ts_pool *pool,
                           void *visit_userdata,
                           int (*visit) (void *item, void *pool_userdata, void *visit_userdata))
{
   /* Pointer to the pointer that must be updated in case of an item pruning */
   pool_node **node_ptrptr;
   /* The node we are looking at */
   pool_node *node;
   bson_mutex_lock (&pool->mtx);
   node_ptrptr = &pool->head;
   node = pool->head;
   while (node) {
      const bool should_remove = visit (_pool_node_get_data (node), pool->params.userdata, visit_userdata);
      pool_node *const next_node = node->next;
      if (!should_remove) {
         node_ptrptr = &node->next;
         node = next_node;
         continue;
      }
      /* Retarget the previous pointer to the next node in line */
      *node_ptrptr = node->next;
      _delete_item (node);
      pool->size--;
      /* Leave node_ptrptr pointing to the previous pointer, because we may
       * need to erase another item */
      node = next_node;
   }
   bson_mutex_unlock (&pool->mtx);
}
