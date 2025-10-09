/*
 * Copyright 2020-present MongoDB, Inc.
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

#ifndef MONGOC_STREAM_INTERRUPTIBLE_PRIVATE_H
#define MONGOC_STREAM_INTERRUPTIBLE_PRIVATE_H

#include "mongoc-stream.h"

/* Creates a stream to use to interrupt calls to mongoc_stream_poll.
 *
 * The expected use is to cancel in-progress hello commands (especially for
 * awaitable hello). A hello command may not respond for a long time, so
 * reading the reply may block on mongoc_stream_poll until data is readable. To
 * interrupt mongoc_stream_poll, a stream retrieved by
 * _mongoc_interrupt_get_stream can be added to the call of poll. Any other
 * thread can call _mongoc_interrupt_interrupt to write to that stream.
 */
typedef struct _mongoc_interrupt_t mongoc_interrupt_t;

mongoc_interrupt_t *
_mongoc_interrupt_new (uint32_t timeout_ms);

/* Interrupt the stream. An in progress poll for POLLIN should return. */
bool
_mongoc_interrupt_interrupt (mongoc_interrupt_t *interrupt);

/* Returns a socket stream, that can be polled alongside other
 * socket streams. */
mongoc_stream_t *
_mongoc_interrupt_get_stream (mongoc_interrupt_t *interrupt);

/* Flushes queued data on an interrupt.
 *
 * This is not guaranteed to flush all data, but it does not block.
 */
bool
_mongoc_interrupt_flush (mongoc_interrupt_t *interrupt);

void
_mongoc_interrupt_destroy (mongoc_interrupt_t *interrupt);

#endif /* MONGOC_STREAM_INTERRUPTIBLE_PRIVATE_H */
