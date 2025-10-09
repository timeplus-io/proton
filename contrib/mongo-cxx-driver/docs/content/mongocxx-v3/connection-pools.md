+++
date = "2017-04-18 13:39:39-04:00"
title = "Connection pools"
[menu.main]
  weight = 15
  parent="mongocxx3"
+++

## `mongocxx::client` vs `mongocxx::pool`

A standalone `mongocxx::client` uses a single-threaded algorithm to
monitor the state of the cluster it's connected to. When connected to a
replica set, the thread "stops the world" every 60 seconds to check the
status of the cluster. A `mongocxx::pool`, on the other hand, uses a
separate background thread for each server in the cluster, each of which
checks the status of the server it monitors every 10 seconds. Because of
the performance advantages of monitoring the cluster in the background 
rather than "stopping the world", it's highly recommended to use a
`mongocxx::pool` rather than a set of standalone clients if your
application has access to multiple threads, even if your application only
uses one thread.

## Connection pools and thread safety

A `mongocxx::pool` can be shared across multiple threads and used to create
clients. However, each `mongocxx::client` can only be used in a single
thread. See the [thread safety documentation](../thread-safety/) for
details on how to use a `mongocxx::client` in a thread-safe manner.

## Configuring a connection pool

The number of clients in a connection pool is determined by the URI
parameters `minPoolSize` and `maxPoolSize`. The `minPoolSize` and
`maxPoolSize` options set resource usage targets for when the driver is
idle or fully-utilized.  When fully utilized, up to maxPoolSize clients
are available. When clients are returned to the pool, they are destroyed
until the pool has shrunk again to the minPoolSize.

‌‌            | 
------------|---
maxPoolSize | The maximum number of clients created by a mongocxx::pool (both in the pool and checked out). The default value is 100. Once it is reached, mongocxx::pool::acquire blocks until another thread returns a client to the pool.
minPoolSize | Sets a target size for the pool when idle.  Once this many clients have been created, there will never be fewer than this many clients in the pool. If additional clients above minPoolSize are created, they will be destroyed when returned to the pool. The default value is "0", which disables this feature. When disabled, clients are never destroyed.

## Using a connection pool

To use a connection pool, first create a `mongocxx::pool`, passing the URI
as an argument. Then, call `mongocxx::pool::acquire` to receive a client
from the pool. The client will automatically be returned to the pool when
it goes out of scope.

See the [connection pool example](https://github.com/mongodb/mongo-cxx-driver/blob/master/examples/mongocxx/pool.cpp)
for more details.
