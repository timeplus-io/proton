+++
date = "2016-08-15T16:11:58+05:30"
title = "Thread and fork safety"
[menu.main]
  weight = 14
  parent="mongocxx3"
+++

TLDR: **Always give each thread its own `mongocxx::client`**.

In general each `mongocxx::client` object AND all of its child objects,
including `mongocxx::client_session`, `mongocxx::database`, `mongocxx::collection`,
and `mongocxx::cursor`, **should be used by a single thread at a time**. This
is true even for clients acquired from a `mongocxx::pool`.

Even if you create multiple child objects from a single `client`, and
synchronize them individually, that is unsafe as they will concurrently
modify internal structures of the `client`. The same is true if you copy a
child object.

## Never do this

```c++
mongocxx::instance instance{};
mongocxx::uri uri{};
mongocxx::client c{uri};
auto db1 = c["db1"];
auto db2 = c["db2"];
std::mutex db1_mtx{};
std::mutex db2_mtx{};

auto threadfunc = [](mongocxx::database& db, std::mutex& mtx) {
  mtx.lock();
  db["col"].insert_one({});
  mtx.unlock();
};

// BAD! These two databases are individually synchronized, but they are derived from the same
// client, so they can only be accessed by one thread at a time
std::thread t1([&]() { threadfunc(db1, db1_mtx); threadfunc(db2, db2_mtx); });
std::thread t2([&]() { threadfunc(db2, db2_mtx); threadfunc(db1, db1_mtx); });

t1.join();
t2.join();
```

In the above example, even though the two databases are individually
synchronized, they are derived from the same client. There is shared state
inside the library that is now being modified without synchronization. The
same problem occurs if `db2` is a copy of `db1`.

## A better version

```c++
mongocxx::instance instance{};
mongocxx::uri uri{};
mongocxx::client c1{uri};
mongocxx::client c2{uri};
std::mutex c1_mtx{};
std::mutex c2_mtx{};

auto threadfunc = [](std::string dbname, mongocxx::client& client, std::mutex& mtx) {
  mtx.lock();
  client[dbname]["col"].insert_one({});
  mtx.unlock();
};

// These two clients are individually synchronized, so it is safe to share them between
// threads.
std::thread t1([&]() { threadfunc("db1", c1, c1_mtx); threadfunc("db2", c2, c2_mtx); });
std::thread t2([&]() { threadfunc("db2", c2, c2_mtx); threadfunc("db1", c1, c1_mtx); });

t1.join();
t2.join();
```

## The best version

```c++
mongocxx::instance instance{};
mongocxx::pool pool{mongocxx::uri{}};

auto threadfunc = [](mongocxx::client& client, std::string dbname) {
  auto col = client[dbname]["col"].insert_one({});
};

// Great! Using the pool allows the clients to be synchronized while sharing only one
// background monitoring thread.
std::thread t1 ([&]() {
  auto c = pool.acquire();
  threadfunc(*c, "db1");
  threadfunc(*c, "db2");
});

std::thread t2 ([&]() {
  auto c = pool.acquire();
  threadfunc(*c, "db2");
  threadfunc(*c, "db1");
});

t1.join();
t2.join();
```

In most programs, clients will be long lived - so it is less of a hassle (and
more performant) to make one per-thread. Obviously in this contrived example,
there's quite a bit of overhead because we're doing so little work with each
client - but in a real program this is the best solution.

## Fork safety

Neither a `mongocxx::client` or a `mongocxx::pool` can be safely copied
when forking. Because of this, any client or pool must be created *after*
forking, not before.
