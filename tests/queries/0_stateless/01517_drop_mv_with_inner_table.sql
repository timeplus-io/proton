-- Tags: no-parallel

--
-- Atomic no SYNC
-- (should go first to check that thread for DROP STREAM does not hang)
--
SET query_mode = 'table';
drop database if exists db_01517_atomic;
create database db_01517_atomic Engine=Atomic;

create stream db_01517_atomic.source (key int) engine=Null;
create materialized view db_01517_atomic.mv engine=Null as select * from db_01517_atomic.source;

drop stream db_01517_atomic.mv;
-- ensure that the inner had been removed after sync drop
drop stream db_01517_atomic.source sync;
show tables from db_01517_atomic;

--
-- Atomic
--
drop database if exists db_01517_atomic_sync;
create database db_01517_atomic_sync Engine=Atomic;

create stream db_01517_atomic_sync.source (key int) engine=Null;
create materialized view db_01517_atomic_sync.mv engine=Null as select * from db_01517_atomic_sync.source;

-- drops it and hangs with Atomic engine, due to recursive DROP
drop stream db_01517_atomic_sync.mv sync;
show tables from db_01517_atomic_sync;

--
-- Ordinary
---
drop database if exists db_01517_ordinary;
create database db_01517_ordinary Engine=Ordinary;

create stream db_01517_ordinary.source (key int) engine=Null;
create materialized view db_01517_ordinary.mv engine=Null as select * from db_01517_ordinary.source;

-- drops it and hangs with Atomic engine, due to recursive DROP
drop stream db_01517_ordinary.mv sync;
show tables from db_01517_ordinary;

drop stream db_01517_atomic_sync.source;
drop stream db_01517_ordinary.source;

drop database db_01517_atomic;
drop database db_01517_atomic_sync;
drop database db_01517_ordinary;
