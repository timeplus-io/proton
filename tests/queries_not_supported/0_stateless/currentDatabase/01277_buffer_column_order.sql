-- Check for Block::sortColumns(), can be done using Buffer.
SET query_mode = 'table';
drop stream if exists out_01277;
drop stream if exists in_01277;
drop stream if exists buffer_01277;
drop stream if exists mv_01277_1;
drop stream if exists mv_01277_2;

create stream out_01277
(
    k1 int,
    k2 int,
    a1 int,
    a2 int,
    b1 int,
    b2 int,
    c  int
) Engine=Null();
create stream buffer_01277 as out_01277 Engine=Buffer(currentDatabase(), out_01277, 1,
    86400, 86400,
    1e5, 1e6,
    10e6, 100e6);
create stream in_01277 as out_01277 Engine=Null();

-- differs in order of fields in SELECT clause
create materialized view mv_01277_1 to buffer_01277 as select k1, k2, a1, a2, b1, b2, c from in_01277;
create materialized view mv_01277_2 to buffer_01277 as select a1, a2, k1, k2, b1, b2, c from in_01277;

-- column order is ignored, just for humans
insert into mv_01277_1 select
    number k1,
    number k2,
    number a1,
    number a2,
    number b1,
    number b2,
    number c
from numbers(1);

-- with wrong order in Block::sortColumns() triggers:
--
--     Code: 171. DB::Exception: Received from localhost:9000. DB::Exception: Block structure mismatch in Buffer stream: different names of columns:
--     c int32 int32(size = 1), b2 int32 int32(size = 1), a2 int32 int32(size = 1), a1 int32 int32(size = 1), k2 int32 int32(size = 1), b1 int32 int32(size = 1), k1 int32 int32(size = 1)
--     c int32 int32(size = 1), b2 int32 int32(size = 1), k2 int32 int32(size = 1), a1 int32 int32(size = 1), b1 int32 int32(size = 1), k1 int32 int32(size = 1), a2 int32 int32(size = 1).
insert into mv_01277_2 select
    number a1,
    number a2,
    number k1,
    number k2,
    number b1,
    number b2,
    number c
from numbers(1);

drop stream mv_01277_1;
drop stream mv_01277_2;
drop stream buffer_01277;
drop stream out_01277;
drop stream in_01277;
