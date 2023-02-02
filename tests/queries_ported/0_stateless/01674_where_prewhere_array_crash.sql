drop stream if exists tab;
create stream tab  (x uint64, `arr.a` array(uint64), `arr.b` array(uint64)) engine = MergeTree order by x;
select x from tab array join arr prewhere x != 0 where arr; -- { serverError 47; }
select x from tab array join arr prewhere arr where x != 0; -- { serverError 47; }
drop stream if exists tab;
