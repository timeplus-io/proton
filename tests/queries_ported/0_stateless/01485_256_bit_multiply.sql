set query_mode='table';

select count() from
(
    select to_int128(number) * number as x, to_int256(number) * number as y from numbers_mt(100000000) where x != y
);
