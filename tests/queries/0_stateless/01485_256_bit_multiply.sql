select count() from
(
    select to_int128(number) * number x, toInt256(number) * number y from numbers_mt(100000000) where x != y
);
