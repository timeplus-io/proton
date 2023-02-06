select * from (select 0 as k, to_int128('18446744073709551617') as v) t1 asof join (select 0 as k, to_int128('18446744073709551616') as v) t2 using(k, v);
select * from (select 0 as k, to_int256('340282366920938463463374607431768211457') as v) t1 asof join (select 0 as k, to_int256('340282366920938463463374607431768211456') as v) t2 using(k, v);

select * from (select 0 as k, to_uint128('18446744073709551617') as v) t1 asof join (select 0 as k, to_uint128('18446744073709551616') as v) t2 using(k, v);
select * from (select 0 as k, to_uint256('340282366920938463463374607431768211457') as v) t1 asof join (select 0 as k, to_uint256('340282366920938463463374607431768211456') as v) t2 using(k, v);
