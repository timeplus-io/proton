drop stream if exists utf8_overlap;
create stream utf8_overlap (str string) engine=Memory();

-- { echoOn }
-- NOTE: total string size should be > 16 (sizeof(__m128i))
insert into utf8_overlap values ('\xe2'), ('Foo⚊BarBazBam'), ('\xe2'), ('Foo⚊BarBazBam');
--                                             ^
--                                             MONOGRAM FOR YANG
with lower_utf8(str) as l_, upper_utf8(str) as u_, '0x' || hex(str) as h_
select length(str), if(l_ == '\xe2', h_, l_), if(u_ == '\xe2', h_, u_) from utf8_overlap format CSV;

-- NOTE: regression test for introduced bug
-- https://github.com/ClickHouse/ClickHouse/issues/42756
SELECT lower_utf8('КВ АМ И СЖ');
SELECT upper_utf8('кв ам и сж');
SELECT lower_utf8('КВ АМ И СЖ КВ АМ И СЖ');
SELECT upper_utf8('кв ам и сж кв ам и сж');
-- Test at 32 and 64 byte boundaries
SELECT lower_utf8(repeat('0', 16) || 'КВ АМ И СЖ');
SELECT upper_utf8(repeat('0', 16) || 'кв ам и сж');
SELECT lower_utf8(repeat('0', 48) || 'КВ АМ И СЖ');
SELECT upper_utf8(repeat('0', 48) || 'кв ам и сж');
