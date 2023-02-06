select to_type_name(make_date(1991, 8, 24));
select to_type_name(make_date(cast(1991 as nullable(uint64)), 8, 24));
select to_type_name(make_date(1991, cast(8 as nullable(uint64)), 24));
select to_type_name(make_date(1991, 8, cast(24 as nullable(uint64))));
select to_type_name(make_date(1991, cast(8 as nullable(uint64)), cast(24 as nullable(uint64))));

select make_date(1970, 01, 01);
select make_date(2020, 08, 24);
select make_date(1980, 10, 17);
select make_date(-1980, 10, 17);
select make_date(1980, -10, 17);
select make_date(1980, 10, -17);
select make_date(1980.0, 9, 30.0/2);
select make_date(-1980.0, 9, 32.0/2);
select make_date(cast(1980.1 as decimal(20,5)), 9, 17);
select make_date(cast('-1980.1' as decimal(20,5)), 9, 18);
select make_date(cast(1980.1 as float32), 9, 19);
select make_date(cast(-1980.1 as float32), 9, 20);

select make_date(cast(1980 as Date), 10, 30); -- { serverError 43 }
select make_date(cast(-1980 as Date), 10, 30); -- { serverError 43 }
select make_date(cast(1980 as Date32), 10, 30); -- { serverError 43 }
select make_date(cast(-1980 as Date32), 10, 30); -- { serverError 43 }
select make_date(cast(1980 as DateTime), 10, 30); -- { serverError 43 }
select make_date(cast(-1980 as DateTime), 10, 30); -- { serverError 43 }
select make_date(cast(1980 as DateTime64), 10, 30); -- { serverError 43 }
select make_date(cast(-1980 as DateTime64), 10, 30); -- { serverError 43 }

select make_date(0.0, 1, 2);
select make_date(1980, 15, 1);
select make_date(1980, 2, 29);
select make_date(1984, 2, 30);
select make_date(19800, 12, 3);
select make_date(2148,1,1);
select make_date(2149,1,1);
select make_date(2149,6,6);
select make_date(2149,6,7);
select make_date(2150,1,1);
select make_date(1969,1,1);
select make_date(1969,12,1);
select make_date(1969,12,31);
select make_date(2282,1,1);
select make_date(2283,1,1);
select make_date(2283,11,11);
select make_date(2283,11,12);
select make_date(2284,1,1);
select make_date(1924,1,1);
select make_date(1924,12,1);
select make_date(1924,12,31);
select make_date(1970,0,0);
select make_date(1970,0,1);
select make_date(1970,1,0);
select make_date(1990,0,1);
select make_date(1990,1,0);

select make_date(0x7fff+2010,1,1);
select make_date(0xffff+2010,1,2);
select make_date(0x7fffffff+2010,1,3);
select make_date(0xffffffff+2010,1,4);
select make_date(0x7fffffffffffffff+2010,1,3);
select make_date(0xffffffffffffffff+2010,1,4);

select make_date('1980', '10', '20'); -- { serverError 43 }
select make_date('-1980', 3, 17); -- { serverError 43 }

select make_date('aa', 3, 24); -- { serverError 43 }
select make_date(1994, 'aa', 24); -- { serverError 43 }
select make_date(1984, 3, 'aa'); -- { serverError 43 }

select make_date(True, 3, 24);
select make_date(1994, True, 24);
select make_date(1984, 3, True);
select make_date(False, 3, 24);
select make_date(1994, False, 24);
select make_date(1984, 3, False);

select make_date(NULL, 3, 4);
select make_date(1980, NULL, 4);
select make_date(1980, 3, NULL);

select make_date(1980); -- { serverError 42 }
select make_date(1980, 1); -- { serverError 42 }
select make_date(1980, 1, 1, 1); -- { serverError 42 }

select make_date(year, month, day) from (select NULL as year, 2 as month, 3 as day union all select 1984 as year, 2 as month, 3 as day) order by year, month, day;

select make_date(year, month, day) from (select NULL as year, 2 as month, 3 as day union all select NULL as year, 2 as month, 3 as day) order by year, month, day;

select make_date(year, month, day) from (select 1984 as year, 2 as month, 3 as day union all select 1984 as year, 2 as month, 4 as day) order by year, month, day;
