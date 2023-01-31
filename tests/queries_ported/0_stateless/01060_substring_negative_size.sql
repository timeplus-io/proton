select substring('abcdefgh', 2, -2);
select substring('abcdefgh', materialize(2), -2);
select substring('abcdefgh', 2, materialize(-2));
select substring('abcdefgh', materialize(2), materialize(-2));

select '-';

select substring(cast('abcdefgh' as fixed_string(8)), 2, -2);
select substring(cast('abcdefgh' as fixed_string(8)), materialize(2), -2);
select substring(cast('abcdefgh' as fixed_string(8)), 2, materialize(-2));
select substring(cast('abcdefgh' as fixed_string(8)), materialize(2), materialize(-2));

select '-';

drop stream if exists t;
create stream t (s string, l int8, r int8) engine = Memory;
insert into t values ('abcdefgh', 2, -2), ('12345678', 3, -3);

select substring(s, 2, -2) from t;
select substring(s, l, -2) from t;
select substring(s, 2, r) from t;
select substring(s, l, r) from t;

select '-';

drop stream if exists t;
create stream t (s fixed_string(8), l int8, r int8) engine = Memory;
insert into t values ('abcdefgh', 2, -2), ('12345678', 3, -3);

select substring(s, 2, -2) from t;
select substring(s, l, -2) from t;
select substring(s, 2, r) from t;
select substring(s, l, r) from t;

drop stream if exists t;

