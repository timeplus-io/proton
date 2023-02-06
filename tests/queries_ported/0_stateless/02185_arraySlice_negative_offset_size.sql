select array_slice([1, 2, 3, 4, 5, 6, 7, 8], -2, -2);
select array_slice(materialize([1, 2, 3, 4, 5, 6, 7, 8]), -2, -2);
select array_slice(materialize([1, 2, 3, 4, 5, 6, 7, 8]), materialize(-2), materialize(-2));

select array_slice([1, 2, 3, 4, 5, 6, 7, 8], -2, -1);
select array_slice(materialize([1, 2, 3, 4, 5, 6, 7, 8]), -2, -1);
select array_slice(materialize([1, 2, 3, 4, 5, 6, 7, 8]), materialize(-2), materialize(-1));

select '-';
drop stream if exists t;
create stream t
(
    s array(int),
    l int8,
    r int8
) engine = Memory;

insert into t values ([1, 2, 3, 4, 5, 6, 7, 8], -2, -2), ([1, 2, 3, 4, 5, 6, 7, 8], -3, -3);

select array_slice(s, -2, -2) from t;
select array_slice(s, l, -2) from t;
select array_slice(s, -2, r) from t;
select array_slice(s, l, r) from t;

drop stream t;
