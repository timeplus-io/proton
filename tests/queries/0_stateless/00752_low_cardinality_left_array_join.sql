SET query_mode = 'table';
drop stream if exists lc_left_aj;
create stream lc_left_aj
(
    str array(low_cardinality(string)), 
    null_str array(low_cardinality(nullable(string))), 
    val array(low_cardinality(float64)), 
    null_val array(low_cardinality(nullable(float64)))
)
;

insert into lc_left_aj values (['a', 'b'], ['c', Null], [1, 2.0], [3., Null]), ([], ['c', Null], [1, 2.0], [3., Null]), (['a', 'b'], [], [1, 2.0], [3., Null]), (['a', 'b'], ['c', Null], [], [3., Null]), (['a', 'b'], ['c', Null], [1, 2.0], []);

select *, arr from lc_left_aj left array join str as arr;
select '-';
select *, arr from lc_left_aj left array join null_str as arr;
select '-';
select *, arr from lc_left_aj left array join val as arr;
select '-';
select *, arr from lc_left_aj left array join null_val as arr;
drop stream if exists lc_left_aj;

