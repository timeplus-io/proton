SET query_mode = 'table';
drop stream if exists perf_lc_num;

create stream perf_lc_num(　        num uint8,　        arr array(low_cardinality(int64)) default [num]　        ) ;

INSERT INTO perf_lc_num (num) SELECT to_uint8(number) FROM numbers(10000000);

select sum(length(arr)) from perf_lc_num;
select sum(length(arr)), sum(num) from perf_lc_num;

INSERT INTO perf_lc_num (num) SELECT to_uint8(number) FROM numbers(10000000);

select sum(length(arr)) from perf_lc_num;
select sum(length(arr)), sum(num) from perf_lc_num;

drop stream if exists perf_lc_num;


create stream perf_lc_num(　        num uint8,　        arr array(low_cardinality(int64)) default [num]　        )  ;

INSERT INTO perf_lc_num (num) SELECT to_uint8(number) FROM numbers(10000000);

select sum(length(arr)) from perf_lc_num;
select sum(length(arr)), sum(num) from perf_lc_num;

INSERT INTO perf_lc_num (num) SELECT to_uint8(number) FROM numbers(10000000);

select sum(length(arr)) from perf_lc_num;
select sum(length(arr)), sum(num) from perf_lc_num;

drop stream if exists perf_lc_num;


create stream perf_lc_num(　        num uint8,　        arr array(low_cardinality(int64)) default [num]　        ) ENGINE = StripeLog;

INSERT INTO perf_lc_num (num) SELECT to_uint8(number) FROM numbers(10000000);

select sum(length(arr)) from perf_lc_num;
select sum(length(arr)), sum(num) from perf_lc_num;

INSERT INTO perf_lc_num (num) SELECT to_uint8(number) FROM numbers(10000000);

select sum(length(arr)) from perf_lc_num;
select sum(length(arr)), sum(num) from perf_lc_num;

drop stream if exists perf_lc_num;


