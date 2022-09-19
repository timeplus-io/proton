-- Tags: no-parallel

create stream ttl_01280_error (a int, b int, x int64, y int64, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by x set y = max(y); -- { serverError 450}
create stream ttl_01280_error (a int, b int, x int64, y int64, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by b set y = max(y); -- { serverError 450}
create stream ttl_01280_error (a int, b int, x int64, y int64, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by a, b, x set y = max(y); -- { serverError 450}
create stream ttl_01280_error (a int, b int, x int64, y int64, d DateTime) engine = MergeTree order by (a, b) ttl d + interval 1 second group by a, b set y = max(y), y = max(y); -- { serverError 450}
