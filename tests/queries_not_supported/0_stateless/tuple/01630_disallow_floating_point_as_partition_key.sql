DROP STREAM IF EXISTS test;
create stream test (a Float32, b int) Engine = MergeTree() ORDER BY tuple() PARTITION BY a; -- { serverError 36 }
create stream test (a Float32, b int) Engine = MergeTree() ORDER BY tuple() PARTITION BY a settings allow_floating_point_partition_key=true;
DROP STREAM IF EXISTS test;
create stream test (a Float32, b int, c string, d float64) Engine = MergeTree() ORDER BY tuple() PARTITION BY (b, c, d) settings allow_floating_point_partition_key=false; -- { serverError 36 }
create stream test (a Float32, b int, c string, d float64) Engine = MergeTree() ORDER BY tuple() PARTITION BY (b, c, d) settings allow_floating_point_partition_key=true;
DROP STREAM IF EXISTS test;
