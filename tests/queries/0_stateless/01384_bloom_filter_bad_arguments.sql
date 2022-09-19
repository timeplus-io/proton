DROP STREAM IF EXISTS test;

create stream test (a string, index a a type tokenbf_v1(0, 2, 0) granularity 1) engine MergeTree order by a; -- { serverError 36 }
create stream test (a string, index a a type tokenbf_v1(2, 0, 0) granularity 1) engine MergeTree order by a; -- { serverError 36 }
create stream test (a string, index a a type tokenbf_v1(0, 1, 1) granularity 1) engine MergeTree order by a; -- { serverError 36 }
create stream test (a string, index a a type tokenbf_v1(1, 0, 1) granularity 1) engine MergeTree order by a; -- { serverError 36 }

create stream test (a string, index a a type tokenbf_v1(0.1, 2, 0) granularity 1) engine MergeTree order by a; -- { serverError 36 }
create stream test (a string, index a a type tokenbf_v1(-1, 2, 0) granularity 1) engine MergeTree order by a; -- { serverError 36 }
create stream test (a string, index a a type tokenbf_v1(0xFFFFFFFF, 2, 0) granularity 1) engine MergeTree order by a; -- { serverError 36 }
