-- Tags: distributed

DROP STREAM IF EXISTS t_subcolumns_local;
DROP STREAM IF EXISTS t_subcolumns_dist;

create stream t_subcolumns_local (arr array(uint32), n Nullable(string), t tuple(s1 string, s2 string))
ENGINE = MergeTree ORDER BY tuple();

create stream t_subcolumns_dist AS t_subcolumns_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_subcolumns_local);

INSERT INTO t_subcolumns_local VALUES ([1, 2, 3], 'aaa', ('bbb', 'ccc'));

SELECT arr.size0, n.null, t.s1, t.s2 FROM t_subcolumns_dist;

DROP STREAM t_subcolumns_local;

-- StripeLog doesn't support subcolumns.
create stream t_subcolumns_local (arr array(uint32), n Nullable(string), t tuple(s1 string, s2 string)) ENGINE = StripeLog;

SELECT arr.size0, n.null, t.s1, t.s2 FROM t_subcolumns_dist; -- { serverError 47 }

DROP STREAM t_subcolumns_local;
DROP STREAM t_subcolumns_dist;
