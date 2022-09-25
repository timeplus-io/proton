-- Tags: no-fasttest

SET allow_experimental_object_type = 1;

DROP STREAM IF EXISTS t_json_local;
DROP STREAM IF EXISTS t_json_dist;

create stream t_json_local(data JSON) ENGINE = MergeTree ORDER BY tuple();
create stream t_json_dist AS t_json_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_json_local);

INSERT INTO t_json_local FORMAT JSONAsObject {"k1": 2, "k2": {"k3": "qqq", "k4": [44, 55]}}
;

SELECT data, to_type_name(data) FROM t_json_dist;
SELECT data.k1, data.k2.k3, data.k2.k4 FROM t_json_dist;

DROP STREAM IF EXISTS t_json_local;
DROP STREAM IF EXISTS t_json_dist;
