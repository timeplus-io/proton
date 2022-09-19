DROP STREAM IF EXISTS test_00563;

create stream test_00563 ( dt date, site_id int32, site_key string ) ENGINE = MergeTree(dt, (site_id, site_key, dt), 8192);
INSERT INTO test_00563 (dt,site_id, site_key) VALUES ('2018-1-29', 100, 'key');
SELECT * FROM test_00563 WHERE to_int32(site_id) IN (100);
SELECT * FROM test_00563 WHERE to_int32(site_id) IN (100,101);

DROP STREAM IF EXISTS test_00563;

DROP STREAM IF EXISTS join_with_index;
create stream join_with_index (key uint32, data uint64) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity=1;
INSERT INTO join_with_index VALUES (1, 0), (2, 99);

SELECT key + 1
FROM join_with_index
ALL INNER JOIN
(
    SELECT
        key,
        data
    FROM join_with_index
    WHERE to_uint64(data) IN (0, 529335254087962442)
) js2 USING (key);

SELECT _uniq, _uniq IN (0, 99)
FROM join_with_index
ARRAY JOIN
    [key, data] AS _uniq
ORDER BY _uniq;

DROP STREAM IF EXISTS join_with_index;
