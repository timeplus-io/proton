DROP STREAM IF EXISTS segfault;
DROP STREAM IF EXISTS segfault_mv;

CREATE STREAM segfault
(
    id          uint32,
    uuid        uuid,
    tags_ids    array(uint32)
) ENGINE = MergeTree()
ORDER BY (id);

CREATE MATERIALIZED VIEW segfault_mv
    ENGINE = AggregatingMergeTree()
    ORDER BY (id)
AS SELECT
    id,
    uniqState(uuid) as uniq_uuids,
    uniqMapState(CAST((tags_ids, array_map(_ -> to_string(uuid), tags_ids)), 'map(uint32, string)')) as uniq_tags_ids
FROM segfault
GROUP BY id;

INSERT INTO segfault SELECT * FROM generateRandom('id uint32, uuid uuid, c array(uint32)', 10, 5, 5) LIMIT 100;
INSERT INTO segfault SELECT * FROM generateRandom('id uint32, uuid uuid, c array(uint32)', 10, 5, 5) LIMIT 100;
INSERT INTO segfault SELECT * FROM generateRandom('id uint32, uuid uuid, c array(uint32)', 10, 5, 5) LIMIT 100;

SELECT ignore(CAST((array_map(k -> to_string(k), mapKeys(uniqMapMerge(uniq_tags_ids) AS m)), mapValues(m)), 'map(string, uint32)')) FROM segfault_mv;
