-- the test from simPod, https://github.com/ClickHouse/ClickHouse/issues/5608

DROP STREAM IF EXISTS joint; -- the table name from the original issue.
DROP STREAM IF EXISTS t;

create stream IF NOT EXISTS joint
(
    id    uuid,
    value low_cardinality(string)
)
ENGINE = Join (ANY, LEFT, id);

create stream IF NOT EXISTS t
(
    id    uuid,
    d     datetime
)
ENGINE = MergeTree
PARTITION BY to_date(d)
ORDER BY id;

insert into joint VALUES ('00000000-0000-0000-0000-000000000000', 'yo');
insert into t VALUES ('00000000-0000-0000-0000-000000000000', now());

SELECT id FROM t
ANY LEFT JOIN joint ON t.id = joint.id;

DROP STREAM joint;
DROP STREAM t;
