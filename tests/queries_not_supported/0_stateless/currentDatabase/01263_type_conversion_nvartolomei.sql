DROP STREAM IF EXISTS m;
DROP STREAM IF EXISTS d;

create stream m
(
    `v` uint8
)
ENGINE = MergeTree()
PARTITION BY tuple()
ORDER BY v;

create stream d
(
    `v` UInt16
)
ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), m, rand());

INSERT INTO m VALUES (123);
SELECT * FROM d;


DROP STREAM m;
DROP STREAM d;


create stream m
(
    `v` Enum8('a' = 1, 'b' = 2)
)
ENGINE = MergeTree()
PARTITION BY tuple()
ORDER BY v;

create stream d
(
    `v` Enum8('a' = 1)
)
ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), m, rand());

INSERT INTO m VALUES ('a');
SELECT * FROM d;

SELECT '---';

INSERT INTO m VALUES ('b');
SELECT to_string(v) FROM (SELECT v FROM d ORDER BY v) FORMAT Null; -- { serverError 36 }


DROP STREAM m;
DROP STREAM d;
