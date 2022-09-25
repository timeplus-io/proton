-- Tags: zookeeper

DROP STREAM IF EXISTS merge_tree_pk;

create stream merge_tree_pk
(
    key uint64,
    value string
)
ENGINE = ReplacingMergeTree()
PRIMARY KEY key;

SHOW create stream merge_tree_pk;

INSERT INTO merge_tree_pk VALUES (1, 'a');
INSERT INTO merge_tree_pk VALUES (2, 'b');

SELECT * FROM merge_tree_pk ORDER BY key;

INSERT INTO merge_tree_pk VALUES (1, 'c');

DETACH STREAM merge_tree_pk;
ATTACH STREAM merge_tree_pk;

SELECT * FROM merge_tree_pk FINAL ORDER BY key;

DROP STREAM IF EXISTS merge_tree_pk;

DROP STREAM IF EXISTS merge_tree_pk_sql;

create stream merge_tree_pk_sql
(
    key uint64,
    value string,
    PRIMARY KEY (key)
)
ENGINE = ReplacingMergeTree();

SHOW create stream merge_tree_pk_sql;

INSERT INTO merge_tree_pk_sql VALUES (1, 'a');
INSERT INTO merge_tree_pk_sql VALUES (2, 'b');

SELECT * FROM merge_tree_pk_sql ORDER BY key;

INSERT INTO merge_tree_pk_sql VALUES (1, 'c');

DETACH STREAM merge_tree_pk_sql;
ATTACH STREAM merge_tree_pk_sql;

SELECT * FROM merge_tree_pk_sql FINAL ORDER BY key;

ALTER STREAM merge_tree_pk_sql ADD COLUMN key2 uint64, MODIFY ORDER BY (key, key2);

INSERT INTO merge_tree_pk_sql VALUES (2, 'd', 555);

INSERT INTO merge_tree_pk_sql VALUES (2, 'e', 555);

SELECT * FROM merge_tree_pk_sql FINAL ORDER BY key;

SHOW create stream merge_tree_pk_sql;

DROP STREAM IF EXISTS merge_tree_pk_sql;

DROP STREAM IF EXISTS replicated_merge_tree_pk_sql;

create stream replicated_merge_tree_pk_sql
(
    key uint64,
    value string,
    PRIMARY KEY (key)
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/test/01532_primary_key_without', 'r1');

SHOW create stream replicated_merge_tree_pk_sql;

INSERT INTO replicated_merge_tree_pk_sql VALUES (1, 'a');
INSERT INTO replicated_merge_tree_pk_sql VALUES (2, 'b');

SELECT * FROM replicated_merge_tree_pk_sql ORDER BY key;

INSERT INTO replicated_merge_tree_pk_sql VALUES (1, 'c');

DETACH STREAM replicated_merge_tree_pk_sql;
ATTACH STREAM replicated_merge_tree_pk_sql;

SELECT * FROM replicated_merge_tree_pk_sql FINAL ORDER BY key;

ALTER STREAM replicated_merge_tree_pk_sql ADD COLUMN key2 uint64, MODIFY ORDER BY (key, key2);

INSERT INTO replicated_merge_tree_pk_sql VALUES (2, 'd', 555);

INSERT INTO replicated_merge_tree_pk_sql VALUES (2, 'e', 555);

SELECT * FROM replicated_merge_tree_pk_sql FINAL ORDER BY key;

DETACH STREAM replicated_merge_tree_pk_sql;
ATTACH STREAM replicated_merge_tree_pk_sql;

SHOW create stream replicated_merge_tree_pk_sql;

DROP STREAM IF EXISTS replicated_merge_tree_pk_sql;
