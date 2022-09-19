DROP STREAM IF EXISTS signed_table;

create stream signed_table (
    k uint32,
    v string,
    s int8
) ENGINE CollapsingMergeTree(s) ORDER BY k;

INSERT INTO signed_table(k, v, s) VALUES (1, 'a', 1);

ALTER STREAM signed_table DROP COLUMN s; --{serverError 524}
ALTER STREAM signed_table RENAME COLUMN s TO s1; --{serverError 524}

DROP STREAM IF EXISTS signed_table;
