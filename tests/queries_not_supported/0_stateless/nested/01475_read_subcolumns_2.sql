DROP STREAM IF EXISTS subcolumns;

create stream subcolumns
(
    t tuple
    (
        a array(Nullable(uint32)),
        u uint32,
        s Nullable(string)
    ),
    arr array(Nullable(string)),
    arr2 array(array(Nullable(string))),
    lc LowCardinality(string),
    nested nested(col1 string, col2 Nullable(uint32))
)
ENGINE = MergeTree order by tuple() SETTINGS min_bytes_for_wide_part = '10M';

INSERT INTO subcolumns VALUES (([1, NULL], 2, 'a'), ['foo', NULL, 'bar'], [['123'], ['456', '789']], 'qqqq', ['zzz', 'xxx'], [42, 43]);
SELECT * FROM subcolumns;
SELECT t.a, t.u, t.s, nested.col1, nested.col2, lc FROM subcolumns;
SELECT t.a.size0, t.a.null, t.u, t.s, t.s.null FROM subcolumns;
SELECT sumArray(arr.null), sum(arr.size0) FROM subcolumns;
SELECT arr2, arr2.size0, arr2.size1, arr2.null FROM subcolumns;
-- SELECT nested.col1, nested.col2, nested.col1.size0, nested.col2.size0, nested.col2.null FROM subcolumns;

DROP STREAM IF EXISTS subcolumns;

create stream subcolumns
(
    t tuple
    (
        a array(Nullable(uint32)),
        u uint32,
        s Nullable(string)
    ),
    arr array(Nullable(string)),
    arr2 array(array(Nullable(string))),
    lc LowCardinality(string),
    nested nested(col1 string, col2 Nullable(uint32))
)
ENGINE = MergeTree order by tuple() SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO subcolumns VALUES (([1, NULL], 2, 'a'), ['foo', NULL, 'bar'], [['123'], ['456', '789']], 'qqqq', ['zzz', 'xxx'], [42, 43]);
SELECT * FROM subcolumns;
SELECT t.a, t.u, t.s, nested.col1, nested.col2, lc FROM subcolumns;
SELECT t.a.size0, t.a.null, t.u, t.s, t.s.null FROM subcolumns;
SELECT sumArray(arr.null), sum(arr.size0) FROM subcolumns;
SELECT arr2, arr2.size0, arr2.size1, arr2.null FROM subcolumns;
-- SELECT nested.col1, nested.col2, nested.size0, nested.size0, nested.col2.null FROM subcolumns;

DROP STREAM subcolumns;
