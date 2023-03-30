DROP STREAM IF EXISTS stream_a;
DROP STREAM IF EXISTS stream_b;

CREATE STREAM stream_a (
    event_id uint64,
    something string,
    other nullable(string)
) ENGINE = MergeTree ORDER BY (event_id);

CREATE STREAM stream_b (
    event_id uint64,
    something nullable(string),
    other string
) ENGINE = MergeTree ORDER BY (event_id);

INSERT INTO stream_a VALUES (1, 'foo', 'foo'), (2, 'foo', 'foo'), (3, 'bar', 'bar');
INSERT INTO stream_b VALUES (1, 'bar', 'bar'), (2, 'bar', 'bar'), (3, 'test', 'test'), (4, NULL, '');

SELECT s1.other, s2.other, count_a, count_b, to_type_name(s1.other), to_type_name(s2.other) FROM
    ( SELECT other, count() AS count_a FROM stream_a GROUP BY other ) s1
ALL FULL JOIN
    ( SELECT other, count() AS count_b FROM stream_b GROUP BY other ) s2
ON s1.other = s2.other
ORDER BY s2.other DESC, count_a, s1.other;

SELECT s1.other, s2.other, count_a, count_b, to_type_name(s1.other), to_type_name(s2.other) FROM
    ( SELECT other, count() AS count_a FROM stream_a GROUP BY other ) s1
ALL FULL JOIN
    ( SELECT other, count() AS count_b FROM stream_b GROUP BY other ) s2
USING other
ORDER BY s2.other DESC, count_a, s1.other;

SELECT s1.something, s2.something, count_a, count_b, to_type_name(s1.something), to_type_name(s2.something) FROM
    ( SELECT something, count() AS count_a FROM stream_a GROUP BY something ) s1
ALL FULL JOIN
    ( SELECT something, count() AS count_b FROM stream_b GROUP BY something ) s2
ON s1.something = s2.something
ORDER BY count_a DESC, something, s2.something;

SELECT s1.something, s2.something, count_a, count_b, to_type_name(s1.something), to_type_name(s2.something) FROM
    ( SELECT something, count() AS count_a FROM stream_a GROUP BY something ) s1
ALL RIGHT JOIN
    ( SELECT something, count() AS count_b FROM stream_b GROUP BY something ) s2
USING (something)
ORDER BY count_a DESC, s1.something, s2.something;

SET joined_subquery_requires_alias = 0;

SELECT something, count_a, count_b, to_type_name(something) FROM
    ( SELECT something, count() AS count_a FROM stream_a GROUP BY something ) as s1
ALL FULL JOIN
    ( SELECT something, count() AS count_b FROM stream_b GROUP BY something ) as s2
USING (something)
ORDER BY count_a DESC, something DESC;

DROP STREAM stream_a;
DROP STREAM stream_b;
