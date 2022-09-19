DROP STREAM IF EXISTS src_01019;
DROP STREAM IF EXISTS dest_01019;
DROP STREAM IF EXISTS pipe_01019;

create stream src_01019(v uint64) ENGINE = Null;
create stream dest_01019(v uint64) Engine = MergeTree() ORDER BY v;

CREATE MATERIALIZED VIEW pipe_01019 TO dest_01019 AS
SELECT v FROM src_01019;

INSERT INTO src_01019 VALUES (1), (2), (3);

SET allow_experimental_alter_materialized_view_structure = 1;

-- Live alter which changes query logic and adds an extra column.
ALTER STREAM pipe_01019
    MODIFY QUERY
    SELECT
        v * 2 as v,
        1 as v2
    FROM src_01019;

INSERT INTO src_01019 VALUES (1), (2), (3);

SELECT * FROM dest_01019 ORDER BY v;

ALTER STREAM dest_01019
    ADD COLUMN v2 uint64;

INSERT INTO src_01019 VALUES (42);
SELECT * FROM dest_01019 ORDER BY v;

DROP STREAM src_01019;
DROP STREAM dest_01019;
DROP STREAM pipe_01019;
