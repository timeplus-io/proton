-- Tags: no-parallel

DROP STREAM IF EXISTS test;
create stream test (x uint8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test (x) VALUES (1), (2), (3);
ALTER STREAM test CLEAR COLUMN x; --{serverError 36}
DROP STREAM test;

DROP STREAM IF EXISTS test;

create stream test (x uint8, y uint8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test (x, y) VALUES (1, 1), (2, 2), (3, 3);

ALTER STREAM test CLEAR COLUMN x;

ALTER STREAM test CLEAR COLUMN x IN PARTITION ''; --{serverError 248}
ALTER STREAM test CLEAR COLUMN x IN PARTITION 'asdasd'; --{serverError 248}
ALTER STREAM test CLEAR COLUMN x IN PARTITION '123'; --{serverError 248}

ALTER STREAM test CLEAR COLUMN y; --{serverError 36}

ALTER STREAM test ADD COLUMN z string DEFAULT 'Hello';

-- y is only real column in table
ALTER STREAM test CLEAR COLUMN y; --{serverError 36}
ALTER STREAM test CLEAR COLUMN x;
ALTER STREAM test CLEAR COLUMN z;

INSERT INTO test (x, y, z) VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 3, 'c');

ALTER STREAM test CLEAR COLUMN z;
ALTER STREAM test CLEAR COLUMN x;

SELECT * FROM test ORDER BY y;

DROP STREAM IF EXISTS test;
