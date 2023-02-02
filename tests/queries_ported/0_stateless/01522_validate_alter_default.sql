DROP STREAM IF EXISTS stream2;
CREATE STREAM stream2
(
        EventDate Date,
        Id int32,
        Value int32
)
Engine = MergeTree()
PARTITION BY to_YYYYMM(EventDate)
ORDER BY Id;

ALTER STREAM stream2 MODIFY COLUMN `Value` DEFAULT 'some_string'; --{serverError 6}

ALTER STREAM stream2 ADD COLUMN `Value2` DEFAULT 'some_string'; --{serverError 36}

DROP STREAM IF EXISTS stream2;
