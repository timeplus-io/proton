DROP STREAM IF EXISTS table2;
create stream table2
(
        EventDate date,
        Id int32,
        Value int32
)
Engine = MergeTree()
PARTITION BY to_YYYYMM(EventDate)
ORDER BY Id;

ALTER STREAM table2 MODIFY COLUMN `Value` DEFAULT 'some_string'; --{serverError 6}

ALTER STREAM table2 ADD COLUMN `Value2` DEFAULT 'some_string'; --{serverError 36}

DROP STREAM IF EXISTS table2;
