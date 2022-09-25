SELECT CAST(array_join(['', 'abc', '123', '123a', '-123']) AS nullable(uint8));
SELECT CAST(array_join(['', '2018', '2018-01-02', '2018-1-2', '2018-01-2', '2018-1-02', '2018-ab-cd', '2018-01-02a']) AS nullable(date));
SELECT CAST(array_join(['', '2018', '2018-01-02 01:02:03', '2018-01-02T01:02:03', '2018-01-02 01:02:03 abc']) AS nullable(datetime));
SELECT CAST(array_join(['', 'abc', '123', '123a', '-123']) AS nullable(string));

SELECT toDateOrZero(array_join(['', '2018', '2018-01-02', '2018-1-2', '2018-01-2', '2018-1-02', '2018-ab-cd', '2018-01-02a']));
SELECT toDateOrNull(array_join(['', '2018', '2018-01-02', '2018-1-2', '2018-01-2', '2018-1-02', '2018-ab-cd', '2018-01-02a']));

SELECT toDateTimeOrZero(array_join(['', '2018', '2018-01-02 01:02:03', '2018-01-02T01:02:03', '2018-01-02 01:02:03 abc']), 'UTC');
SELECT toDateTimeOrNull(array_join(['', '2018', '2018-01-02 01:02:03', '2018-01-02T01:02:03', '2018-01-02 01:02:03 abc']));

