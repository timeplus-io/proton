DROP STREAM IF EXISTS testNullableStates;
DROP STREAM IF EXISTS testNullableStatesAgg;

create stream testNullableStates (
   ts DateTime,
   id string,
   string Nullable(string),
   float64 Nullable(float64),
   float32 Nullable(Float32),
   decimal325 Nullable(Decimal32(5)),
   date Nullable(date),
   datetime Nullable(DateTime),
   datetime64 Nullable(DateTime64),
   int64 Nullable(int64),
   int32 Nullable(int32),
   int16 Nullable(Int16),
   int8 Nullable(int8))
ENGINE=MergeTree PARTITION BY to_start_of_day(ts) ORDER BY id;

INSERT INTO testNullableStates SELECT
    to_datetime('2020-01-01 00:00:00') + number AS ts,
    to_string(number % 999) AS id,
    to_string(number) AS string,
    number / 333 AS float64,
    number / 333 AS float32,
    number / 333 AS decimal325,
    to_date(ts),
    ts,
    ts,
    number,
    to_int32(number),
    to_int16(number),
    to_int8(number)
FROM numbers(100000);

INSERT INTO testNullableStates SELECT
    to_datetime('2020-01-01 00:00:00') + number AS ts,
    to_string(number % 999 - 5) AS id,
    NULL AS string,
    NULL AS float64,
    NULL AS float32,
    NULL AS decimal325,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL
FROM numbers(500);


create stream testNullableStatesAgg
(
    `ts` DateTime,
    `id` string,
    `stringMin` aggregate_function(min, Nullable(string)),
    `stringMax` aggregate_function(max, Nullable(string)),
    `float64Min` aggregate_function(min, Nullable(float64)),
    `float64Max` aggregate_function(max, Nullable(float64)),
    `float64Avg` aggregate_function(avg, Nullable(float64)),
    `float64Sum` aggregate_function(sum, Nullable(float64)),
    `float32Min` aggregate_function(min, Nullable(Float32)),
    `float32Max` aggregate_function(max, Nullable(Float32)),
    `float32Avg` aggregate_function(avg, Nullable(Float32)),
    `float32Sum` aggregate_function(sum, Nullable(Float32)),
    `decimal325Min` aggregate_function(min, Nullable(Decimal32(5))),
    `decimal325Max` aggregate_function(max, Nullable(Decimal32(5))),
    `decimal325Avg` aggregate_function(avg, Nullable(Decimal32(5))),
    `decimal325Sum` aggregate_function(sum, Nullable(Decimal32(5))),
    `dateMin` aggregate_function(min, Nullable(date)),
    `dateMax` aggregate_function(max, Nullable(date)),
    `datetimeMin` aggregate_function(min, Nullable(DateTime)),
    `datetimeMax` aggregate_function(max, Nullable(DateTime)),
    `datetime64Min` aggregate_function(min, Nullable(datetime64)),
    `datetime64Max` aggregate_function(max, Nullable(datetime64)),
    `int64Min` aggregate_function(min, Nullable(int64)),
    `int64Max` aggregate_function(max, Nullable(int64)),
    `int64Avg` aggregate_function(avg, Nullable(int64)),
    `int64Sum` aggregate_function(sum, Nullable(int64)),
    `int32Min` aggregate_function(min, Nullable(int32)),
    `int32Max` aggregate_function(max, Nullable(int32)),
    `int32Avg` aggregate_function(avg, Nullable(int32)),
    `int32Sum` aggregate_function(sum, Nullable(int32)),
    `int16Min` aggregate_function(min, Nullable(Int16)),
    `int16Max` aggregate_function(max, Nullable(Int16)),
    `int16Avg` aggregate_function(avg, Nullable(Int16)),
    `int16Sum` aggregate_function(sum, Nullable(Int16)),
    `int8Min` aggregate_function(min, Nullable(int8)),
    `int8Max` aggregate_function(max, Nullable(int8)),
    `int8Avg` aggregate_function(avg, Nullable(int8)),
    `int8Sum` aggregate_function(sum, Nullable(int8))
)
ENGINE = AggregatingMergeTree()
PARTITION BY to_start_of_day(ts)
ORDER BY id;




insert into testNullableStatesAgg
select
   ts DateTime,
   id string,
   minState(string) stringMin,
   maxState(string) stringMax,
   minState(float64) float64Min,
   maxState(float64) float64Max,
   avgState(float64) float64Avg,
   sumState(float64) float64Sum,
   minState(float32) float32Min,
   maxState(float32) float32Max,
   avgState(float32) float32Avg,
   sumState(float32) float32Sum,
   minState(decimal325) decimal325Min,
   maxState(decimal325) decimal325Max,
   avgState(decimal325) decimal325Avg,
   sumState(decimal325) decimal325Sum,
   minState(date) dateMin,
   maxState(date) dateMax,
   minState(datetime) datetimeMin,
   maxState(datetime) datetimeMax,
   minState(datetime64) datetime64Min,
   maxState(datetime64) datetime64Max,
   minState(int64) int64Min,
   maxState(int64) int64Max,
   avgState(int64) int64Avg,
   sumState(int64) int64Sum,
   minState(int32) int32Min,
   maxState(int32) int32Max,
   avgState(int32) int32Avg,
   sumState(int32) int32Sum,
   minState(int16) int16Min,
   maxState(int16) int16Max,
   avgState(int16) int16Avg,
   sumState(int16) int16Sum,
   minState(int8) int8Min,
   maxState(int8) int8Max,
   avgState(int8) int8Avg,
   sumState(int8) int8Sum
from testNullableStates
group by ts, id;

OPTIMIZE STREAM testNullableStatesAgg FINAL;

select count() from testNullableStates;

select count() from testNullableStatesAgg;

select ' ---- select without states ---- ';

SELECT id, count(),
    min(string),
    max(string),
    floor(min(float64),5),
    floor(max(float64),5),
    floor(avg(float64),5),
    floor(sum(float64),5),
    floor(min(float32),5),
    floor(max(float32),5),
    floor(avg(float32),5),
    floor(sum(float32),5),
    min(decimal325),
    max(decimal325),
    avg(decimal325),
    sum(decimal325),
    min(date),
    max(date),
    min(datetime),
    max(datetime),
    min(datetime64),
    max(datetime64),
    min(int64),
    max(int64),
    avg(int64),
    sum(int64),
    min(int32),
    max(int32),
    avg(int32),
    sum(int32),
    min(int16),
    max(int16),
    avg(int16),
    sum(int16),
    min(int8),
    max(int8),
    avg(int8),
    sum(int8)
FROM testNullableStates
GROUP BY id
ORDER BY id ASC;

select ' ---- select with states ---- ';

SELECT id, count(),
    minMerge(stringMin),
    maxMerge(stringMax),
    floor(minMerge(float64Min),5),
    floor(maxMerge(float64Max),5),
    floor(avgMerge(float64Avg),5),
    floor(sumMerge(float64Sum),5),
    floor(minMerge(float32Min),5),
    floor(maxMerge(float32Max),5),
    floor(avgMerge(float32Avg),5),
    floor(sumMerge(float32Sum),5),
    minMerge(decimal325Min),
    maxMerge(decimal325Max),
    avgMerge(decimal325Avg),
    sumMerge(decimal325Sum),
    minMerge(dateMin),
    maxMerge(dateMax),
    minMerge(datetimeMin),
    maxMerge(datetimeMax),
    minMerge(datetime64Min),
    maxMerge(datetime64Max),
    minMerge(int64Min),
    maxMerge(int64Max),
    avgMerge(int64Avg),
    sumMerge(int64Sum),
    minMerge(int32Min),
    maxMerge(int32Max),
    avgMerge(int32Avg),
    sumMerge(int32Sum),
    minMerge(int16Min),
    maxMerge(int16Max),
    avgMerge(int16Avg),
    sumMerge(int16Sum),
    minMerge(int8Min),
    maxMerge(int8Max),
    avgMerge(int8Avg),
    sumMerge(int8Sum)
FROM testNullableStatesAgg
GROUP BY id
ORDER BY id ASC;


select ' ---- select row with nulls without states ---- ';

SELECT id, count(),
    min(string),
    max(string),
    floor(min(float64),5),
    floor(max(float64),5),
    floor(avg(float64),5),
    floor(sum(float64),5),
    floor(min(float32),5),
    floor(max(float32),5),
    floor(avg(float32),5),
    floor(sum(float32),5),
    min(decimal325),
    max(decimal325),
    avg(decimal325),
    sum(decimal325),
    min(date),
    max(date),
    min(datetime),
    max(datetime),
    min(datetime64),
    max(datetime64),
    min(int64),
    max(int64),
    avg(int64),
    sum(int64),
    min(int32),
    max(int32),
    avg(int32),
    sum(int32),
    min(int16),
    max(int16),
    avg(int16),
    sum(int16),
    min(int8),
    max(int8),
    avg(int8),
    sum(int8)
FROM testNullableStates
WHERE id = '-2'
GROUP BY id
ORDER BY id ASC;

select ' ---- select row with nulls with states ---- ';

SELECT id, count(),
    minMerge(stringMin),
    maxMerge(stringMax),
    floor(minMerge(float64Min),5),
    floor(maxMerge(float64Max),5),
    floor(avgMerge(float64Avg),5),
    floor(sumMerge(float64Sum),5),
    floor(minMerge(float32Min),5),
    floor(maxMerge(float32Max),5),
    floor(avgMerge(float32Avg),5),
    floor(sumMerge(float32Sum),5),
    minMerge(decimal325Min),
    maxMerge(decimal325Max),
    avgMerge(decimal325Avg),
    sumMerge(decimal325Sum),
    minMerge(dateMin),
    maxMerge(dateMax),
    minMerge(datetimeMin),
    maxMerge(datetimeMax),
    minMerge(datetime64Min),
    maxMerge(datetime64Max),
    minMerge(int64Min),
    maxMerge(int64Max),
    avgMerge(int64Avg),
    sumMerge(int64Sum),
    minMerge(int32Min),
    maxMerge(int32Max),
    avgMerge(int32Avg),
    sumMerge(int32Sum),
    minMerge(int16Min),
    maxMerge(int16Max),
    avgMerge(int16Avg),
    sumMerge(int16Sum),
    minMerge(int8Min),
    maxMerge(int8Max),
    avgMerge(int8Avg),
    sumMerge(int8Sum)
FROM testNullableStatesAgg
WHERE id = '-2'
GROUP BY id
ORDER BY id ASC;


select ' ---- select no rows without states ---- ';

SELECT count(),
    min(string),
    max(string),
    floor(min(float64),5),
    floor(max(float64),5),
    floor(avg(float64),5),
    floor(sum(float64),5),
    floor(min(float32),5),
    floor(max(float32),5),
    floor(avg(float32),5),
    floor(sum(float32),5),
    min(decimal325),
    max(decimal325),
    avg(decimal325),
    sum(decimal325),
    min(date),
    max(date),
    min(datetime),
    max(datetime),
    min(datetime64),
    max(datetime64),
    min(int64),
    max(int64),
    avg(int64),
    sum(int64),
    min(int32),
    max(int32),
    avg(int32),
    sum(int32),
    min(int16),
    max(int16),
    avg(int16),
    sum(int16),
    min(int8),
    max(int8),
    avg(int8),
    sum(int8)
FROM testNullableStates
WHERE id = '-22';

select ' ---- select no rows with states ---- ';

SELECT count(),
    minMerge(stringMin),
    maxMerge(stringMax),
    floor(minMerge(float64Min),5),
    floor(maxMerge(float64Max),5),
    floor(avgMerge(float64Avg),5),
    floor(sumMerge(float64Sum),5),
    floor(minMerge(float32Min),5),
    floor(maxMerge(float32Max),5),
    floor(avgMerge(float32Avg),5),
    floor(sumMerge(float32Sum),5),
    minMerge(decimal325Min),
    maxMerge(decimal325Max),
    avgMerge(decimal325Avg),
    sumMerge(decimal325Sum),
    minMerge(dateMin),
    maxMerge(dateMax),
    minMerge(datetimeMin),
    maxMerge(datetimeMax),
    minMerge(datetime64Min),
    maxMerge(datetime64Max),
    minMerge(int64Min),
    maxMerge(int64Max),
    avgMerge(int64Avg),
    sumMerge(int64Sum),
    minMerge(int32Min),
    maxMerge(int32Max),
    avgMerge(int32Avg),
    sumMerge(int32Sum),
    minMerge(int16Min),
    maxMerge(int16Max),
    avgMerge(int16Avg),
    sumMerge(int16Sum),
    minMerge(int8Min),
    maxMerge(int8Max),
    avgMerge(int8Avg),
    sumMerge(int8Sum)
FROM testNullableStatesAgg
WHERE id = '-22';

DROP STREAM testNullableStates;
DROP STREAM testNullableStatesAgg;
