DROP STREAM IF EXISTS tt_error_1373;

create stream tt_error_1373
( a   int64, d   int64, val int64 ) 
ENGINE = SummingMergeTree((a, val)) PARTITION BY (a) ORDER BY (d); -- { serverError 36 }

DROP STREAM IF EXISTS tt_error_1373;