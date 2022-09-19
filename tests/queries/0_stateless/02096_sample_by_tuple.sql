DROP STREAM IF EXISTS t;

create stream t (n uint8) ENGINE=MergeTree ORDER BY n SAMPLE BY tuple(); -- { serverError 80 }

create stream t (n uint8) ENGINE=MergeTree ORDER BY tuple();

ALTER STREAM t MODIFY SAMPLE BY tuple(); -- { serverError 80 }
