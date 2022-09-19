DROP STREAM IF EXISTS alter_bug;

create stream alter_bug (
  epoch uint64 CODEC(Delta,LZ4),
  _time_dec float64
) Engine = MergeTree ORDER BY (epoch);


SELECT name, type, compression_codec FROM system.columns WHERE table='alter_bug' AND database=currentDatabase();

ALTER STREAM alter_bug MODIFY COLUMN epoch DEFAULT to_uint64(_time_dec) CODEC(Delta,LZ4);

SELECT name, type, default_expression, compression_codec FROM system.columns WHERE table='alter_bug' AND database=currentDatabase();

INSERT INTO alter_bug(_time_dec) VALUES(1577351080);

SELECT * FROM alter_bug;

DROP STREAM IF EXISTS alter_bug;
