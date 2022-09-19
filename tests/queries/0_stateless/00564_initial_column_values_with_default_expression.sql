DROP STREAM IF EXISTS test;

create stream IF NOT EXISTS test( id uint32, track uint8, codec string, content string, rdate date DEFAULT '2018-02-03', track_id string DEFAULT concat(concat(concat(to_string(track), '-'), codec), content) ) ENGINE=MergeTree(rdate, (id, track_id), 8192);

INSERT INTO test(id, track, codec) VALUES(1, 0, 'h264');

SELECT * FROM test ORDER BY id;

INSERT INTO test(id, track, codec, content) VALUES(2, 0, 'h264', 'CONTENT');

SELECT * FROM test ORDER BY id;

DROP STREAM IF EXISTS test;
