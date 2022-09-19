-- Tags: no-parallel

-- Create dictionary, since dictGet*() uses DB::Context in executeImpl()
-- (To cover scope of the Context in DB::PushingToViewsBlockOutputStream::process)

set insert_distributed_sync=1;

DROP STREAM IF EXISTS mv;
DROP DATABASE IF EXISTS dict_in_01023;
CREATE DATABASE dict_in_01023;

create stream dict_in_01023.input (key uint64, val uint64) Engine=Memory();

CREATE DICTIONARY dict_in_01023.dict
(
  key uint64 DEFAULT 0,
  val uint64 DEFAULT 1
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'input' PASSWORD '' DB 'dict_in_01023'))
LIFETIME(MIN 0 MAX 0)
LAYOUT(HASHED());

create stream input    (key uint64) Engine=Distributed(test_shard_localhost, currentDatabase(), buffer_, key);
create stream null_    (key uint64) Engine=Null();
create stream buffer_  (key uint64) Engine=Buffer(currentDatabase(), dist_out, 1, 0, 0, 0, 0, 0, 0);
create stream dist_out (key uint64) Engine=Distributed(test_shard_localhost, currentDatabase(), null_, key);

create stream output (key uint64, val uint64) Engine=Memory();
CREATE MATERIALIZED VIEW mv TO output AS SELECT key, dictGetUInt64('dict_in_01023.dict', 'val', key) val FROM dist_out;

INSERT INTO input VALUES (1);

SELECT count() FROM output;

DROP STREAM mv;
DROP STREAM output;
DROP STREAM dist_out;
DROP STREAM buffer_;
DROP STREAM null_;
DROP STREAM input;
DROP DICTIONARY dict_in_01023.dict;
DROP STREAM dict_in_01023.input;
DROP DATABASE dict_in_01023;
