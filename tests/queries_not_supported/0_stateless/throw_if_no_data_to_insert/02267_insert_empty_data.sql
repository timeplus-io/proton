DROP STREAM IF EXISTS t;

CREATE STREAM t (n uint32) ENGINE=Memory;

INSERT INTO t VALUES; -- { clientError 108 }

set throw_if_no_data_to_insert = 0;

INSERT INTO t VALUES;

DROP STREAM t;
