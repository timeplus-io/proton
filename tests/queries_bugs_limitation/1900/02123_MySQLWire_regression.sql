DROP STREAM IF EXISTS stream_MySQLWire;
CREATE STREAM stream_MySQLWire (x uint64) ENGINE = File(MySQLWire);
INSERT INTO stream_MySQLWire SELECT number FROM numbers(10);
-- regression for not initializing serializations
INSERT INTO stream_MySQLWire SELECT number FROM numbers(10);
DROP STREAM stream_MySQLWire;
