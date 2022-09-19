DROP STREAM IF EXISTS table_MySQLWire;
create stream table_MySQLWire (x uint64) ENGINE = File(MySQLWire);
INSERT INTO table_MySQLWire SELECT number FROM numbers(10);
-- regression for not initializing serializations
INSERT INTO table_MySQLWire SELECT number FROM numbers(10);
DROP STREAM table_MySQLWire;
