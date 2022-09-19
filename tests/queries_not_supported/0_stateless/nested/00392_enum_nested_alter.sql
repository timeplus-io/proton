DROP STREAM IF EXISTS enum_nested_alter;
create stream enum_nested_alter
(d date DEFAULT '2000-01-01', x uint64, n nested(a string, e Enum8('Hello' = 1), b uint8)) 
ENGINE = MergeTree(d, x, 1);

INSERT INTO enum_nested_alter (x, n.e) VALUES (1, ['Hello']);
SELECT * FROM enum_nested_alter;

ALTER STREAM enum_nested_alter MODIFY COLUMN n.e array(Enum8('Hello' = 1, 'World' = 2));
INSERT INTO enum_nested_alter (x, n.e) VALUES (2, ['World']);
SELECT * FROM enum_nested_alter ORDER BY x;

ALTER STREAM enum_nested_alter MODIFY COLUMN n.e array(Enum16('Hello' = 1, 'World' = 2, 'a' = 300));
SELECT * FROM enum_nested_alter ORDER BY x;

ALTER STREAM enum_nested_alter MODIFY COLUMN n.e array(UInt16);
SELECT * FROM enum_nested_alter ORDER BY x;

ALTER STREAM enum_nested_alter MODIFY COLUMN n.e array(Enum16('Hello' = 1, 'World' = 2, 'a' = 300));
SELECT * FROM enum_nested_alter ORDER BY x;

ALTER STREAM enum_nested_alter MODIFY COLUMN n.e array(string);
SELECT * FROM enum_nested_alter ORDER BY x;

ALTER STREAM enum_nested_alter MODIFY COLUMN n.e array(Enum16('Hello' = 1, 'World' = 2, 'a' = 300));
SELECT * FROM enum_nested_alter ORDER BY x;

DROP STREAM enum_nested_alter;


create stream enum_nested_alter
(
    d date DEFAULT '2000-01-01', 
    x uint64, 
    tasks nested(
        errcategory Enum8(
            'undefined' = 0, 'system' = 1, 'generic' = 2, 'asio.netdb' = 3, 'asio.misc' = 4, 
            'asio.addrinfo' = 5, 'rtb.client' = 6, 'rtb.logic' = 7, 'http.status' = 8), 
        status Enum16('hello' = 1, 'world' = 2)))
ENGINE = MergeTree(d, x, 1);

INSERT INTO enum_nested_alter (x, tasks.errcategory, tasks.status) VALUES (1, ['system', 'rtb.client'], ['hello', 'world']);
SELECT * FROM enum_nested_alter ORDER BY x;

ALTER STREAM enum_nested_alter 
    MODIFY COLUMN tasks.errcategory array(Enum8(
            'undefined' = 0, 'system' = 1, 'generic' = 2, 'asio.netdb' = 3, 'asio.misc' = 4, 
            'asio.addrinfo' = 5, 'rtb.client' = 6, 'rtb.logic' = 7, 'http.status' = 8, 'http.code' = 9)),
    MODIFY COLUMN tasks.status array(Enum8('hello' = 1, 'world' = 2, 'goodbye' = 3));

INSERT INTO enum_nested_alter (x, tasks.errcategory, tasks.status) VALUES (2, ['http.status', 'http.code'], ['hello', 'goodbye']);
SELECT * FROM enum_nested_alter ORDER BY x;

DROP STREAM enum_nested_alter;


DROP STREAM IF EXISTS enum_nested_alter;
create stream enum_nested_alter
(d date DEFAULT '2000-01-01', x uint64, n nested(a string, e Enum8('Hello.world' = 1), b uint8)) 
ENGINE = MergeTree(d, x, 1);

INSERT INTO enum_nested_alter (x, n.e) VALUES (1, ['Hello.world']);
SELECT * FROM enum_nested_alter;

ALTER STREAM enum_nested_alter MODIFY COLUMN n.e array(Enum8('Hello.world' = 1, 'a' = 2));
SELECT * FROM enum_nested_alter;

DROP STREAM enum_nested_alter;
