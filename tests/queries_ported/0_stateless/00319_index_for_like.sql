SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS index_for_like;
create stream index_for_like (s string, d date DEFAULT today()) ENGINE = MergeTree(d, (s, d), 1);

INSERT INTO index_for_like (s) VALUES ('Hello'), ('Hello, World'), ('Hello, World 1'), ('Hello 1'), ('Goodbye'), ('Goodbye, World'), ('Goodbye 1'), ('Goodbye, World 1'); 
SELECT sleep(3);

SET max_rows_to_read = 3;
SELECT s FROM index_for_like WHERE s LIKE 'Hello, World%';

SET max_rows_to_read = 2;
SELECT s FROM index_for_like WHERE s LIKE 'Hello, World %';

SET max_rows_to_read = 2;
SELECT s FROM index_for_like WHERE s LIKE 'Hello, World 1%';

SET max_rows_to_read = 1;
SELECT s FROM index_for_like WHERE s LIKE 'Hello, World 2%';

SET max_rows_to_read = 1;
SELECT s FROM index_for_like WHERE s LIKE 'Hello, Worle%';

SET max_rows_to_read = 3;
SELECT s FROM index_for_like WHERE s LIKE 'Hello, Wor%';

SET max_rows_to_read = 5;
SELECT s FROM index_for_like WHERE s LIKE 'Hello%';

SET max_rows_to_read = 2;
SELECT s FROM index_for_like WHERE s LIKE 'Hello %';

SET max_rows_to_read = 3;
SELECT s FROM index_for_like WHERE s LIKE 'Hello,%';

SET max_rows_to_read = 1;
SELECT s FROM index_for_like WHERE s LIKE 'Hello;%';

SET max_rows_to_read = 5;
SELECT s FROM index_for_like WHERE s LIKE 'H%';

SET max_rows_to_read = 4;
SELECT s FROM index_for_like WHERE s LIKE 'Good%';

SET max_rows_to_read = 8;
SELECT s FROM index_for_like WHERE s LIKE '%';
SELECT s FROM index_for_like WHERE s LIKE '%Hello%';
SELECT s FROM index_for_like WHERE s LIKE '%Hello';

SET max_rows_to_read = 3;
SELECT s FROM index_for_like WHERE s LIKE 'Hello, World% %';
SELECT s FROM index_for_like WHERE s LIKE 'Hello, Worl_%';

SET max_rows_to_read = 1;
SELECT s FROM index_for_like WHERE s LIKE 'Hello, Worl\\_%';

DROP STREAM index_for_like;
