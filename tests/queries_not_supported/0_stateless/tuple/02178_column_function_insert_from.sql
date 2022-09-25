DROP STREAM IF EXISTS TESTTABLE;

create stream TESTTABLE (
  _id uint64,  pt string, attr_list array(string)
) ENGINE = MergeTree() PARTITION BY (pt) ORDER BY tuple();

INSERT INTO TESTTABLE values (0,'0',['1']), (1,'1',['1']);

SET max_threads = 1;

SELECT attr, _id, array_filter(x -> (x IN (select '1')), attr_list) z
FROM TESTTABLE ARRAY JOIN z AS attr ORDER BY _id LIMIT 3 BY attr;

DROP STREAM TESTTABLE;
