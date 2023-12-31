-- Tags: no-parallel

-- TODO: can't just remove default prefix, it breaks the test!
SET query_mode = 'table';
drop database if exists db_01080;
create database db_01080;

drop stream if exists db_01080.test_table_01080;
create stream db_01080.test_table_01080 (dim_key int64, dim_id string) ENGINE = MergeTree Order by (dim_key);
insert into db_01080.test_table_01080 values(1,'test1');

drop DICTIONARY if exists db_01080.test_dict_01080;

CREATE DICTIONARY db_01080.test_dict_01080 ( dim_key int64, dim_id string )
PRIMARY KEY dim_key
source(clickhouse(host 'localhost' port tcpPort() user 'default' password '' db 'db_01080' table 'test_table_01080'))
LIFETIME(MIN 0 MAX 0) LAYOUT(complex_key_hashed());

SELECT dictGetString('db_01080.test_dict_01080', 'dim_id', tuple(to_int64(1)));

SELECT dictGetString('db_01080.test_dict_01080', 'dim_id', tuple(to_int64(0)));

select dictGetString('db_01080.test_dict_01080', 'dim_id', x)  from (select tuple(to_int64(0)) as x);

select dictGetString('db_01080.test_dict_01080', 'dim_id', x)  from (select tuple(to_int64(1)) as x);

select dictGetString('db_01080.test_dict_01080', 'dim_id', x)  from (select tuple(to_int64(number)) as x from numbers(5));

select dictGetString('db_01080.test_dict_01080', 'dim_id', x) from (select tuple(to_int64(rand64()*0)) as x);

select dictGetString('db_01080.test_dict_01080', 'dim_id', x) from (select tuple(to_int64(blockSize()=0)) as x);

select dictGetString('db_01080.test_dict_01080', 'dim_id', x) from (select tuple(to_int64(materialize(0))) as x);

select dictGetString('db_01080.test_dict_01080', 'dim_id', x) from (select tuple(to_int64(materialize(1))) as x);


drop DICTIONARY   db_01080.test_dict_01080;
drop stream   db_01080.test_table_01080;
drop database db_01080;
