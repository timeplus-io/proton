-- Tags: replica

DROP STREAM IF EXISTS bad_arrays;
create stream bad_arrays (a array(string), b array(uint8)) ;

INSERT INTO bad_arrays VALUES ([''],[]),([''],[1]);

SELECT a FROM bad_arrays ARRAY JOIN b;

DROP STREAM bad_arrays;


DROP STREAM IF EXISTS bad_arrays;
create stream bad_arrays (a array(string), b array(string)) ;

INSERT INTO bad_arrays VALUES ([''],[]),([''],[]),([''],[]),([''],[]),([''],[]),([''],[]),([''],[]),([''],[]),([''],[]),(['abc'],['223750']),(['ноутбук acer aspire e5-532-p3p2'],[]),([''],[]),([''],[]),([''],[]),([''],[]),(['лучшие моноблоки 2016'],[]),(['лучшие моноблоки 2016'],[]),([''],[]),([''],[]);

SELECT a FROM bad_arrays ARRAY JOIN b;

DROP STREAM bad_arrays;


DROP STREAM IF EXISTS bad_arrays;
create stream bad_arrays (a array(string), b array(uint8)) ;

INSERT INTO bad_arrays VALUES (['abc','def'],[1,2,3]),([],[1,2]),(['a','b'],[]),(['Hello'],[1,2]),([],[]),(['x','y','z'],[4,5,6]);

SELECT a, b FROM bad_arrays ARRAY JOIN b;

DROP STREAM bad_arrays;
