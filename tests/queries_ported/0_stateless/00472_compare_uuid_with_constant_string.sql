SELECT to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba0') = '61f0c404-5cb3-11e7-907b-a6006ad3dba0';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'         = to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba0');
SELECT to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba0') = '61f0c404-5cb3-11e7-907b-a6006ad3dba1';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'         = to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba1');

SELECT to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba0') != '61f0c404-5cb3-11e7-907b-a6006ad3dba0';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'         != to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba0');
SELECT to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba0') != '61f0c404-5cb3-11e7-907b-a6006ad3dba1';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'         != to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba1');


SELECT to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba0') < '61f0c404-5cb3-11e7-907b-a6006ad3dba1';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'         < to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba1');
SELECT to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba1') < '61f0c404-5cb3-11e7-907b-a6006ad3dba0';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba1'         < to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba0');
SELECT to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba0') < '61f0c404-5cb3-11e7-907b-a6006ad3dba0';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'         < to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba0');


SELECT to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba0') > '61f0c404-5cb3-11e7-907b-a6006ad3dba1';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'         > to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba1');
SELECT to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba2') > '61f0c404-5cb3-11e7-907b-a6006ad3dba1';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba2'         > to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba1');
SELECT to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba1') > '61f0c404-5cb3-11e7-907b-a6006ad3dba1';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba1'         > to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba1');


SELECT to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba0') <= '61f0c404-5cb3-11e7-907b-a6006ad3dba1';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'         <= to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba1');
SELECT to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba0') <= '61f0c404-5cb3-11e7-907b-a6006ad3dba0';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'         <= to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba0');
SELECT to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba2') <= '61f0c404-5cb3-11e7-907b-a6006ad3dba1';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba2'         <= to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba1');

SELECT to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba0') >= '61f0c404-5cb3-11e7-907b-a6006ad3dba1';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'         >= to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba1');
SELECT to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba0') >= '61f0c404-5cb3-11e7-907b-a6006ad3dba0';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba0'         >= to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba0');
SELECT to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba2') >= '61f0c404-5cb3-11e7-907b-a6006ad3dba1';
SELECT '61f0c404-5cb3-11e7-907b-a6006ad3dba2'         >= to_uuid('61f0c404-5cb3-11e7-907b-a6006ad3dba1');



