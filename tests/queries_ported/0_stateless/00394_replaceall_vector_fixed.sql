SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS replaceall;
create stream replaceall (str fixed_string(3)) ;

INSERT INTO replaceall (str) VALUES ('foo');
INSERT INTO replaceall (str) VALUES ('boa');
INSERT INTO replaceall (str) VALUES ('bar');
INSERT INTO replaceall (str) VALUES ('bao');
SELECT sleep(3);

SELECT
    str,
     replace_all(str, 'o', '*') AS replaced
FROM replaceall
ORDER BY str ASC;

DROP STREAM replaceall;

create stream replaceall (date date DEFAULT today(), fs fixed_string(16)) ENGINE = MergeTree(date, (date, fs), 8192);
INSERT INTO replaceall (fs) VALUES ('54db0d43009d\0\0\0\0'), ('fe2b58224766cf10'), ('54db0d43009d\0\0\0\0'), ('fe2b58224766cf10');
SELECT sleep(3);

SELECT fs,  replace_all(fs, '\0', '*')
FROM replaceall
ORDER BY fs ASC;

DROP STREAM replaceall;
