DROP STREAM IF EXISTS replaceall;
create stream replaceall (str FixedString(3)) ;

INSERT INTO replaceall VALUES ('foo');
INSERT INTO replaceall VALUES ('boa');
INSERT INTO replaceall VALUES ('bar');
INSERT INTO replaceall VALUES ('bao');

SELECT
    str,
    replaceAll(str, 'o', '*') AS replaced
FROM replaceall
ORDER BY str ASC;

DROP STREAM replaceall;

create stream replaceall (date date DEFAULT today(), fs FixedString(16)) ENGINE = MergeTree(date, (date, fs), 8192);
INSERT INTO replaceall (fs) VALUES ('54db0d43009d\0\0\0\0'), ('fe2b58224766cf10'), ('54db0d43009d\0\0\0\0'), ('fe2b58224766cf10');

SELECT fs, replaceAll(fs, '\0', '*')
FROM replaceall
ORDER BY fs ASC;

DROP STREAM replaceall;
