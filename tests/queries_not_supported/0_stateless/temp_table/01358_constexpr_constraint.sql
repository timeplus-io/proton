CREATE TEMPORARY STREAM constrained
(
    `URL` string,
    CONSTRAINT is_yandex CHECK domainWithoutWWW(URL) = domainWithoutWWW(URL),
    CONSTRAINT is_utf8 CHECK isValidUTF8(URL)
);

insert into constrained values ('a');

DROP TEMPORARY TABLE constrained;
CREATE TEMPORARY STREAM constrained (x uint8, CONSTRAINT bogus CHECK 0);
INSERT INTO constrained VALUES (1); -- { serverError 469 }
