DROP STREAM IF EXISTS constrained;
create stream constrained (URL string, CONSTRAINT is_yandex CHECK domainWithoutWWW(URL) = 'yandex.ru', CONSTRAINT is_utf8 CHECK isValidUTF8(URL)) ENGINE = Null;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('Hello'), ('test'); -- { serverError 469 }
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('ftp://yandex.ru/Hello'), ('https://yandex.ru/te\xFFst'); -- { serverError 469 }
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('ftp://yandex.ru/Hello'), (toValidUTF8('https://yandex.ru/te\xFFst'));
DROP STREAM constrained;

create stream constrained (URL string, CONSTRAINT is_yandex CHECK domainWithoutWWW(URL) = 'yandex.ru', CONSTRAINT is_utf8 CHECK isValidUTF8(URL)) ;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('Hello'), ('test'); -- { serverError 469 }
SELECT count() FROM constrained;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('ftp://yandex.ru/Hello'), ('https://yandex.ru/te\xFFst'); -- { serverError 469 }
SELECT count() FROM constrained;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('ftp://yandex.ru/Hello'), (toValidUTF8('https://yandex.ru/te\xFFst'));
SELECT count() FROM constrained;
DROP STREAM constrained;

create stream constrained (URL string, CONSTRAINT is_yandex CHECK domainWithoutWWW(URL) = 'yandex.ru', CONSTRAINT is_utf8 CHECK isValidUTF8(URL)) ENGINE = StripeLog;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('Hello'), ('test'); -- { serverError 469 }
SELECT count() FROM constrained;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('ftp://yandex.ru/Hello'), ('https://yandex.ru/te\xFFst'); -- { serverError 469 }
SELECT count() FROM constrained;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('ftp://yandex.ru/Hello'), (toValidUTF8('https://yandex.ru/te\xFFst'));
SELECT count() FROM constrained;
DROP STREAM constrained;

create stream constrained (URL string, CONSTRAINT is_yandex CHECK domainWithoutWWW(URL) = 'yandex.ru', CONSTRAINT is_utf8 CHECK isValidUTF8(URL)) ;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('Hello'), ('test'); -- { serverError 469 }
SELECT count() FROM constrained;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('ftp://yandex.ru/Hello'), ('https://yandex.ru/te\xFFst'); -- { serverError 469 }
SELECT count() FROM constrained;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('ftp://yandex.ru/Hello'), (toValidUTF8('https://yandex.ru/te\xFFst'));
SELECT count() FROM constrained;
DROP STREAM constrained;

create stream constrained (URL string, CONSTRAINT is_yandex CHECK domainWithoutWWW(URL) = 'yandex.ru', CONSTRAINT is_utf8 CHECK isValidUTF8(URL))  ;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('Hello'), ('test'); -- { serverError 469 }
SELECT count() FROM constrained;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('ftp://yandex.ru/Hello'), ('https://yandex.ru/te\xFFst'); -- { serverError 469 }
SELECT count() FROM constrained;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('ftp://yandex.ru/Hello'), (toValidUTF8('https://yandex.ru/te\xFFst'));
SELECT count() FROM constrained;
DROP STREAM constrained;


DROP STREAM IF EXISTS constrained2;
create stream constrained (URL string, CONSTRAINT is_yandex CHECK domainWithoutWWW(URL) = 'yandex.ru', CONSTRAINT is_utf8 CHECK isValidUTF8(URL))  ;
create stream constrained2 AS constrained;
SHOW create stream constrained;
SHOW create stream constrained2;
INSERT INTO constrained VALUES ('https://www.yandex.ru/?q=upyachka'), ('Hello'), ('test'); -- { serverError 469 }
INSERT INTO constrained2 VALUES ('https://www.yandex.ru/?q=upyachka'), ('Hello'), ('test'); -- { serverError 469 }
DROP STREAM constrained;
DROP STREAM constrained2;
