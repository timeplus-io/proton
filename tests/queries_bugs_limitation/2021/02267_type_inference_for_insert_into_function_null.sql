INSERT INTO function null() SELECT 1;
INSERT INTO function null() SELECT number FROM numbers(10);
INSERT INTO function null() SELECT number, to_string(number) FROM numbers(10);
INSERT INTO function null('auto') SELECT 1;
INSERT INTO function null('auto') SELECT number FROM numbers(10);
INSERT INTO function null('auto') SELECT number, to_string(number) FROM numbers(10);
