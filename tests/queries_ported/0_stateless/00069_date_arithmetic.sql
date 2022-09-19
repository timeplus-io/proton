SELECT to_type_name(now() - now()) = 'int32';
SELECT to_type_name(now() + 1) LIKE 'datetime%';
SELECT to_type_name(1 + now()) LIKE 'datetime%';
SELECT to_type_name(now() - 1) LIKE 'datetime%';
SELECT to_datetime(1) + 1 = to_datetime(2);
SELECT 1 + to_datetime(1) = to_datetime(2);
SELECT to_datetime(1) - 1 = to_datetime(0);

SELECT to_type_name(today()) = 'date';
SELECT today() = to_date(now());

SELECT to_type_name(yesterday()) = 'date';

SELECT to_type_name(today() - today()) = 'int32';
SELECT to_type_name(today() + 1) = 'date';
SELECT to_type_name(1 + today()) = 'date';
SELECT to_type_name(today() - 1) = 'date';
SELECT yesterday() + 1 = today();
SELECT 1 + yesterday() = today();
SELECT today() - 1 = yesterday();
