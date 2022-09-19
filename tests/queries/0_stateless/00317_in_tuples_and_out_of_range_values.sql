SELECT (1,'') IN ((-1,''));
SELECT (1,'') IN ((1,''));
SELECT (1,'') IN (-1,'');
SELECT (1,'') IN (1,'');
SELECT (1,'') IN ((-1,''),(1,''));

SELECT (number, to_string(number)) IN ((1, '1'), (-1, '-1')) FROM system.numbers LIMIT 10;
SELECT (number - 1, to_string(number - 1)) IN ((1, '1'), (-1, '-1')) FROM system.numbers LIMIT 10;
