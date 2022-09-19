DROP STREAM IF EXISTS has_column_in_table;
create stream has_column_in_table (i Int64, s string, nest nested(x uint8, y uint32)) ;

/* existing column */
SELECT hasColumnInTable(currentDatabase(), 'has_column_in_table', 'i');
SELECT hasColumnInTable('localhost', currentDatabase(), 'has_column_in_table', 'i');
SELECT hasColumnInTable(currentDatabase(), 'has_column_in_table', 's');
SELECT hasColumnInTable('localhost', currentDatabase(), 'has_column_in_table', 's');
SELECT hasColumnInTable(currentDatabase(), 'has_column_in_table', 'nest.x');
SELECT hasColumnInTable('localhost', currentDatabase(), 'has_column_in_table', 'nest.x');
SELECT hasColumnInTable(currentDatabase(), 'has_column_in_table', 'nest.y');
SELECT hasColumnInTable('localhost', currentDatabase(), 'has_column_in_table', 'nest.y');

/* not existing column */
SELECT hasColumnInTable(currentDatabase(), 'has_column_in_table', 'nest');
SELECT hasColumnInTable('localhost', currentDatabase(), 'has_column_in_table', 'nest');
SELECT hasColumnInTable(currentDatabase(), 'has_column_in_table', 'nest.not_existing');
SELECT hasColumnInTable('localhost', currentDatabase(), 'has_column_in_table', 'nest.not_existing');
SELECT hasColumnInTable(currentDatabase(), 'has_column_in_table', 'not_existing');
SELECT hasColumnInTable('localhost', currentDatabase(), 'has_column_in_table', 'not_existing');
SELECT hasColumnInTable('system', 'one', '');

/* bad queries */
SELECT hasColumnInTable('', '', '');  -- { serverError 60; }
SELECT hasColumnInTable('', 't', 'c');  -- { serverError 81; }
SELECT hasColumnInTable(currentDatabase(), '', 'c'); -- { serverError 60; }
SELECT hasColumnInTable('d', 't', 's');  -- { serverError 81; }
SELECT hasColumnInTable(currentDatabase(), 't', 's');  -- { serverError 60; }


DROP STREAM has_column_in_table;
