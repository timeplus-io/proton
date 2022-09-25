SELECT name FROM system.tables WHERE NOT is_temporary AND ((database != 'system') OR (database = 'system' AND (name = 'tables' OR name = 'tasks')))

SELECT database, name FROM system.tables WHERE NOT is_temporary AND ((database != 'system') OR (database = 'system' AND (name = 'tables' OR name = 'tasks'))) AND create_table_query = '';

for i in `seq 1000000`
do
    proton-client --query "SELECT * FROM system.tables WHERE NOT is_temporary AND ((database != 'system') OR (database = 'system' AND (name = 'tables' OR name = 'tasks'))) AND create_table_query = ''"
done