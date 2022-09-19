SET query_mode = 'table';
drop stream if exists A1;
drop stream if exists A_M;
create stream A1( a DateTime ) ENGINE = MergeTree ORDER BY tuple();
create stream A_M as A1 ENGINE = Merge(currentDatabase(), '^A1$');
insert into A1(a) select now();

set optimize_move_to_prewhere=0;

SELECT tupleElement(array_join([(1, 1)]), 1) FROM A_M PREWHERE tupleElement((1, 1), 1) =1;

SELECT tupleElement(array_join([(1, 1)]), 1) FROM A_M WHERE tupleElement((1, 1), 1) =1;

SELECT tupleElement(array_join([(1, 1)]), 1) FROM A1 PREWHERE tupleElement((1, 1), 1) =1;

drop stream A1;
drop stream A_M;
