select number, array_reduce( 'sumMap', [a],[b]  ) from (select [100,100,200] as a,[10,20,30] as b, number from numbers(1));
select number, array_reduce( 'sumMap', [a],[b]  ) from (select materialize([100,100,200]) as a,materialize([10,20,30]) as b, number from numbers(10));
select number, array_reduce( 'sumMap', [a],[b]  ) from (select [100,100,200] as a,[10,20,30] as b, number from numbers(10));
select number, array_reduce( 'sum', a) from (select materialize([100,100,200]) as a, number from numbers(10));
select number, array_reduce( 'max', [a] ) from (select materialize([100,100,200]) as a, number from numbers(10));

select dumpColumnStructure([a]), array_reduce('sumMap', [a], [a]) from (select [1, 2] as a FROM numbers(2));
