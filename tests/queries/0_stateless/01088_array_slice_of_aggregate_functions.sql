select arraySlice(group_array(x),1,1) as y from (select uniqState(number) as x from numbers(10) group by number);
