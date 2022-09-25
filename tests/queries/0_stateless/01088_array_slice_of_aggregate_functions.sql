select array_slice(group_array(x),1,1) as y from (select uniq_state(number) as x from numbers(10) group by number);
